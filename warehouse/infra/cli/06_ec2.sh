#!/usr/bin/env bash
# Paso 6: EC2 con Airflow + dbt (Docker Compose). IMDSv2 con hop limit 2 (contenedores usan el rol).
source "$(dirname "$0")/config.sh"
DIR="$(cd "$(dirname "$0")" && pwd)"
for v in RDS_HOST DOCDB_HOST RS_HOST RS_ROLE_ARN EC2_SG_ID; do
  [[ -n "${!v:-}" ]] || { echo "ERROR: falta $v en .state.env; ejecute los pasos 1 a 4 primero"; exit 1; }
done

SUBNET_ID=$(aws ec2 describe-subnets --filters Name=vpc-id,Values="$VPC_ID" Name=default-for-az,Values=true \
            --query 'Subnets[0].SubnetId' --output text)
AMI_ID=$(aws ssm get-parameter --name /aws/service/ami-amazon-linux-latest/al2023-ami-kernel-default-x86_64 \
         --query Parameter.Value --output text)

echo ">> key pair $KEY_NAME"
if ! aws ec2 describe-key-pairs --key-names "$KEY_NAME" >/dev/null 2>&1; then
  mkdir -p "$HOME/.ssh" && chmod 700 "$HOME/.ssh"
  aws ec2 create-key-pair --key-name "$KEY_NAME" --key-type ed25519 --query KeyMaterial --output text > "$KEY_PATH"
  chmod 400 "$KEY_PATH"
fi

echo ">> user data"
sed -e "s|__BUCKET__|$BUCKET|g" -e "s|__REGION__|$AWS_REGION|g" -e "s|__SSM_PREFIX__|$SSM_PREFIX|g" \
    -e "s|__ACCOUNT__|$ACCOUNT_ID|g" -e "s|__RDS_HOST__|$RDS_HOST|g" -e "s|__DOCDB_HOST__|$DOCDB_HOST|g" \
    -e "s|__RS_HOST__|$RS_HOST|g" -e "s|__RS_ROLE_ARN__|$RS_ROLE_ARN|g" -e "s|__DB_ADMIN__|$DB_ADMIN|g" \
    "$DIR/user_data.sh.tpl" > "$DIR/.user_data.rendered.sh"

if [[ -n "${INSTANCE_ID:-}" ]] && [[ "$(aws ec2 describe-instances --instance-ids "$INSTANCE_ID" \
      --query 'Reservations[0].Instances[0].State.Name' --output text 2>/dev/null)" =~ ^(pending|running|stopped)$ ]]; then
  echo "   ya existe $INSTANCE_ID"
else
  INSTANCE_ID=""
  for intento in 1 2 3 4 5 6; do   # IAM tarda en propagar un perfil recién creado
    if INSTANCE_ID=$(aws ec2 run-instances --image-id "$AMI_ID" --instance-type "$INSTANCE_TYPE" \
        --key-name "$KEY_NAME" --subnet-id "$SUBNET_ID" --security-group-ids "$EC2_SG_ID" --associate-public-ip-address \
        --iam-instance-profile Name="$EC2_PROFILE" \
        --metadata-options "HttpTokens=required,HttpPutResponseHopLimit=2,HttpEndpoint=enabled" \
        --block-device-mappings '[{"DeviceName":"/dev/xvda","Ebs":{"VolumeSize":30,"VolumeType":"gp3","Encrypted":true,"DeleteOnTermination":true}}]' \
        --user-data "file://$DIR/.user_data.rendered.sh" \
        --tag-specifications "ResourceType=instance,Tags=[{Key=Name,Value=${PROJECT}-airflow},{Key=Project,Value=${PROJECT}}]" \
        --query 'Instances[0].InstanceId' --output text); then break; fi
    echo "   run-instances falló (intento $intento/6); reintento en 15 s"; sleep 15
  done
  [[ -n "$INSTANCE_ID" ]] || { echo "ERROR: no se pudo crear la instancia"; exit 1; }
  save_state INSTANCE_ID "$INSTANCE_ID"
fi
aws ec2 wait instance-running --instance-ids "$INSTANCE_ID"
PUBLIC_DNS=$(aws ec2 describe-instances --instance-ids "$INSTANCE_ID" --query 'Reservations[0].Instances[0].PublicDnsName' --output text)
save_state PUBLIC_DNS "$PUBLIC_DNS"
cat <<MSG

OK  instancia=$INSTANCE_ID
    Airflow tarda ~8-12 min (instala Docker y construye la imagen con dbt).
    Progreso:    ssh -i $KEY_PATH ec2-user@$PUBLIC_DNS 'sudo tail -f /var/log/user-data.log'
    Contraseña:  aws ssm get-parameter --name $SSM_PREFIX/airflow_admin_password --with-decryption --query Parameter.Value --output text
    UI:          http://$PUBLIC_DNS:8080   (usuario admin)
MSG
