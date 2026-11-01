#!/usr/bin/env bash
# Paso 4: Amazon Redshift Serverless (namespace + workgroup de 8 RPU, el mínimo).
#   * El rol tallerdw-redshift-s3-role queda como rol por defecto para COPY desde S3.
#   * publicly-accessible para que QuickSight (Standard) llegue por su rango de IPs; el SG limita
#     el puerto 5439 a la EC2/VPC y a QuickSight.
#   * Límite de uso diario (corta el workgroup si se pasa) para controlar costos.
source "$(dirname "$0")/config.sh"

RS_PW=$(aws ssm get-parameter --name "$SSM_PREFIX/redshift_password" --with-decryption --query Parameter.Value --output text)
# Redshift Serverless no está disponible en todas las zonas (p. ej. use1-az3): se excluye esa zona.
SUBNETS=$(aws ec2 describe-subnets --filters Name=vpc-id,Values="$VPC_ID" Name=default-for-az,Values=true \
          --query "Subnets[?AvailabilityZoneId!='use1-az3'].SubnetId" --output text)

echo ">> namespace $RS_NAMESPACE"
if ! aws redshift-serverless get-namespace --namespace-name "$RS_NAMESPACE" >/dev/null 2>&1; then
  aws redshift-serverless create-namespace --namespace-name "$RS_NAMESPACE" \
    --admin-username "$DB_ADMIN" --admin-user-password "$RS_PW" --db-name "$RS_DB" \
    --iam-roles "$RS_ROLE_ARN" --default-iam-role-arn "$RS_ROLE_ARN" \
    --tags key=Project,value="$PROJECT" >/dev/null
fi
for i in $(seq 1 30); do
  st=$(aws redshift-serverless get-namespace --namespace-name "$RS_NAMESPACE" --query namespace.status --output text)
  [[ "$st" == "AVAILABLE" ]] && break; echo "   namespace $st..."; sleep 10
done

echo ">> workgroup $RS_WORKGROUP (8 RPU)"
if ! aws redshift-serverless get-workgroup --workgroup-name "$RS_WORKGROUP" >/dev/null 2>&1; then
  aws redshift-serverless create-workgroup --workgroup-name "$RS_WORKGROUP" --namespace-name "$RS_NAMESPACE" \
    --base-capacity 8 --publicly-accessible --security-group-ids "$RS_SG_ID" --subnet-ids $SUBNETS \
    --tags key=Project,value="$PROJECT" >/dev/null
fi
for i in $(seq 1 60); do
  st=$(aws redshift-serverless get-workgroup --workgroup-name "$RS_WORKGROUP" --query workgroup.status --output text)
  [[ "$st" == "AVAILABLE" ]] && break; echo "   workgroup $st..."; sleep 15
done

WG_ARN=$(aws redshift-serverless get-workgroup --workgroup-name "$RS_WORKGROUP" --query workgroup.workgroupArn --output text)
echo ">> límite de uso: 10 RPU-hora por día (≈ US\$3,6/día como máximo)"
aws redshift-serverless create-usage-limit --resource-arn "$WG_ARN" --usage-type serverless-compute \
  --amount 10 --period daily --breach-action deactivate >/dev/null 2>&1 || echo "   (ya existía o no se pudo crear; revíselo en la consola)"

RS_HOST=$(aws redshift-serverless get-workgroup --workgroup-name "$RS_WORKGROUP" --query workgroup.endpoint.address --output text)
save_state RS_HOST "$RS_HOST"
echo "OK  redshift=$RS_HOST:5439/$RS_DB  usuario=$DB_ADMIN"
