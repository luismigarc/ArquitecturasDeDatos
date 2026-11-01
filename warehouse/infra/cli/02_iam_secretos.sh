#!/usr/bin/env bash
# Paso 2: contraseñas en SSM Parameter Store (SecureString) y roles IAM.
#   * tallerdw-ec2-role: la EC2 lee/escribe el bucket, lee los parámetros /tallerdw/* y refresca QuickSight.
#   * tallerdw-redshift-s3-role: Redshift lo asume para COPY desde S3 (sólo lectura del bucket).
source "$(dirname "$0")/config.sh"

echo ">> contraseñas (se generan una vez y se guardan cifradas en SSM)"
for p in mysql_password docdb_password redshift_password quicksight_password airflow_admin_password; do
  if ! aws ssm get-parameter --name "$SSM_PREFIX/$p" >/dev/null 2>&1; then
    aws ssm put-parameter --name "$SSM_PREFIX/$p" --type SecureString --value "$(gen_password)" >/dev/null
    echo "   creado $SSM_PREFIX/$p"
  else
    echo "   ya existe $SSM_PREFIX/$p"
  fi
done

echo ">> rol de Redshift para COPY desde S3"
aws iam get-role --role-name "$RS_ROLE" >/dev/null 2>&1 || aws iam create-role --role-name "$RS_ROLE" \
  --assume-role-policy-document '{"Version":"2012-10-17","Statement":[{"Effect":"Allow","Principal":{"Service":["redshift.amazonaws.com","redshift-serverless.amazonaws.com"]},"Action":"sts:AssumeRole"}]}' >/dev/null
aws iam put-role-policy --role-name "$RS_ROLE" --policy-name s3-read-raw --policy-document "{
  \"Version\":\"2012-10-17\",\"Statement\":[
   {\"Effect\":\"Allow\",\"Action\":[\"s3:ListBucket\",\"s3:GetBucketLocation\"],\"Resource\":\"arn:aws:s3:::$BUCKET\"},
   {\"Effect\":\"Allow\",\"Action\":[\"s3:GetObject\"],\"Resource\":\"arn:aws:s3:::$BUCKET/raw/*\"}]}"
RS_ROLE_ARN=$(aws iam get-role --role-name "$RS_ROLE" --query Role.Arn --output text)
save_state RS_ROLE_ARN "$RS_ROLE_ARN"

echo ">> rol de la EC2"
aws iam get-role --role-name "$EC2_ROLE" >/dev/null 2>&1 || aws iam create-role --role-name "$EC2_ROLE" \
  --assume-role-policy-document '{"Version":"2012-10-17","Statement":[{"Effect":"Allow","Principal":{"Service":"ec2.amazonaws.com"},"Action":"sts:AssumeRole"}]}' >/dev/null
aws iam put-role-policy --role-name "$EC2_ROLE" --policy-name "${PROJECT}-least-privilege" --policy-document "{
  \"Version\":\"2012-10-17\",\"Statement\":[
   {\"Effect\":\"Allow\",\"Action\":[\"s3:ListBucket\",\"s3:GetBucketLocation\"],\"Resource\":\"arn:aws:s3:::$BUCKET\"},
   {\"Effect\":\"Allow\",\"Action\":[\"s3:GetObject\",\"s3:PutObject\",\"s3:DeleteObject\"],\"Resource\":\"arn:aws:s3:::$BUCKET/*\"},
   {\"Effect\":\"Allow\",\"Action\":[\"ssm:GetParameter\",\"ssm:GetParameters\"],\"Resource\":\"arn:aws:ssm:$AWS_REGION:$ACCOUNT_ID:parameter$SSM_PREFIX/*\"},
   {\"Effect\":\"Allow\",\"Action\":\"kms:Decrypt\",\"Resource\":\"*\",\"Condition\":{\"StringEquals\":{\"kms:ViaService\":\"ssm.$AWS_REGION.amazonaws.com\"}}},
   {\"Effect\":\"Allow\",\"Action\":[\"quicksight:CreateIngestion\",\"quicksight:DescribeIngestion\"],\"Resource\":\"*\"}]}"
aws iam attach-role-policy --role-name "$EC2_ROLE" --policy-arn arn:aws:iam::aws:policy/AmazonSSMManagedInstanceCore
if ! aws iam get-instance-profile --instance-profile-name "$EC2_PROFILE" >/dev/null 2>&1; then
  aws iam create-instance-profile --instance-profile-name "$EC2_PROFILE" >/dev/null
  aws iam add-role-to-instance-profile --instance-profile-name "$EC2_PROFILE" --role-name "$EC2_ROLE"
fi
aws iam get-instance-profile --instance-profile-name "$EC2_PROFILE" --query 'InstanceProfile.Roles[].RoleName' --output text
echo "OK  rol_redshift=$RS_ROLE_ARN perfil_ec2=$EC2_PROFILE"
