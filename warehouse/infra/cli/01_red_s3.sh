#!/usr/bin/env bash
# Paso 1: grupos de seguridad (red) y bucket S3 privado del raw.
#   EC2 (Airflow)  <- tu IP en 22 y 8080
#   RDS/DocumentDB <- sólo desde la EC2 (3306, 27017): NO son públicos
#   Redshift       <- desde la EC2/VPC y desde el rango de QuickSight (5439)
source "$(dirname "$0")/config.sh"

ensure_sg() {  # ensure_sg NOMBRE DESCRIPCION
  local id; id=$(sg_id "$1")
  if [[ "$id" == "None" ]]; then
    id=$(aws ec2 create-security-group --group-name "$1" --description "$2" --vpc-id "$VPC_ID" \
         --tag-specifications "ResourceType=security-group,Tags=[{Key=Project,Value=$PROJECT}]" --query GroupId --output text)
  fi
  echo "$id"
}
allow() {  # allow SG puerto origen(cidr|sg-) descripcion  (ignora si ya existe)
  if [[ "$3" == sg-* ]]; then
    aws ec2 authorize-security-group-ingress --group-id "$1" --ip-permissions \
      "IpProtocol=tcp,FromPort=$2,ToPort=$2,UserIdGroupPairs=[{GroupId=$3,Description=$4}]" >/dev/null 2>&1 || true
  else
    aws ec2 authorize-security-group-ingress --group-id "$1" --ip-permissions \
      "IpProtocol=tcp,FromPort=$2,ToPort=$2,IpRanges=[{CidrIp=$3,Description=$4}]" >/dev/null 2>&1 || true
  fi
}

echo ">> security groups en $VPC_ID"
EC2_SG_ID=$(ensure_sg "$SG_EC2" "Airflow EC2: SSH y UI solo desde mi IP")
DB_SG_ID=$(ensure_sg "$SG_DB" "RDS MySQL y DocumentDB: solo desde la EC2")
RS_SG_ID=$(ensure_sg "$SG_RS" "Redshift Serverless: EC2/VPC y QuickSight")
allow "$EC2_SG_ID" 22 "$MY_IP" SSH
allow "$EC2_SG_ID" 8080 "$MY_IP" Airflow-UI
allow "$DB_SG_ID" 3306 "$EC2_SG_ID" MySQL-desde-EC2
allow "$DB_SG_ID" 27017 "$EC2_SG_ID" DocumentDB-desde-EC2
allow "$RS_SG_ID" 5439 "$EC2_SG_ID" Redshift-desde-EC2
allow "$RS_SG_ID" 5439 "$VPC_CIDR" Redshift-desde-VPC
allow "$RS_SG_ID" 5439 "$QUICKSIGHT_CIDR" QuickSight-us-east-1
save_state EC2_SG_ID "$EC2_SG_ID"; save_state DB_SG_ID "$DB_SG_ID"; save_state RS_SG_ID "$RS_SG_ID"

echo ">> bucket s3://$BUCKET (privado, cifrado, versionado, solo TLS)"
aws s3api head-bucket --bucket "$BUCKET" 2>/dev/null || aws s3api create-bucket --bucket "$BUCKET"
aws s3api put-public-access-block --bucket "$BUCKET" --public-access-block-configuration \
  BlockPublicAcls=true,IgnorePublicAcls=true,BlockPublicPolicy=true,RestrictPublicBuckets=true
aws s3api put-bucket-encryption --bucket "$BUCKET" --server-side-encryption-configuration \
  '{"Rules":[{"ApplyServerSideEncryptionByDefault":{"SSEAlgorithm":"AES256"},"BucketKeyEnabled":true}]}'
aws s3api put-bucket-versioning --bucket "$BUCKET" --versioning-configuration Status=Enabled
aws s3api put-bucket-policy --bucket "$BUCKET" --policy "{\"Version\":\"2012-10-17\",\"Statement\":[{\"Sid\":\"DenyInsecureTransport\",\"Effect\":\"Deny\",\"Principal\":\"*\",\"Action\":\"s3:*\",\"Resource\":[\"arn:aws:s3:::$BUCKET\",\"arn:aws:s3:::$BUCKET/*\"],\"Condition\":{\"Bool\":{\"aws:SecureTransport\":\"false\"}}}]}"
aws s3api put-bucket-lifecycle-configuration --bucket "$BUCKET" --lifecycle-configuration \
  '{"Rules":[{"ID":"noncurrent-30d","Status":"Enabled","Filter":{},"NoncurrentVersionExpiration":{"NoncurrentDays":30},"AbortIncompleteMultipartUpload":{"DaysAfterInitiation":1}}]}'
save_state BUCKET "$BUCKET"
echo "OK  ec2_sg=$EC2_SG_ID db_sg=$DB_SG_ID redshift_sg=$RS_SG_ID bucket=$BUCKET"
