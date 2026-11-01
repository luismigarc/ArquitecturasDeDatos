#!/usr/bin/env bash
# Elimina TODO lo creado por los scripts (para no generar más costos). Pide confirmación.
source "$(dirname "$0")/config.sh"
set +e
read -r -p "Se borrarán EC2, RDS, DocumentDB, Redshift, S3 ($BUCKET), roles y parámetros. Escriba BORRAR: " ok
[[ "$ok" == "BORRAR" ]] || { echo "cancelado"; exit 1; }

echo ">> EC2";        [[ -n "${INSTANCE_ID:-}" ]] && aws ec2 terminate-instances --instance-ids "$INSTANCE_ID" >/dev/null
echo ">> Redshift";   aws redshift-serverless delete-workgroup --workgroup-name "$RS_WORKGROUP" >/dev/null
echo ">> RDS";        aws rds delete-db-instance --db-instance-identifier "$RDS_ID" --skip-final-snapshot --delete-automated-backups >/dev/null
echo ">> DocumentDB"; aws docdb delete-db-instance --db-instance-identifier "$DOCDB_INSTANCE" >/dev/null
echo ">> esperando que se borren (10-15 min)..."
[[ -n "${INSTANCE_ID:-}" ]] && aws ec2 wait instance-terminated --instance-ids "$INSTANCE_ID"
aws docdb wait db-instance-deleted --db-instance-identifier "$DOCDB_INSTANCE"
aws docdb delete-db-cluster --db-cluster-identifier "$DOCDB_CLUSTER" --skip-final-snapshot >/dev/null
for i in $(seq 1 60); do
  aws redshift-serverless get-workgroup --workgroup-name "$RS_WORKGROUP" >/dev/null 2>&1 || break; sleep 15
done
aws redshift-serverless delete-namespace --namespace-name "$RS_NAMESPACE" >/dev/null
aws rds wait db-instance-deleted --db-instance-identifier "$RDS_ID"
for i in $(seq 1 40); do
  aws docdb describe-db-clusters --db-cluster-identifier "$DOCDB_CLUSTER" >/dev/null 2>&1 || break; sleep 15
done
aws rds delete-db-subnet-group --db-subnet-group-name "$DB_SUBNET_GROUP"
aws ec2 delete-key-pair --key-name "$KEY_NAME"; rm -f "$KEY_PATH"

echo ">> security groups (pueden tardar mientras AWS libera interfaces de red)"
for sg in "$RS_SG_ID" "$DB_SG_ID" "$EC2_SG_ID"; do
  for i in $(seq 1 20); do aws ec2 delete-security-group --group-id "$sg" 2>/dev/null && break; sleep 15; done
done

echo ">> IAM y SSM"
aws iam remove-role-from-instance-profile --instance-profile-name "$EC2_PROFILE" --role-name "$EC2_ROLE"
aws iam delete-instance-profile --instance-profile-name "$EC2_PROFILE"
aws iam delete-role-policy --role-name "$EC2_ROLE" --policy-name "${PROJECT}-least-privilege"
aws iam detach-role-policy --role-name "$EC2_ROLE" --policy-arn arn:aws:iam::aws:policy/AmazonSSMManagedInstanceCore
aws iam delete-role --role-name "$EC2_ROLE"
aws iam delete-role-policy --role-name "$RS_ROLE" --policy-name s3-read-raw
aws iam delete-role --role-name "$RS_ROLE"
for p in mysql_password docdb_password redshift_password quicksight_password airflow_admin_password; do
  aws ssm delete-parameter --name "$SSM_PREFIX/$p"
done

echo ">> bucket (todas las versiones)"
PY='import sys, boto3
b = boto3.resource("s3").Bucket(sys.argv[1]); b.object_versions.delete(); b.delete(); print("bucket eliminado")'
python3 -c "import boto3" 2>/dev/null && python3 -c "$PY" "$BUCKET" || uv run --with boto3 python -c "$PY" "$BUCKET"
rm -f "$(dirname "$0")/.state.env" "$(dirname "$0")/.user_data.rendered.sh"
echo "OK. Revise en la consola que no queden recursos con la etiqueta Project=$PROJECT."
echo "QuickSight: cancele la suscripción en QuickSight > Manage QuickSight > Account settings si no la usa más."
