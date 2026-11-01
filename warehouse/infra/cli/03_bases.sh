#!/usr/bin/env bash
# Paso 3: fuentes administradas.
#   * Amazon RDS for MySQL 8.0 (db.t3.micro, capa gratuita) - privada, cifrada.
#   * Amazon DocumentDB 5.0 (1 instancia db.t3.medium) - privada, TLS obligatorio, cifrada.
# Ambas tardan ~10 min en quedar disponibles; el script espera.
source "$(dirname "$0")/config.sh"

MYSQL_PW=$(aws ssm get-parameter --name "$SSM_PREFIX/mysql_password" --with-decryption --query Parameter.Value --output text)
DOCDB_PW=$(aws ssm get-parameter --name "$SSM_PREFIX/docdb_password" --with-decryption --query Parameter.Value --output text)
SUBNETS=$(aws ec2 describe-subnets --filters Name=vpc-id,Values="$VPC_ID" Name=default-for-az,Values=true \
          --query 'Subnets[].SubnetId' --output text)

echo ">> subnet group (subredes de la VPC por defecto)"
aws rds describe-db-subnet-groups --db-subnet-group-name "$DB_SUBNET_GROUP" >/dev/null 2>&1 || \
  aws rds create-db-subnet-group --db-subnet-group-name "$DB_SUBNET_GROUP" \
    --db-subnet-group-description "Subredes para RDS y DocumentDB del taller" --subnet-ids $SUBNETS >/dev/null

echo ">> RDS MySQL $RDS_ID"
if ! aws rds describe-db-instances --db-instance-identifier "$RDS_ID" >/dev/null 2>&1; then
  aws rds create-db-instance --db-instance-identifier "$RDS_ID" \
    --engine mysql --engine-version 8.0 --db-instance-class db.t3.micro \
    --allocated-storage 20 --storage-type gp3 --storage-encrypted \
    --master-username "$DB_ADMIN" --master-user-password "$MYSQL_PW" --db-name ecommerce \
    --vpc-security-group-ids "$DB_SG_ID" --db-subnet-group-name "$DB_SUBNET_GROUP" \
    --no-publicly-accessible --no-multi-az --backup-retention-period 1 --no-deletion-protection \
    --tags Key=Project,Value="$PROJECT" >/dev/null
fi

echo ">> DocumentDB $DOCDB_CLUSTER"
if ! aws docdb describe-db-clusters --db-cluster-identifier "$DOCDB_CLUSTER" >/dev/null 2>&1; then
  aws docdb create-db-cluster --db-cluster-identifier "$DOCDB_CLUSTER" \
    --engine docdb --engine-version 5.0.0 \
    --master-username "$DB_ADMIN" --master-user-password "$DOCDB_PW" \
    --vpc-security-group-ids "$DB_SG_ID" --db-subnet-group-name "$DB_SUBNET_GROUP" \
    --storage-encrypted --no-deletion-protection --backup-retention-period 1 \
    --tags Key=Project,Value="$PROJECT" >/dev/null
fi
if ! aws docdb describe-db-instances --db-instance-identifier "$DOCDB_INSTANCE" >/dev/null 2>&1; then
  aws docdb create-db-instance --db-instance-identifier "$DOCDB_INSTANCE" \
    --db-cluster-identifier "$DOCDB_CLUSTER" --db-instance-class db.t3.medium --engine docdb >/dev/null
fi

echo ">> esperando RDS (5-10 min)..."
aws rds wait db-instance-available --db-instance-identifier "$RDS_ID"
echo ">> esperando DocumentDB (5-10 min)..."
aws docdb wait db-instance-available --db-instance-identifier "$DOCDB_INSTANCE"

RDS_HOST=$(aws rds describe-db-instances --db-instance-identifier "$RDS_ID" --query 'DBInstances[0].Endpoint.Address' --output text)
DOCDB_HOST=$(aws docdb describe-db-clusters --db-cluster-identifier "$DOCDB_CLUSTER" --query 'DBClusters[0].Endpoint' --output text)
save_state RDS_HOST "$RDS_HOST"
save_state DOCDB_HOST "$DOCDB_HOST"
echo "OK  rds=$RDS_HOST"
echo "    docdb=$DOCDB_HOST"
