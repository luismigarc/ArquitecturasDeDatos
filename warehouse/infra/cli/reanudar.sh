#!/usr/bin/env bash
# Enciende RDS, DocumentDB y la EC2; guarda el nuevo DNS y actualiza tu IP en el security group.
source "$(dirname "$0")/config.sh"
set +e
DIR="$(cd "$(dirname "$0")" && pwd)"
aws rds start-db-instance --db-instance-identifier "$RDS_ID" >/dev/null 2>&1
aws docdb start-db-cluster --db-cluster-identifier "$DOCDB_CLUSTER" >/dev/null 2>&1
aws ec2 start-instances --instance-ids "$INSTANCE_ID" >/dev/null
echo ">> esperando EC2, RDS y DocumentDB (5-10 min)..."
aws ec2 wait instance-running --instance-ids "$INSTANCE_ID"
NEW_DNS=$(aws ec2 describe-instances --instance-ids "$INSTANCE_ID" --query 'Reservations[0].Instances[0].PublicDnsName' --output text)
save_state PUBLIC_DNS "$NEW_DNS"

echo ">> security group de la EC2: sólo tu IP actual ($MY_IP)"
for port in 22 8080; do
  for old in $(aws ec2 describe-security-groups --group-ids "$EC2_SG_ID" \
      --query "SecurityGroups[0].IpPermissions[?FromPort==\`$port\`].IpRanges[].CidrIp" --output text); do
    [[ "$old" != "$MY_IP" ]] && aws ec2 revoke-security-group-ingress --group-id "$EC2_SG_ID" --protocol tcp --port "$port" --cidr "$old" >/dev/null
  done
  aws ec2 authorize-security-group-ingress --group-id "$EC2_SG_ID" --protocol tcp --port "$port" --cidr "$MY_IP" >/dev/null 2>&1
done

aws rds wait db-instance-available --db-instance-identifier "$RDS_ID"
aws docdb wait db-instance-available --db-instance-identifier "$DOCDB_INSTANCE"
echo ">> esperando Airflow..."
for i in $(seq 1 30); do
  ssh -o ConnectTimeout=5 -o StrictHostKeyChecking=accept-new -o LogLevel=ERROR -i "$KEY_PATH" \
      ec2-user@"$NEW_DNS" 'curl -sf http://localhost:8080/health >/dev/null' 2>/dev/null && break
  sleep 10
done
echo "OK  Airflow: http://$NEW_DNS:8080  (usuario admin)"
echo "    En esta terminal:  source infra/cli/config.sh; set +euo pipefail"
