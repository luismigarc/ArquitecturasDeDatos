#!/usr/bin/env bash
# Configuración común (WSL/Ubuntu + AWS CLI v2). Uso: source infra/cli/config.sh; set +euo pipefail
set -euo pipefail

export AWS_REGION="${AWS_REGION:-us-east-1}"
export AWS_DEFAULT_REGION="$AWS_REGION"
export AWS_PAGER=""
export PROJECT="tallerdw"
export ACCOUNT_ID="$(aws sts get-caller-identity --query Account --output text)"

STATE_FILE="$(dirname "${BASH_SOURCE[0]}")/.state.env"
touch "$STATE_FILE"
# shellcheck disable=SC1090
source "$STATE_FILE"
if [[ -z "${SUFFIX:-}" ]]; then
  SUFFIX="$(od -An -N3 -tx1 /dev/urandom | tr -d ' \n')"
  echo "SUFFIX=$SUFFIX" >> "$STATE_FILE"
fi
export SUFFIX
export BUCKET="${PROJECT}-${ACCOUNT_ID}-${SUFFIX}"

# Nombres de recursos
export SG_EC2="${PROJECT}-ec2-sg" SG_DB="${PROJECT}-db-sg" SG_RS="${PROJECT}-redshift-sg"
export DB_SUBNET_GROUP="${PROJECT}-db-subnets"
export RDS_ID="${PROJECT}-mysql" DOCDB_CLUSTER="${PROJECT}-docdb" DOCDB_INSTANCE="${PROJECT}-docdb-1"
export RS_NAMESPACE="${PROJECT}-ns" RS_WORKGROUP="${PROJECT}-wg" RS_DB="dev"
export DB_ADMIN="dwadmin"
export EC2_ROLE="${PROJECT}-ec2-role" EC2_PROFILE="${PROJECT}-ec2-profile" RS_ROLE="${PROJECT}-redshift-s3-role"
export SSM_PREFIX="/${PROJECT}"
export INSTANCE_TYPE="${INSTANCE_TYPE:-t3.medium}"
export KEY_NAME="${KEY_NAME:-${PROJECT}-key}"
export KEY_PATH="${KEY_PATH:-$HOME/.ssh/${KEY_NAME}.pem}"
export MY_IP="${MY_IP:-$(curl -s https://checkip.amazonaws.com | tr -d '[:space:]')/32}"
# Rango de IPs de Amazon QuickSight en us-east-1 (para que QuickSight llegue a Redshift).
export QUICKSIGHT_CIDR="${QUICKSIGHT_CIDR:-52.23.63.224/27}"

export VPC_ID="${VPC_ID:-$(aws ec2 describe-vpcs --filters Name=isDefault,Values=true --query 'Vpcs[0].VpcId' --output text)}"
export VPC_CIDR="${VPC_CIDR:-$(aws ec2 describe-vpcs --vpc-ids "$VPC_ID" --query 'Vpcs[0].CidrBlock' --output text)}"

save_state() {  # save_state CLAVE valor
  grep -v "^$1=" "$STATE_FILE" > "$STATE_FILE.tmp" || true
  mv "$STATE_FILE.tmp" "$STATE_FILE"
  echo "$1=$2" >> "$STATE_FILE"
  export "$1=$2"
}

sg_id() {  # sg_id NOMBRE -> id o None
  aws ec2 describe-security-groups --filters Name=group-name,Values="$1" Name=vpc-id,Values="$VPC_ID" \
    --query 'SecurityGroups[0].GroupId' --output text
}

gen_password() {  # 20 alfanuméricos + Aa9 (cumple reglas de RDS, DocumentDB y Redshift)
  echo "$(head -c 48 /dev/urandom | base64 | tr -dc 'A-Za-z0-9' | cut -c1-20)Aa9"
}

echo ">> cuenta=$ACCOUNT_ID region=$AWS_REGION bucket=$BUCKET vpc=$VPC_ID mi_ip=$MY_IP"
