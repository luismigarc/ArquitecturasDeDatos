#!/bin/bash
# User data de la EC2 (Amazon Linux 2023): Docker + Compose, código desde S3, .env desde SSM, Airflow.
set -euxo pipefail
exec > >(tee /var/log/user-data.log) 2>&1

BUCKET="__BUCKET__"; REGION="__REGION__"; SSM="__SSM_PREFIX__"
APP=/home/ec2-user/warehouse

if [ ! -f /swapfile ]; then
  dd if=/dev/zero of=/swapfile bs=1M count=2048 && chmod 600 /swapfile && mkswap /swapfile && swapon /swapfile
  echo '/swapfile swap swap defaults 0 0' >> /etc/fstab
fi
dnf install -y docker git python3-pip
systemctl enable --now docker
usermod -aG docker ec2-user
mkdir -p /usr/local/lib/docker/cli-plugins
curl -fsSL "https://github.com/docker/compose/releases/download/v2.29.7/docker-compose-linux-x86_64" \
  -o /usr/local/lib/docker/cli-plugins/docker-compose
curl -fsSL "https://github.com/docker/buildx/releases/download/v0.17.1/buildx-v0.17.1.linux-amd64" \
  -o /usr/local/lib/docker/cli-plugins/docker-buildx
chmod +x /usr/local/lib/docker/cli-plugins/*

mkdir -p "$APP"
aws s3 sync "s3://$BUCKET/deploy/code/" "$APP/" --region "$REGION"
cat > /usr/local/bin/sync_code <<EOS
#!/bin/bash
aws s3 sync "s3://$BUCKET/deploy/code/" "$APP/" --region "$REGION" --exclude "docker/.env" --exclude "docker/logs/*"
EOS
chmod +x /usr/local/bin/sync_code

ssm() { aws ssm get-parameter --region "$REGION" --name "$SSM/$1" --with-decryption --query Parameter.Value --output text; }
if [ ! -f "$APP/docker/.env" ]; then
  cat > "$APP/docker/.env" <<EOS
DW_BUCKET=$BUCKET
AWS_REGION=$REGION
AWS_DEFAULT_REGION=$REGION
AWS_ACCOUNT_ID=__ACCOUNT__
MYSQL_HOST=__RDS_HOST__
MYSQL_PORT=3306
MYSQL_USER=__DB_ADMIN__
MYSQL_PASSWORD=$(ssm mysql_password)
MYSQL_DATABASE=ecommerce
MONGODB_URI=mongodb://__DB_ADMIN__:$(ssm docdb_password)@__DOCDB_HOST__:27017/?tls=true&tlsCAFile=/opt/certs/global-bundle.pem&replicaSet=rs0&readPreference=secondaryPreferred&retryWrites=false
MONGO_DATABASE=ecommerce
REDSHIFT_HOST=__RS_HOST__
REDSHIFT_PORT=5439
REDSHIFT_DB=dev
REDSHIFT_USER=__DB_ADMIN__
REDSHIFT_PASSWORD=$(ssm redshift_password)
REDSHIFT_IAM_ROLE_ARN=__RS_ROLE_ARN__
QUICKSIGHT_RO_PASSWORD=$(ssm quicksight_password)
WAREHOUSE_ENGINE=redshift
DBT_TARGET=redshift
QUICKSIGHT_DATASET_ID=
DW_SCHEDULE=
AIRFLOW_UID=1000
AIRFLOW_ADMIN_USER=admin
AIRFLOW_ADMIN_PASSWORD=$(ssm airflow_admin_password)
POSTGRES_PASSWORD=$(openssl rand -hex 16)
AIRFLOW_FERNET_KEY=$(python3 -c "import base64,os;print(base64.urlsafe_b64encode(os.urandom(32)).decode())")
AIRFLOW_WEBSERVER_SECRET_KEY=$(openssl rand -hex 32)
EOS
  chmod 600 "$APP/docker/.env"
fi
chown -R ec2-user:ec2-user "$APP"

cd "$APP/docker"
sudo -u ec2-user docker compose build
sudo -u ec2-user docker compose up airflow-init
sudo -u ec2-user docker compose up -d
echo "AIRFLOW LISTO"
