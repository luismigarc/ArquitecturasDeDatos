#!/usr/bin/env bash
# Paso 5: sube el código al bucket (la EC2 lo descarga). Repetir tras cambiar código y luego:
#   bash infra/cli/airflow.sh sync
source "$(dirname "$0")/config.sh"
ROOT="$(cd "$(dirname "$0")/../.." && pwd)"
aws s3 sync "$ROOT" "s3://$BUCKET/deploy/code/" --delete --exclude "*" \
  --include "dags/*" --include "src/*" --include "docker/*" --include "tools/*" --include "sql/*" \
  --exclude "*__pycache__*" --exclude "src/dbt/target/*" --exclude "src/dbt/dbt_packages/*" \
  --exclude "src/dbt/logs/*" --exclude "docker/.env" --exclude "docker/logs/*"
echo "OK -> s3://$BUCKET/deploy/code/"
