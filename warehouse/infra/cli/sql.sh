#!/usr/bin/env bash
# Ejecuta SQL en Redshift desde la EC2 e imprime el resultado (para evidencias en texto).
#   bash infra/cli/sql.sh "select count(*) from marts.fct_sales"
#   bash infra/cli/sql.sh -f /opt/airflow/sql/validacion_kpi.sql
source "$(dirname "$0")/config.sh" >/dev/null
SSH="ssh -o StrictHostKeyChecking=accept-new -o LogLevel=ERROR -i $KEY_PATH ec2-user@$PUBLIC_DNS"
$SSH "cd warehouse/docker && docker compose exec -T airflow-scheduler python /opt/airflow/tools/sql.py $(printf '%q ' "$@")"
