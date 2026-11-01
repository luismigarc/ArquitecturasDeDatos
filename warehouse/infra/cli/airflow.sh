#!/usr/bin/env bash
# CLI de Airflow dentro de la EC2 por SSH.
#   bash infra/cli/airflow.sh dags unpause dw_pipeline
#   bash infra/cli/airflow.sh dags trigger dw_pipeline
#   bash infra/cli/airflow.sh dags trigger dw_pipeline --conf '{"reprocess_batch_id":"<id>"}'
#   bash infra/cli/airflow.sh dags list-runs -d dw_pipeline
#   bash infra/cli/airflow.sh variables set dw_fail_at dbt_run_marts
#   bash infra/cli/airflow.sh sync      # descarga el código nuevo (tras 05_codigo.sh) y reinicia
source "$(dirname "$0")/config.sh" >/dev/null
SSH="ssh -o StrictHostKeyChecking=accept-new -o LogLevel=ERROR -i $KEY_PATH ec2-user@$PUBLIC_DNS"
if [[ "${1:-}" == "sync" ]]; then
  $SSH "sync_code && cd warehouse/docker && docker compose restart airflow-scheduler airflow-webserver"
  exit 0
fi
$SSH "cd warehouse/docker && docker compose exec -T airflow-scheduler airflow $(printf '%q ' "$@")"
