#!/usr/bin/env bash
# Operaciones sobre las fuentes (RDS MySQL y DocumentDB) desde la EC2 (están en la VPC privada).
#   bash infra/cli/fuentes.sh generar [--bad-records 2] [--seed 42]   # crea esquema y datos iniciales
#   bash infra/cli/fuentes.sh lote --batch-id 1                       # agrega un lote diario
#   bash infra/cli/fuentes.sh cambio --customer-id 1 --city Medellín --department Antioquia \
#                                    --product-id 1 --category Hogar --changed-at 2025-07-01
source "$(dirname "$0")/config.sh" >/dev/null
SSH="ssh -o StrictHostKeyChecking=accept-new -o LogLevel=ERROR -i $KEY_PATH ec2-user@$PUBLIC_DNS"
cmd="${1:-}"; shift || true
case "$cmd" in
  generar) mod=ecommerce_dw.sources.generate_data ;;
  lote)    mod=ecommerce_dw.sources.append_batch ;;
  cambio)  mod=ecommerce_dw.sources.simulate_changes ;;
  *) echo "uso: $0 generar|lote|cambio [opciones]"; exit 2 ;;
esac
$SSH "cd warehouse/docker && docker compose exec -T airflow-scheduler python -m $mod $(printf '%q ' "$@")"
