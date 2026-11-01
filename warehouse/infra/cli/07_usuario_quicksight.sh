#!/usr/bin/env bash
# Paso 7: usuario de sólo lectura en Redshift para QuickSight (SELECT sobre marts).
source "$(dirname "$0")/config.sh"
SSH="ssh -o StrictHostKeyChecking=accept-new -o LogLevel=ERROR -i $KEY_PATH ec2-user@$PUBLIC_DNS"
$SSH "cd warehouse/docker && docker compose exec -T airflow-scheduler python /opt/airflow/tools/redshift_reporting_user.py"
echo
echo "Datos para crear el data source en QuickSight:"
echo "  Servidor:   $RS_HOST"
echo "  Puerto:     5439      Base: $RS_DB      Usuario: quicksight_ro"
echo "  Contraseña: aws ssm get-parameter --name $SSM_PREFIX/quicksight_password --with-decryption --query Parameter.Value --output text"
