#!/usr/bin/env bash
# Apaga lo que cobra por hora, sin borrar datos:
#   EC2 (Airflow), RDS MySQL y DocumentDB se DETIENEN. Redshift Serverless no cobra cómputo si no
#   hay consultas (sólo almacenamiento). S3 queda igual.
# OJO: AWS vuelve a encender RDS y DocumentDB automáticamente a los 7 días de detenidos.
source "$(dirname "$0")/config.sh"
set +e
echo ">> EC2 $INSTANCE_ID";        aws ec2 stop-instances --instance-ids "$INSTANCE_ID" >/dev/null
echo ">> RDS $RDS_ID";             aws rds stop-db-instance --db-instance-identifier "$RDS_ID" >/dev/null
echo ">> DocumentDB $DOCDB_CLUSTER"; aws docdb stop-db-cluster --db-cluster-identifier "$DOCDB_CLUSTER" >/dev/null
aws ec2 wait instance-stopped --instance-ids "$INSTANCE_ID" && echo "OK: EC2 detenida"
echo "RDS y DocumentDB terminan de detenerse en unos minutos (estado 'stopped' en la consola)."
echo "Costo mientras está pausado: discos/almacenamiento (centavos por día)."
