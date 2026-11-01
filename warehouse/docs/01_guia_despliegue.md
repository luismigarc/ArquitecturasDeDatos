# Guía de despliegue y ejecución (AWS CLI desde WSL)

Tiempo total: ~1 h 30 min (la mitad es esperar a que AWS cree las bases de datos).
Cada paso termina con **📸 Evidencia**: qué guardar para la entrega (captura o archivo de texto en `evidencias/`).

> **Costos:** mientras todo está encendido cuesta aprox. **US$0,3/h + uso de Redshift**
> (ver [05_costos_seguridad.md](05_costos_seguridad.md)). Al terminar cada sesión: `bash infra/cli/pausar.sh`.
> Al terminar el taller: `bash infra/cli/99_destroy.sh`.

## 0. Requisitos

* WSL (Ubuntu) con **AWS CLI v2** y sesión iniciada (`aws login` o credenciales de un usuario IAM).
  En AWS Academy Learner Lab **no** se puede: el taller crea roles IAM y Redshift Serverless.
* Región **us-east-1**.

```bash
cd /mnt/c/Users/<usuario>/Documents/Uni/ingData/warehouse
aws configure set region us-east-1
aws sts get-caller-identity          # muestra la cuenta
mkdir -p evidencias
```
Todos los scripts se ejecutan con `bash infra/cli/<script>.sh` desde la raíz del repo.
Si abre una terminal nueva: `source infra/cli/config.sh; set +euo pipefail` para cargar las variables.

📸 **Evidencia:** ninguna.

---

## 1. Red y bucket S3

```bash
bash infra/cli/01_red_s3.sh
```
Crea tres security groups y el bucket privado:

| Security group | Permite | Desde |
|---|---|---|
| `tallerdw-ec2-sg` | 22 (SSH), 8080 (Airflow) | sólo tu IP /32 |
| `tallerdw-db-sg` | 3306 (MySQL), 27017 (DocumentDB) | sólo la EC2 |
| `tallerdw-redshift-sg` | 5439 | la EC2/VPC y el rango de QuickSight |

Bucket `tallerdw-<cuenta>-<sufijo>`: Block Public Access, cifrado SSE-S3, versionado, política que niega tráfico sin TLS.

📸 **Evidencia:** captura de la consola **S3 → bucket → Permissions** (Block public access *On* y la bucket policy).

## 2. Contraseñas (SSM) y roles IAM

```bash
bash infra/cli/02_iam_secretos.sh
```
* Genera 5 contraseñas aleatorias y las guarda **cifradas** en SSM Parameter Store (`/tallerdw/*`).
  Nunca se escriben en el repositorio.
* `tallerdw-redshift-s3-role`: lo usa Redshift para `COPY` (sólo lectura de `raw/` del bucket).
* `tallerdw-ec2-role`: la EC2 lee/escribe el bucket, lee `/tallerdw/*` y refresca QuickSight.

📸 **Evidencia:** captura de **Systems Manager → Parameter Store** (5 parámetros tipo *SecureString*)
y de **IAM → Roles → tallerdw-redshift-s3-role → Permissions**.

## 3. Fuentes: RDS MySQL y DocumentDB (≈ 10-15 min)

```bash
bash infra/cli/03_bases.sh
```
* RDS for MySQL 8.0, `db.t3.micro`, **no pública**, cifrada, base `ecommerce`.
* DocumentDB 5.0, 1 instancia `db.t3.medium`, **no pública**, TLS obligatorio, cifrada.
El script espera hasta que ambas queden *available* y guarda sus endpoints.

📸 **Evidencia:** captura de **RDS → Databases** con `tallerdw-mysql` y `tallerdw-docdb` en estado *Available*
(DocumentDB aparece en su propia consola: **Amazon DocumentDB → Clusters**).

## 4. Redshift Serverless (≈ 5 min)

```bash
bash infra/cli/04_redshift.sh
```
Namespace `tallerdw-ns` (base `dev`, usuario `dwadmin`, rol de COPY por defecto) y workgroup
`tallerdw-wg` de **8 RPU** (el mínimo) con un límite de uso diario de 10 RPU-hora.

> Cuentas nuevas suelen tener **crédito de prueba de US$300** para Redshift Serverless.

📸 **Evidencia:** captura de **Amazon Redshift → Serverless dashboard → Workgroup** (*Available*, 8 RPU) y de
**Namespace → Security and encryption** mostrando el rol IAM asociado.

## 5. Subir el código

```bash
bash infra/cli/05_codigo.sh
```
📸 **Evidencia:** ninguna.

## 6. EC2 con Airflow + dbt (≈ 10-12 min)

```bash
bash infra/cli/06_ec2.sh
source infra/cli/config.sh; set +euo pipefail
ssh -i $KEY_PATH ec2-user@$PUBLIC_DNS 'sudo tail -f /var/log/user-data.log'   # Ctrl+C al ver "AIRFLOW LISTO"
```
Contraseña del usuario `admin` de Airflow:
```bash
aws ssm get-parameter --name /tallerdw/airflow_admin_password --with-decryption --query Parameter.Value --output text
echo "http://$PUBLIC_DNS:8080"
```
El user data genera `docker/.env` **dentro de la EC2** leyendo SSM (archivo con permisos 600).

📸 **Evidencia:** guardar la configuración de la instancia:
```bash
aws ec2 describe-instances --instance-ids $INSTANCE_ID --query 'Reservations[0].Instances[0].{Tipo:InstanceType,Rol:IamInstanceProfile.Arn,IMDSv2:MetadataOptions.HttpTokens,HopLimit:MetadataOptions.HttpPutResponseHopLimit}' --output table | tee evidencias/06_ec2.txt
```

## 7. Datos iniciales en las fuentes

```bash
bash infra/cli/fuentes.sh generar --bad-records 2 | tee evidencias/07_fuentes.txt
```
Crea el esquema MySQL en RDS y las colecciones en DocumentDB, con los mismos datos sintéticos del
proyecto local (`--seed 42`): 50 clientes, 25 productos, 100 pedidos, 150 reseñas, 500 eventos,
más registros defectuosos inyectados a propósito.
Si DocumentDB rechaza el validador `$jsonSchema`, el script avisa y sigue (ver diferencias en el doc 04).

📸 **Evidencia:** el archivo `evidencias/07_fuentes.txt` con los conteos generados.
(Volver a ejecutarlo reinicia las fuentes; si lo hace después de cargar lotes, ejecute luego el DAG
con `extraction_mode = full` para que las marcas de agua no dejen datos por fuera.)

## 8. Primera ejecución del pipeline (carga inicial)

En la UI de Airflow: activar `dw_pipeline` → ▶ **Trigger DAG** (dejar los parámetros por defecto).
O por terminal:
```bash
bash infra/cli/airflow.sh dags unpause dw_pipeline
bash infra/cli/airflow.sh dags trigger dw_pipeline
bash infra/cli/airflow.sh dags list-runs -d dw_pipeline
```
Tarda ~4-6 min (la primera vez dbt descarga paquetes).

📸 **Evidencia (requerida: "ejecución exitosa del pipeline en Airflow"):**
* Captura del **Grid** con las 19 tareas en verde.
* Captura del log de `load_raw_redshift` (líneas `[batch=...] N filas en raw.mysql_...`).
* Captura del log de `dbt_test` (resumen `PASS=... ERROR=0`) — **"resultados de las pruebas de dbt"**.

## 9. Usuario de sólo lectura para QuickSight

```bash
bash infra/cli/07_usuario_quicksight.sh
```
📸 **Evidencia:** ninguna (se usa en el doc 03).

## 10. Evidencias de datos (texto)

```bash
bash infra/cli/sql.sh -f /opt/airflow/sql/evidencias.sql | tee evidencias/10_evidencias_redshift.txt
aws s3 ls s3://$BUCKET/raw/ --recursive | tee evidencias/10_s3_raw.txt
aws s3 ls s3://$BUCKET/control/batches/ --recursive | tee evidencias/10_s3_control.txt
```
📸 **Evidencia:** esos tres archivos, más una captura de **S3 → raw/mysql/orders/batch_id=…/** mostrando
`part-00000.parquet` y `_manifest.json`.

## 11. Escenarios, dashboard y validación

* Escenarios de la sección 3.5: [02_escenarios.md](02_escenarios.md)
* Dashboard y consulta de validación: [03_quicksight.md](03_quicksight.md)

## 12. Pausar entre sesiones / reanudar

```bash
bash infra/cli/pausar.sh      # detiene EC2, RDS y DocumentDB (Redshift Serverless no cobra si no se usa)
bash infra/cli/reanudar.sh    # enciende todo, actualiza tu IP en el SG y el nuevo DNS
source infra/cli/config.sh; set +euo pipefail
```
AWS vuelve a encender RDS y DocumentDB solos a los 7 días de detenidos.

## 13. Eliminar todo

```bash
bash infra/cli/99_destroy.sh      # escribir BORRAR
```
Y en QuickSight: borrar el dashboard, el dataset y el data source; si ya no se usa, cancelar la suscripción
(**Manage QuickSight → Account settings → Unsubscribe**).

📸 **Evidencia:** captura de la consola sin recursos `tallerdw-*` (EC2, RDS, Redshift).

---

## Problemas frecuentes

| Síntoma | Causa / solución |
|---|---|
| Un script termina sin mensaje | Falló un comando con `set -e`. Ejecutarlo con `bash -x infra/cli/<script>.sh 2>&1 \| tail -30` |
| `validate_connections` falla con *timeout* a MySQL o DocumentDB | La base está detenida o el SG `tallerdw-db-sg` no permite a la EC2. Ver estado en la consola |
| `load_raw_redshift` falla con *S3ServiceException / Access denied* | Falta el rol en el namespace o el bucket no coincide. Revisar `REDSHIFT_IAM_ROLE_ARN` en `docker/.env` de la EC2 |
| dbt: *could not connect to server* | Workgroup no disponible o SG `tallerdw-redshift-sg` sin la regla de la VPC |
| La UI de Airflow no abre | Cambió tu IP: `bash infra/cli/reanudar.sh` la actualiza |
| `04_redshift.sh` falla con un error de subred/zona | Quitar la subred de esa zona del filtro `SUBNETS` del script y repetir |
