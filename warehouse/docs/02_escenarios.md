# Escenarios de calidad, incrementalidad y recuperación (sección 3.5)

Prerrequisito: guía 01 completa hasta el paso 9 (carga inicial exitosa).
En cada terminal nueva: `source infra/cli/config.sh; set +euo pipefail`.

Lotes existentes (cada uno es una carpeta en S3):
```bash
aws s3 ls s3://$BUCKET/control/batches/
BATCH1=<el primero de la lista, sin "batch_id=" ni "/">
```
Estado de un lote: `aws s3 cp "s3://$BUCKET/control/batches/batch_id=$BATCH1/status.json" -`

| Escenario | Resultado esperado | Dónde se ve |
|---|---|---|
| 1. Carga inicial | Datos en los modelos analíticos | `marts.*`, `rpt_sales`, Grid verde |
| 2. Registros defectuosos | Rechazos identificados y trazables | `quarantine.*` con `rejection_reason` y `_batch_id` |
| 3. Repetición de un lote exitoso | Sin duplicar hechos ni versiones | conteos antes = después |
| 4. Nuevo lote incremental | Se incorporan los registros nuevos | +3 filas en cada hecho; manifiesto incremental |
| 5. Cambio en atributo historizado | Nueva versión SCD2 y cierre de la anterior | `dim_customer`, `dim_product` |
| 6. Fallo controlado y reejecución | Recuperación sin pérdida ni duplicación | status FAILED → PROCESSED, conteos |

---

## Consulta base (se usa en varios escenarios)

```bash
cat > /tmp/conteos.sql <<'SQL'
SELECT 'fct_sales' AS t, count(*) AS filas, count(DISTINCT sale_sk) AS llaves, sum(subtotal) AS total FROM marts.fct_sales
UNION ALL SELECT 'fct_reviews', count(*), count(DISTINCT review_sk), NULL FROM marts.fct_reviews
UNION ALL SELECT 'fct_navigation_events', count(*), count(DISTINCT navigation_event_sk), NULL FROM marts.fct_navigation_events
UNION ALL SELECT 'dim_customer (versiones)', count(*), count(DISTINCT customer_sk), NULL FROM marts.dim_customer
UNION ALL SELECT 'dim_product (versiones)', count(*), count(DISTINCT product_sk), NULL FROM marts.dim_product;
SQL
conteos() { bash infra/cli/sql.sh "$(cat /tmp/conteos.sql)"; }
```

## 1. Carga inicial

Ya ejecutada en la guía 01 (paso 8).
```bash
conteos | tee evidencias/E1_carga_inicial.txt
```
Con los datos por defecto: `fct_sales` 253 líneas (= llaves), `fct_reviews` 150, `fct_navigation_events` 500,
50 clientes y 25 productos.

📸 **Evidencia:** el archivo, la captura del Grid verde y la del log de `dbt_test`.

## 2. Registros defectuosos

La carga inicial incluyó defectos inyectados (`--bad-records 2`): clientes sin nombre o con correo
inválido, productos sin categoría, pedidos con cliente inexistente o canal inválido, líneas con subtotal
incorrecto o pedido inexistente, reseñas con rating 7 o sin fecha, eventos con tipo inválido, etc.
```bash
bash infra/cli/sql.sh -f /opt/airflow/sql/evidencias.sql | tee evidencias/E2_defectuosos.txt
```
Revisar las secciones **B** (`stg = valid + quarantine`) y **C** (cada rechazo con `rejection_reason` y `_batch_id`).
El lote de origen permite ir al archivo exacto en S3: `raw/<fuente>/<entidad>/batch_id=<_batch_id>/`.

📸 **Evidencia:** el archivo + captura en **Redshift Query editor v2** de
`SELECT * FROM quarantine.quarantine_mysql__order_items;`.

## 3. Repetición de un lote exitoso

```bash
conteos | tee evidencias/E3_antes.txt
bash infra/cli/airflow.sh dags trigger dw_pipeline --conf "{\"reprocess_batch_id\":\"$BATCH1\"}"
# esperar success:  bash infra/cli/airflow.sh dags list-runs -d dw_pipeline | head -4
conteos | tee evidencias/E3_despues.txt
diff evidencias/E3_antes.txt evidencias/E3_despues.txt && echo "SIN CAMBIOS"
```
Qué demuestra: el lote se recarga **desde S3 sin consultar las fuentes** (log de `extract_*`:
*"reprocesamiento: NO se consulta la fuente"*); `load_raw_redshift` borra y vuelve a copiar las filas de ese
`_batch_id` (log: *"reemplazadas N"*); staging toma una sola versión por llave; los hechos son
`delete+insert` por llave subrogada y los snapshots no crean versiones porque `updated_at` no cambió.

📸 **Evidencia:** los dos archivos + `diff` vacío, y captura del log de un `extract_*` del run.

## 4. Nuevo lote incremental

```bash
bash infra/cli/fuentes.sh lote --batch-id 1 | tee evidencias/E4_lote_fuente.txt      # "3 new orders"
bash infra/cli/airflow.sh dags trigger dw_pipeline
# esperar success
conteos | tee evidencias/E4_despues.txt
NUEVO=$(aws s3 ls s3://$BUCKET/control/batches/ | tail -1 | sed 's|.*batch_id=||; s|/$||')
aws s3 cp "s3://$BUCKET/control/batches/batch_id=$NUEVO/manifest.json" - | tee evidencias/E4_manifiesto.json
```
Esperado: cada hecho +3 filas. En el manifiesto, `orders`, `order_items`, `reviews` y `navigation_events`
tienen `"mode": "incremental"`, `watermark_from` = la marca anterior y `rows: 3`;
`customers`/`products` son `full` (dimensiones pequeñas que se copian completas).

Repetir el **mismo** lote en la fuente no agrega nada (`"0 new orders"`), y otra corrida extrae 0 filas nuevas.

📸 **Evidencia:** los archivos + captura de **S3 → raw/mysql/orders/** con las carpetas `batch_id=` de cada lote.

## 5. Cambio en un atributo historizado (SCD tipo 2)

```bash
bash infra/cli/fuentes.sh cambio --customer-id 1 --city Medellín --department Antioquia \
     --product-id 1 --category Hogar --changed-at 2025-07-01 | tee evidencias/E5_cambio.txt
bash infra/cli/airflow.sh dags trigger dw_pipeline
# esperar success
bash infra/cli/sql.sh "SELECT customer_id, city, department, valid_from, valid_to, is_current FROM marts.dim_customer WHERE customer_id = 1 ORDER BY valid_from" | tee evidencias/E5_scd2_cliente.txt
bash infra/cli/sql.sh "SELECT product_id, category, valid_from, valid_to, is_current FROM marts.dim_product WHERE product_id = 1 ORDER BY valid_from" | tee evidencias/E5_scd2_producto.txt
```
Esperado (igual que en local):

| customer_id | city | valid_from | valid_to | is_current |
|---|---|---|---|---|
| 1 | Bogotá | 2024-11-10 | 2025-07-01 | false |
| 1 | Medellín | 2025-07-01 | (vacío) | true |

Las ventas anteriores al 2025-07-01 siguen apuntando a la versión de Bogotá (consulta point-in-time).
Repetir el mismo cambio con la misma fecha no crea otra versión.

📸 **Evidencia:** los archivos + captura de las consultas en Query editor v2.

## 6. Fallo controlado y reejecución

```bash
bash infra/cli/fuentes.sh lote --batch-id 2                     # datos nuevos para este lote
conteos | tee evidencias/E6_antes.txt
bash infra/cli/airflow.sh variables set dw_fail_at dbt_run_marts
bash infra/cli/airflow.sh dags trigger dw_pipeline
# el run queda FAILED en dbt_run_marts; record_batch marca el lote FAILED
FALLIDO=$(aws s3 ls s3://$BUCKET/control/batches/ | tail -1 | sed 's|.*batch_id=||; s|/$||')
aws s3 cp "s3://$BUCKET/control/batches/batch_id=$FALLIDO/status.json" - | tee evidencias/E6_status_fallido.json
aws s3 cp s3://$BUCKET/control/state/watermarks.json - | tee evidencias/E6_marcas_antes.json
bash infra/cli/airflow.sh variables delete dw_fail_at
```
Recuperación: UI → Grid → clic en la tarea roja `dbt_run_marts` → **Clear** (con *Downstream* marcado).
Se reejecutan `dbt_run_marts`, `dbt_test`, `reconcile_sales_kpi`, `refresh_quicksight` y `record_batch` del
**mismo lote**, sin volver a extraer.
```bash
aws s3 cp "s3://$BUCKET/control/batches/batch_id=$FALLIDO/status.json" - | tee evidencias/E6_status_recuperado.json
aws s3 cp s3://$BUCKET/control/state/watermarks.json - | tee evidencias/E6_marcas_despues.json
conteos | tee evidencias/E6_despues.txt
```
Esperado: `status` pasa de `FAILED` a `PROCESSED`; la marca de agua sólo avanza después de la recuperación;
cada hecho +3 filas y `filas = llaves` (sin duplicados).

**Cómo se identifica y recupera una ejecución fallida:** Grid en rojo; log de la tarea con `[batch=<id>]`;
`control/batches/batch_id=<id>/status.json` con `failed_tasks`. Se corrige la causa y se hace *Clear* de la
tarea (o se reprocesa con `reprocess_batch_id`). Como la marca de agua no avanzó, ningún dato se pierde; como
la carga borra y recarga el lote y los hechos son `delete+insert`, nada se duplica.

📸 **Evidencia:** captura del Grid con la tarea roja, captura después del *Clear* (verde) y los archivos.

---

## Conciliación de un indicador (requisito 3.5)

Cada corrida ejecuta `reconcile_sales_kpi` y guarda el resultado:
```bash
aws s3 cp "s3://$BUCKET/control/batches/batch_id=$BATCH1/reconciliation.json" - | tee evidencias/conciliacion.json
```
`origen_total` (suma de `order_items.subtotal` en RDS) − `cuarentena_total` = `marts_fct_sales_total`.
Con los datos por defecto: 7.293.860,26 − 70,00 = **7.293.790,26**.

## Pruebas dbt incluidas

* Genéricas: `unique`, `not_null` de llaves; `relationships` de cada hecho con sus dimensiones;
  `accepted_values` de motivos de rechazo y tipos de evento; `accepted_range` del rating.
* Singulares: calendario completo, festivos por año, fecha de negocio en hora de Bogotá, fechas de venta =
  fechas de pedido, y **totales de ventas por día = fuente validada**.
