# Decisiones técnicas y diferencias frente a la versión local

## 1. Extracción y almacenamiento en S3 (3.2)

**Ruta:** `s3://<bucket>/raw/<fuente>/<entidad>/batch_id=<id>/part-00000.parquet` + `_manifest.json`
(fuente = `mysql` | `mongodb`; entidad = tabla o colección). Además:
`control/batches/batch_id=<id>/{manifest,status,reconciliation}.json` y `control/state/watermarks.json`.

**Formato Parquet** (Snappy): conserva tipos (`DECIMAL(12,2)`, `TIMESTAMP`, `BOOLEAN`), es columnar y
comprimido, y Redshift lo carga con `COPY ... FORMAT AS PARQUET` sin problemas de separadores, comillas o
saltos de línea dentro de textos (las reseñas y el JSON de los documentos los tienen). CSV obligaría a
definir escapes y perdería tipos; JSON ocupa más y es más lento de cargar.

**Cada extracción registra** en `_manifest.json`: lote, fuente, entidad, modo, marca de agua usada,
llave máxima, número de registros, fecha de extracción UTC y ruta del archivo. Cada fila lleva
`_batch_id`, `_extracted_at` y `_source`.

**Extracción inicial e incremental por lote:**

| Entidad | Modo | Criterio |
|---|---|---|
| customers, products | full | copia completa en cada lote (dimensiones pequeñas: detecta cambios SCD2 y bajas sin depender de `updated_at`) |
| orders, order_items, reviews, navigation_events | incremental | llave `> marca de agua` (los ids de las fuentes son crecientes) |

La primera corrida es la inicial (marca de agua 0 = todo). El parámetro `extraction_mode=full` fuerza una
copia completa (por ejemplo, tras regenerar las fuentes).

**Reprocesar sin consultar las fuentes:** el lote queda en S3; `reprocess_batch_id=<id>` lo recarga.
**Sin sobrescrituras silenciosas:** el Parquet se escribe con `If-None-Match: *` (S3 rechaza escribir
sobre un objeto existente) y el bucket es versionado.

## 2. Identificación de lotes y marca de avance

* `batch_id` = id de la corrida de Airflow normalizado (p. ej. `manual__20261101T150000Z`).
* `status.json` del lote pasa por `EXTRACTED → LOADED → PROCESSED` (o `FAILED` con `failed_tasks`).
* **La marca de agua se actualiza sólo al final** (`record_batch`), cuando la carga, dbt y las pruebas
  terminaron bien. Si un lote falla, la marca no avanza: el siguiente lote (o el *Clear* del mismo)
  vuelve a cubrir esos registros y no se pierde nada.

## 3. Carga a Redshift (3.3)

* `COPY` desde S3 autorizado con el rol IAM `tallerdw-redshift-s3-role` asociado al namespace (sin llaves).
* Por entidad, en **una transacción**: `DELETE ... WHERE _batch_id = <lote>` + `COPY` + verificación
  de filas contra el manifiesto. Reprocesar el mismo lote no duplica.
* `raw` **acumula** todos los lotes (historial de lo recibido). Staging elige la versión vigente:
  * entidades full → la última copia completa;
  * entidades incrementales → la versión más reciente de cada llave (`row_number()`).

## 4. Capas y modelo (se conservan de DE-DW)

`raw` → `intermediate` (`stg_*`, `int_*__qa`, `int_*__valid`) → `quarantine` → `snapshots` → `marts`
(3 hechos + 6 dimensiones + vista `rpt_sales` para QuickSight). Mismas reglas de calidad, mismas pruebas,
mismos snapshots SCD2 (estrategia `timestamp` sobre `updated_at`) y los mismos hechos incrementales
(`delete+insert`, ventana de 7 días, reapuntado cuando cambian las llaves SCD2, borrado de huérfanos).
Los rechazos de cuarentena incluyen `rejection_reason` **y `_batch_id`** (lote de origen).

## 5. Diferencias SQL DuckDB → Redshift

| DuckDB (local) | Redshift (AWS) | Solución |
|---|---|---|
| `SELECT * EXCLUDE (col)` | no existe | columnas explícitas en `int_*__valid` |
| `timezone('America/Bogota', ts)` sobre `TIMESTAMPTZ` | `convert_timezone('UTC','America/Bogota', ts)` | macro `to_business_tz` (dispatch por motor) |
| `IS DISTINCT FROM` | no soportado | macro `null_safe_distinct` |
| `extract(isodow ...)` | no existe | macro `iso_day_of_week` (desde `dow`) |
| `datediff('day', a, b)` | sintaxis distinta | resta de fechas `b - a` |
| `json_extract_string(document_json, ...)` | funciones JSON distintas | `is_verified_purchase` se extrae al leer el documento |
| tipo `JSON`, `TIMESTAMPTZ` en raw | `VARCHAR(65535)`, `TIMESTAMP` en UTC | definidos en `entities.py` |
| `VARCHAR` sin límite | `VARCHAR` = 256 bytes por defecto | longitudes explícitas en raw y seeds |
| vistas normales | vistas *late-binding* (`bind: false`) | recrear una tabla no borra las vistas que dependen de ella |
| bloqueo de archivo (`warehouse.lock`) | concurrencia del motor | `max_active_runs=1` en el DAG |
| `EXISTS (... WHERE existing.k = candidate.k)` en el filtro incremental | Redshift: *This type of correlated subquery pattern is not supported* | macro `incremental_candidate_filter` reescrita con `LEFT JOIN` a la tabla actual + `CROSS JOIN` de la ventana (misma lógica) |
| esquema `raw` sin comillas | `RAW` es **palabra reservada** en Redshift (`syntax error at or near "raw"`) | el cargador usa `"raw"`; dbt ya cita los esquemas (`"dev"."raw"."mysql_orders"`) |
| perfil `dbt-duckdb` con ruta de archivo | `dbt-redshift` con host/usuario por variables de entorno | `profiles.yml` sin secretos |

## 6. Diferencias MongoDB → DocumentDB

| Tema | MongoDB local | DocumentDB | Ajuste |
|---|---|---|---|
| Red | `localhost:27017` | sólo dentro de la VPC, sin endpoint público | se opera desde la EC2 (`fuentes.sh`) |
| TLS | opcional | obligatorio | URI con `tls=true&tlsCAFile=/opt/certs/global-bundle.pem` (bundle de AWS en la imagen) |
| Escrituras reintentables | por defecto | no soportadas | `retryWrites=false` en la URI |
| Replica set | — | `rs0` | `replicaSet=rs0&readPreference=secondaryPreferred` |
| Validación `$jsonSchema` / `collMod` | completa | soporte parcial según versión | el generador intenta crear el validador y, si el motor lo rechaza, sigue sin él y lo avisa; la calidad se garantiza en dbt |
| `bypassDocumentValidation` | soportado | puede no estarlo | se intenta y, si falla, se inserta sin el parámetro |
| Fechas | UTC | UTC | igual: se convierten a hora de Bogotá en dbt |
| Usuario | `root` en `admin` | usuario maestro del clúster | `dwadmin` (contraseña en SSM) |

**Verificado en el despliegue real:** DocumentDB 5.0 aceptó los validadores `$jsonSchema` (no fue necesario el modo sin validador).

## 6b. Hallazgos durante el despliegue real (fallos reales y su recuperación)

| Tarea que falló | Error | Causa | Corrección y recuperación |
|---|---|---|---|
| `extract_mysql_products` | `ArrowInvalid: Could not convert 1 with type int: tried to convert to boolean` | MySQL devuelve `BOOLEAN` como `TINYINT` (1/0) y el esquema Parquet espera booleano | conversión a `bool` en `extract.py`; *Clear* de la tarea con *Downstream*: las otras 5 extracciones no se repitieron (ya estaban en S3) |
| `load_raw_redshift` | `syntax error at or near "raw"` | `RAW` es palabra reservada en Redshift | esquema citado `"raw"`; *Clear* desde la carga: el lote se recargó desde S3 sin volver a consultar las fuentes |

| `dbt_run_marts` (2.ª corrida, la primera incremental) | `This type of correlated subquery pattern is not supported` | el filtro incremental usaba una subconsulta correlacionada; la 1.ª corrida no lo usa porque crea las tablas desde cero | macro reescrita con `LEFT JOIN`; *Clear* de `dbt_run_marts`: raw, staging y snapshots no se recalcularon |

Los tres casos muestran el mecanismo de recuperación: la marca de agua no avanzó hasta terminar bien, y reejecutar no duplicó datos.

## 7. Diferencias generales

* **Secretos:** de `.env` local con contraseñas fijas → SSM Parameter Store (SecureString); la EC2 genera
  su `docker/.env` al arrancar. El repositorio sólo tiene `.env.example`.
* **Red:** fuentes y Redshift en subredes de la VPC con security groups por origen; nada expuesto salvo
  Airflow (sólo tu IP) y Redshift (sólo QuickSight y la VPC).
* **Fuentes de datos:** los generadores (`generate_data`, `append_batch`, `simulate_changes`) son los mismos,
  con la conexión adaptada (`connections.py`).
* **Orquestación:** los tres DAGs locales (`ecommerce_pipeline`, `extract_to_raw`, `transform_warehouse`)
  se unieron en uno (`dw_pipeline`) con las etapas pedidas en 3.4 y registro del resultado.
