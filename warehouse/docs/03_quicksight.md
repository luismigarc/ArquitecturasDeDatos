# Dashboard en Amazon QuickSight (sección 3.6)

El dashboard consume la vista **`marts.rpt_sales`** de Redshift: una fila por línea de venta con
fecha, categoría, departamento, ciudad y canal ya resueltos (la categoría y la región son las
**vigentes cuando ocurrió la venta**, gracias a SCD2).

Prerrequisitos: carga inicial exitosa y `bash infra/cli/07_usuario_quicksight.sh` ejecutado
(crea `quicksight_ro`, que sólo puede leer `marts`).

## 1. Activar QuickSight (una vez)

Consola → **QuickSight** → *Sign up for QuickSight*.
* Edición **Standard** basta (Enterprise también sirve). Hay prueba gratuita de 30 días.
* Región: **US East (N. Virginia)**. Correo de notificación: el suyo.
* No hace falta dar acceso a S3 ni a otros servicios.

> Costo tras la prueba: por usuario autor al mes. Cancelar al terminar (ver guía 01, paso 13).

📸 **Evidencia:** ninguna.

## 2. Data source y dataset

**Datasets → New dataset → Redshift (Manual connect)**:

| Campo | Valor |
|---|---|
| Data source name | `tallerdw-redshift` |
| Database server | salida de `07_usuario_quicksight.sh` (`tallerdw-wg.<cuenta>.us-east-1.redshift-serverless.amazonaws.com`) |
| Port | `5439` |
| Database name | `dev` |
| Username | `quicksight_ro` |
| Password | `aws ssm get-parameter --name /tallerdw/quicksight_password --with-decryption --query Parameter.Value --output text` |

*Validate connection* → *Create data source*. Elegir **Schema: `marts`**, **Table: `rpt_sales`** → *Select*.
Elegir **Directly query your data** (recomendado: siempre muestra el último lote, sin refrescos)
o **Import to SPICE** (ver sección 5) → *Visualize*.

Si *Validate connection* falla por tiempo de espera: el SG `tallerdw-redshift-sg` debe permitir
`52.23.63.224/27` en 5439 (lo hace `01_red_s3.sh`) y el workgroup debe ser *publicly accessible*.

📸 **Evidencia:** captura del dataset `rpt_sales` con sus columnas.

## 3. Análisis y dashboard

En el análisis (hoja *Interactive sheet*):

1. **Gráfica de ventas por fecha** — *Line chart*: X = `fecha` (agregación **Month**), Value = `ventas` (**Sum**).
   (Alternativa: *Bar chart* con X = `categoria`.)
2. **Indicador de ventas totales** — *KPI*: Value = `ventas` (**Sum**). Título: *Ventas totales*.
3. **Filtro** — panel *Filters* → *Add filter* → `categoria` → *Add to sheet* (control desplegable).
   Opcional: segundo filtro por `departamento` o por rango de `fecha`.
4. *Publish* → *Publish dashboard* → nombre `Ventas e-commerce (tallerdw)`.

📸 **Evidencia (requerida):** captura del dashboard publicado **sin filtro** y otra **con el filtro**
`categoria = Electrónica` aplicado, mostrando el KPI.

## 4. Validar el indicador con SQL

La consulta de validación está en [`sql/validacion_kpi.sql`](../sql/validacion_kpi.sql):
```bash
bash infra/cli/sql.sh -f /opt/airflow/sql/validacion_kpi.sql | tee evidencias/quicksight_validacion.txt
```
o en **Redshift Query editor v2** (workgroup `tallerdw-wg`, base `dev`, usuario `dwadmin`; la contraseña
está en `/tallerdw/redshift_password`).

* Consulta 1 = KPI sin filtro. Consulta 2 = KPI con `categoria = 'Electrónica'` (el mismo filtro del dashboard).
* Consulta 4 = fecha de la última extracción: los datos que ve el dashboard corresponden a ese lote.
* Consulta 5 = conciliación origen válido = `fct_sales` = vista del dashboard.

Los valores deben ser **iguales** a los del KPI en QuickSight con los mismos filtros. Con los datos por
defecto, el total sin filtro es **7.293.790,26** (antes de lotes adicionales).

📸 **Evidencia (requerida):** el archivo + captura lado a lado del KPI y del resultado SQL.

## 5. Si se usa SPICE: actualización después de cada lote

SPICE guarda una copia; no ve un lote nuevo hasta que se refresca. Tres opciones:

1. **Manual:** Datasets → `rpt_sales` → *Refresh now*.
2. **Programada:** Datasets → `rpt_sales` → *Schedule refresh* (p. ej. cada hora).
3. **Automática al final del DAG (implementada):** la tarea `refresh_quicksight` llama a
   `CreateIngestion` si la EC2 tiene el id del dataset:
   ```bash
   aws quicksight list-data-sets --aws-account-id $ACCOUNT_ID --query "DataSetSummaries[?Name=='rpt_sales'].DataSetId" --output text
   ssh -i $KEY_PATH ec2-user@$PUBLIC_DNS
   sed -i 's/^QUICKSIGHT_DATASET_ID=.*/QUICKSIGHT_DATASET_ID=<id>/' ~/warehouse/docker/.env
   cd ~/warehouse/docker && docker compose up -d     # recrea los contenedores con la nueva variable
   ```
   Desde entonces cada lote exitoso dispara el refresco (se ve en *Datasets → rpt_sales → Refresh history*).

Con **Direct Query** la tarea queda en *skipped* y no se requiere nada.
