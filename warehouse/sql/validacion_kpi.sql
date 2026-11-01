-- =====================================================================================
-- Validación del indicador del dashboard de QuickSight (requisito 3.6)
-- Ejecutar en Redshift (Query editor v2 o: bash infra/cli/sql.sh -f /opt/airflow/sql/validacion_kpi.sql)
-- Usar los MISMOS filtros que el dashboard. Ejemplo: sin filtro y con filtro categoria = 'Electrónica'.
-- =====================================================================================

-- 1. Ventas totales (KPI del dashboard, sin filtros)
SELECT sum(ventas) AS ventas_totales, sum(unidades) AS unidades, count(*) AS lineas
FROM marts.rpt_sales;

-- 2. Mismo KPI con el filtro usado en el dashboard (cambiar la categoría si aplica)
SELECT sum(ventas) AS ventas_totales_electronica
FROM marts.rpt_sales
WHERE categoria = 'Electrónica';

-- 3. Ventas por mes (lo que muestra la gráfica)
SELECT anio, mes, sum(ventas) AS ventas
FROM marts.rpt_sales
GROUP BY anio, mes
ORDER BY anio, mes;

-- 4. Fecha de actualización de los datos que ve el dashboard (último lote procesado)
SELECT max(_extracted_at) AS ultima_extraccion_utc, count(distinct _batch_id) AS lotes_en_raw
FROM "raw".mysql_order_items;

-- 5. Conciliación origen válido vs modelo (debe dar la misma suma)
SELECT
  (SELECT sum(subtotal) FROM intermediate.int_mysql__order_items__valid) AS origen_valido,
  (SELECT sum(subtotal) FROM marts.fct_sales)                            AS modelo_fct_sales,
  (SELECT sum(ventas)   FROM marts.rpt_sales)                            AS vista_dashboard;
