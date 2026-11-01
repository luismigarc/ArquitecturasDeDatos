-- =====================================================================================
-- Evidencias de funcionamiento (Redshift). bash infra/cli/sql.sh -f /opt/airflow/sql/evidencias.sql
-- =====================================================================================

-- A. Lotes cargados en raw (COPY desde S3), filas por lote
SELECT 'mysql_orders' AS tabla, _batch_id, count(*) AS filas, min(_extracted_at) AS extraido_utc FROM "raw".mysql_orders GROUP BY 1, 2
UNION ALL SELECT 'mysql_order_items', _batch_id, count(*), min(_extracted_at) FROM "raw".mysql_order_items GROUP BY 1, 2
UNION ALL SELECT 'mongo_reviews', _batch_id, count(*), min(_extracted_at) FROM "raw".mongo_reviews GROUP BY 1, 2
UNION ALL SELECT 'mongo_navigation_events', _batch_id, count(*), min(_extracted_at) FROM "raw".mongo_navigation_events GROUP BY 1, 2
ORDER BY extraido_utc DESC, tabla;

-- B. Igualdades de calidad: stg = qa = valid + quarantine
SELECT 'order_items' AS entidad,
  (SELECT count(*) FROM intermediate.stg_mysql__order_items) AS stg,
  (SELECT count(*) FROM intermediate.int_mysql__order_items__valid) AS valid,
  (SELECT count(*) FROM quarantine.quarantine_mysql__order_items) AS quarantine
UNION ALL SELECT 'reviews',
  (SELECT count(*) FROM intermediate.stg_mongodb__reviews),
  (SELECT count(*) FROM intermediate.int_mongodb__reviews__valid),
  (SELECT count(*) FROM quarantine.quarantine_mongodb__reviews)
UNION ALL SELECT 'navigation_events',
  (SELECT count(*) FROM intermediate.stg_mongodb__navigation_events),
  (SELECT count(*) FROM intermediate.int_mongodb__navigation_events__valid),
  (SELECT count(*) FROM quarantine.quarantine_mongodb__navigation_events);

-- C. Rechazos con motivo y lote de origen
SELECT 'customers' AS entidad, customer_id AS id, rejection_reason, _batch_id FROM quarantine.quarantine_mysql__customers
UNION ALL SELECT 'orders', order_id, rejection_reason, _batch_id FROM quarantine.quarantine_mysql__orders
UNION ALL SELECT 'order_items', order_item_id, rejection_reason, _batch_id FROM quarantine.quarantine_mysql__order_items
UNION ALL SELECT 'reviews', review_id, rejection_reason, _batch_id FROM quarantine.quarantine_mongodb__reviews
UNION ALL SELECT 'events', event_id, rejection_reason, _batch_id FROM quarantine.quarantine_mongodb__navigation_events
ORDER BY entidad, id;

-- D. Hechos: filas = llaves distintas (sin duplicados)
SELECT 'fct_sales' AS hecho, count(*) AS filas, count(DISTINCT sale_sk) AS llaves, sum(subtotal) AS total FROM marts.fct_sales
UNION ALL SELECT 'fct_reviews', count(*), count(DISTINCT review_sk), NULL FROM marts.fct_reviews
UNION ALL SELECT 'fct_navigation_events', count(*), count(DISTINCT navigation_event_sk), NULL FROM marts.fct_navigation_events;

-- E. SCD tipo 2: historia del cliente 1 y del producto 1
SELECT customer_id, city, department, valid_from, valid_to, is_current FROM marts.dim_customer WHERE customer_id = 1 ORDER BY valid_from;
SELECT product_id, category, valid_from, valid_to, is_current FROM marts.dim_product WHERE product_id = 1 ORDER BY valid_from;

-- F. Una sola versión vigente por entidad (debe devolver 0 filas)
SELECT customer_id, count(*) FROM marts.dim_customer WHERE is_current GROUP BY customer_id HAVING count(*) > 1;

-- G. Ventas por mes y departamento
SELECT anio, mes, departamento, sum(ventas) AS ventas FROM marts.rpt_sales GROUP BY 1, 2, 3 ORDER BY 1, 2, 4 DESC;
