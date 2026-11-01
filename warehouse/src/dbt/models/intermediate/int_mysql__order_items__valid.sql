-- Filas sin motivo de rechazo. Columnas explícitas: Redshift no soporta SELECT * EXCLUDE.
select
    order_item_id,
    order_id,
    product_id,
    quantity,
    unit_price,
    subtotal,
    _batch_id,
    _extracted_at,
    _source
from {{ ref('int_mysql__order_items__qa') }}
where rejection_reason is null
