-- Filas sin motivo de rechazo. Columnas explícitas: Redshift no soporta SELECT * EXCLUDE.
select
    review_id,
    customer_id,
    product_id,
    rating,
    reviewed_at,
    is_verified_purchase,
    document_json,
    _batch_id,
    _extracted_at,
    _source
from {{ ref('int_mongodb__reviews__qa') }}
where rejection_reason is null
