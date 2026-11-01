-- reviewed_at llega en UTC (DocumentDB guarda fechas en UTC). is_verified_purchase se extrae del
-- documento anidado durante la extracción, así no se depende de funciones JSON del motor.
with source as (
    {{ latest_per_key(source('mongodb_raw', 'mongo_reviews'), 'review_id') }}
)
select
    cast(review_id as bigint) as review_id,
    cast(customer_id as bigint) as customer_id,
    cast(product_id as bigint) as product_id,
    cast(rating as integer) as rating,
    cast(reviewed_at as timestamp) as reviewed_at,
    cast(is_verified_purchase as boolean) as is_verified_purchase,
    document_json,
    _batch_id,
    _extracted_at,
    _source
from source
