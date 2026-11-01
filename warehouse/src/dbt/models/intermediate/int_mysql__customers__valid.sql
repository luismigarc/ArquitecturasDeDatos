-- Filas sin motivo de rechazo. Columnas explícitas: Redshift no soporta SELECT * EXCLUDE.
select
    customer_id,
    name,
    email,
    phone,
    city,
    department,
    registration_date,
    updated_at,
    _batch_id,
    _extracted_at,
    _source
from {{ ref('int_mysql__customers__qa') }}
where rejection_reason is null
