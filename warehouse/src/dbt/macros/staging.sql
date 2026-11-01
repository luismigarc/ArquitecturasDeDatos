{# Entidades con extracción FULL: la versión vigente es la última copia completa extraída.
   Si un registro desaparece de la fuente, deja de estar en staging (las bajas se propagan). #}
{% macro latest_full_extract(relation) %}
select *
from {{ relation }}
where _extracted_at = (select max(_extracted_at) from {{ relation }})
{% endmacro %}

{# Entidades con extracción INCREMENTAL: un registro puede llegar en varios lotes (reintentos,
   modo full, solapamiento tras un fallo). Se conserva la versión más reciente de cada llave. #}
{% macro latest_per_key(relation, key) %}
select *
from (
    select
        src.*,
        row_number() over (partition by {{ key }} order by _extracted_at desc, _batch_id desc) as _version_rank
    from {{ relation }} as src
) as ranked
where _version_rank = 1
{% endmacro %}
