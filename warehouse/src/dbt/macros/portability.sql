{# Funciones que cambian entre DuckDB (versión local), Redshift y PostgreSQL (pruebas). #}

{# Convierte un TIMESTAMP guardado en UTC a la hora de negocio (America/Bogota). #}
{% macro to_business_tz(column) -%}
    {{ return(adapter.dispatch('to_business_tz')(column)) }}
{%- endmacro %}

{% macro redshift__to_business_tz(column) -%}
    convert_timezone('UTC', '{{ var("business_timezone") }}', {{ column }})
{%- endmacro %}

{% macro default__to_business_tz(column) -%}
    (({{ column }} at time zone 'UTC') at time zone '{{ var("business_timezone") }}')
{%- endmacro %}

{# Redshift no soporta IS DISTINCT FROM: comparación equivalente que trata NULL como valor. #}
{% macro null_safe_distinct(left, right) -%}
    (({{ left }} <> {{ right }}) or ({{ left }} is null and {{ right }} is not null) or ({{ left }} is not null and {{ right }} is null))
{%- endmacro %}

{# Día de la semana ISO (lunes=1 ... domingo=7). Redshift no tiene isodow. #}
{% macro iso_day_of_week(date_column) -%}
    (case when extract(dow from {{ date_column }}) = 0 then 7 else extract(dow from {{ date_column }}) end)
{%- endmacro %}
