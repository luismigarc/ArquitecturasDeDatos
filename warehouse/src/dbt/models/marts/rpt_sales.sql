{{ config(materialized='view', bind=False) }}
-- Vista de consumo para Amazon QuickSight: una fila por línea de venta con sus atributos de
-- negocio ya resueltos (fecha, categoría y región VIGENTES en el momento de la venta, por SCD2).
-- QuickSight no necesita escribir SQL: filtra y agrega sobre estas columnas.
select
    sales.sale_sk,
    cal.date as fecha,
    cal.year as anio,
    cal.month as mes,
    cal.day_of_week as dia_semana,
    cal.is_weekend as es_fin_de_semana,
    cal.is_holiday as es_festivo,
    product.product_id,
    product.name as producto,
    product.category as categoria,
    customer.customer_id,
    geography.department as departamento,
    geography.city as ciudad,
    channel.sales_channel as canal,
    sales.quantity as unidades,
    sales.unit_price as precio_unitario,
    sales.subtotal as ventas
from {{ ref('fct_sales') }} as sales
inner join {{ ref('dim_date') }} as cal on cal.date_sk = sales.date_sk
inner join {{ ref('dim_product') }} as product on product.product_sk = sales.product_sk
inner join {{ ref('dim_customer') }} as customer on customer.customer_sk = sales.customer_sk
inner join {{ ref('dim_geography') }} as geography on geography.geography_sk = sales.geography_sk
inner join {{ ref('dim_sales_channel') }} as channel on channel.sales_channel_sk = sales.sales_channel_sk
