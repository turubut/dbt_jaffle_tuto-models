-- models/joined_orders_items.sql

{{ config(
    materialized='incremental',
    unique_key='order_id',
    incremental_strategy='merge',
    partition_by={'field': 'ordered_at', 'data_type': 'timestamp'}
) }}

select 
ro.id,
ro.customer,
ro.ordered_at,
ro.store_id,
ro.subtotal,
ro.tax_paid,
ro.order_total,
ri.id as item_id,
ri.sku
from
(select 
id,
customer,
ordered_at,
store_id,
subtotal,
tax_paid,
order_total
from {{ source('raw', 'raw_orders') }}) ro 
left join 
(select
id,
order_id,
sku
from {{ source('raw', 'raw_items') }} ri
) as ri
on ro.id  = ri.order_id

{% if is_incremental() %}
    WHERE ro.ordered_at BETWEEN "2016-09-01" + INTERVAL '2 DAYS' AND MAX(ordered_at)
{% endif %}
