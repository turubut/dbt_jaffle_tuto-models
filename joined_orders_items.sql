-- models/joined_orders_items.sql

{{ config(
    materialized='incremental',
    unique_key='order_id',
    incremental_strategy='merge',
    partition_by={'field': 'ordered_at', 'data_type': 'timestamp'}
) }}

select * from
(select 
id,
customer,
,
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
order by ordered_at desc;


{% if is_incremental() %}
    WHERE ro.ordered_at > (SELECT MAX(ordered_at) FROM {{ this }})
{% endif %}
