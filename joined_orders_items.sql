-- models/joined_orders_items.sql

{{ config(
    materialized='incremental',
    unique_key='order_id',
    incremental_strategy='merge',
    partition_by={'field': 'created_at', 'data_type': 'timestamp'}
) }}

WITH orders AS (
    SELECT 
        order_id,
        customer_id,
        order_date,
        total_amount,
        created_at
    FROM {{ source('raw', 'raw_orders') }}
),
items AS (
    SELECT 
        item_id,
        order_id,
        product_id,
        quantity,
        price
    FROM {{ source('raw', 'raw_items') }}
)

SELECT
    o.order_id,
    o.customer_id,
    o.order_date,
    o.total_amount,
    i.item_id,
    i.product_id,
    i.quantity,
    i.price
FROM orders o
JOIN items i
    ON o.order_id = i.order_id

{% if is_incremental() %}
    WHERE o.created_at > (SELECT MAX(created_at) FROM {{ this }})
{% endif %}
