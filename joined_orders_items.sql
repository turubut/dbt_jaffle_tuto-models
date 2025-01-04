-- models/joined_orders_items.sql

{{ config(
    materialized='incremental',
    unique_key='order_id',
    incremental_strategy='merge',
    partition_by={'field': 'ordered_at', 'data_type': 'timestamp'}
) }}

WITH orders AS (
    SELECT
    		id,
		customer,
		ordered_at,
		store_id,
		subtotal,
		tax_paid,
		order_total
    FROM {{ source('raw', 'raw_orders') }}
),

items AS (
    SELECT 
		id,
		order_id,
		sku
    FROM {{ source('raw', 'raw_items') }}
)

SELECT
    	o.id,
	o.customer,
	o.ordered_at,
	o.store_id,
	o.subtotal,
	o.tax_paid,
	o.order_total,
	i.id as item_id,
	i.sku  
FROM orders o
JOIN items i
    ON o.id = i.order_id

{% if is_incremental() %}
    -- Load records starting from the last loaded date (max ordered_at from previous loads)
    -- Only load data for the next 2 days
    WHERE o.ordered_at >= (COALESCE((SELECT MAX(ordered_at) FROM {{ this }}), '2016-09-01'::timestamp) + INTERVAL '2 DAY')
{% endif %}
