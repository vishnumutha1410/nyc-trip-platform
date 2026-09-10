{{ config(materialized='table', tags=['cdc']) }}

-- The reason SCD Type 2 was built. Each order joined to the customer version
-- that was in force WHEN THE ORDER WAS PLACED, not the version in force today.
--
-- The join predicate is the whole model:
--
--     o.last_changed_at >= h.valid_from
--     and (h.valid_to is null or o.last_changed_at < h.valid_to)
--
-- Half-open windows: valid_from inclusive, valid_to exclusive. Get that wrong
-- in the other direction and an order landing exactly on a boundary matches
-- two versions, silently duplicating revenue. The equal_rowcount test below is
-- what catches it, because the symptom is more rows out than in - not an error.

with orders as (

    select * from {{ ref('fct_orders_current') }}

),

history as (

    select * from {{ ref('dim_customer_history') }}

)

select
    o.order_id,
    o.customer_id,
    o.status,
    o.amount,
    o.currency,
    o.last_changed_at,

    h.customer_version_key,
    h.tier  as tier_at_purchase,
    h.city  as city_at_purchase,

    -- Kept alongside so the difference is visible in one query - this is the
    -- column that makes the point in an interview.
    current_h.tier as tier_today

from orders o
left join history h
       on o.customer_id = h.customer_id
      and o.last_changed_at >= h.valid_from
      and (h.valid_to is null or o.last_changed_at < h.valid_to)
left join history current_h
       on o.customer_id = current_h.customer_id
      and current_h.is_current
