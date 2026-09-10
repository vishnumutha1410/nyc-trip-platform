{{ config(materialized='table', tags=['cdc']) }}

-- Current state of every order, reconstructed from the change log.
--
-- The event stream is append-only, so "what does this order look like now" is
-- a question you answer by taking the last event per key. Two details decide
-- whether that is correct:
--
--   order by lsn desc   the database's own commit order. Ordering by
--                       changed_at would tie whenever two commits land in the
--                       same millisecond, and resolve those ties arbitrarily.
--
--   op <> 'd'           an order whose final event is a delete does not exist.
--                       Filtering deletes BEFORE picking the latest event
--                       instead would resurrect it at its second-to-last
--                       state, which is the classic CDC bug: rows that were
--                       deleted upstream living on downstream forever.

with latest as (

    select
        *,
        row_number() over (partition by order_id order by lsn desc) as rn
    from {{ ref('stg_cdc_orders') }}

)

select
    order_id,
    customer_id,
    status,
    amount,
    currency,
    changed_at as last_changed_at,
    lsn        as last_lsn
from latest
where rn = 1
  and op <> 'd'
