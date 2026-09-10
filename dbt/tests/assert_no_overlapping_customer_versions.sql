{{ config(tags=['cdc']) }}

-- No customer may have two versions valid at the same instant.
--
-- This is THE test for a Type 2 dimension. If windows overlap, any fact joined
-- on a date matches more than one version and your revenue silently doubles
-- for the affected customers. Nothing errors. The totals are just wrong, and
-- they are wrong by an amount that depends on how often that customer changed,
-- which is why it is so hard to spot by eye.

with versions as (

    select
        customer_id,
        valid_from,
        valid_to,
        lead(valid_from) over (
            partition by customer_id order by valid_from
        ) as next_valid_from
    from {{ ref('dim_customer_history') }}

)

select
    customer_id,
    valid_from,
    valid_to,
    next_valid_from
from versions
where next_valid_from is not null
  and valid_to > next_valid_from
