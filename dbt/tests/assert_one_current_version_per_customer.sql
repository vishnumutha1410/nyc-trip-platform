{{ config(tags=['cdc']) }}

-- A customer that still exists has exactly one current version. Never two
-- (ambiguous "who is this customer") and never zero (a live customer that
-- falls out of every current-state join, so their orders quietly disappear
-- from any report that filters on is_current).
--
-- Customers deleted upstream are excluded: their final version is closed by
-- the delete event, so having no current row is the correct answer for them,
-- not a failure.

with live_customers as (

    select distinct customer_id
    from {{ ref('dim_customer_history') }}
    where customer_id not in (
        select customer_id
        from {{ ref('stg_cdc_customers') }} s
        where s.op = 'd'
          and s.lsn = (
              select max(lsn) from {{ ref('stg_cdc_customers') }} s2
              where s2.customer_id = s.customer_id
          )
    )

),

current_counts as (

    select
        h.customer_id,
        count_if(h.is_current) as current_versions
    from {{ ref('dim_customer_history') }} h
    join live_customers l on l.customer_id = h.customer_id
    group by 1

)

select customer_id, current_versions
from current_counts
where current_versions <> 1
