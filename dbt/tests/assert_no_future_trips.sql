-- A singular test: business invariant, not a column property.
-- Returns rows only when something is wrong, which is the dbt test contract.

select trip_key, pickup_at
from {{ ref('fct_trips') }}
where pickup_at > current_timestamp()
