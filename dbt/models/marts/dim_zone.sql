-- Zone dimension from the TLC's own published lookup.
-- 265 rows, changes almost never - a classic small, slowly-changing dimension.

with zones as (

    select * from {{ ref('taxi_zone_lookup') }}

)

select
    cast("LocationID" as integer)  as zone_id,
    "Borough"                      as borough,
    "Zone"                         as zone_name,
    "service_zone"                 as service_zone
from zones
