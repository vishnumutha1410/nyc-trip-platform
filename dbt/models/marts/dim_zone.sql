-- Zone dimension from the TLC's own published lookup.
-- 265 rows, changes almost never - a classic small, slowly-changing dimension.
--
-- Identifiers are unquoted deliberately: Snowflake folds unquoted names to
-- uppercase, and the dbt seed loaded the CSV headers that way. Quoting
-- "LocationID" would look for a mixed-case column that doesn't exist.

with zones as (

    select * from {{ ref('taxi_zone_lookup') }}

)

select
    cast(locationid as integer)  as zone_id,
    borough                      as borough,
    zone                         as zone_name,
    service_zone                 as service_zone
from zones
