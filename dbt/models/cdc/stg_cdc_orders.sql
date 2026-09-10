{{ config(materialized='view', tags=['cdc']) }}

with events as (

    select
        event_uid,
        lsn,
        op,
        to_timestamp_ntz(source_ts_ms, 3) as changed_at,
        try_parse_json(before_image)      as before_image,
        try_parse_json(after_image)       as after_image
    from {{ source('cdc', 'cdc_events') }}
    where source_table = 'orders'

)

select
    event_uid,
    lsn,
    op,
    changed_at,
    coalesce(after_image:order_id, before_image:order_id)::number as order_id,
    after_image:customer_id::number as customer_id,
    after_image:status::string      as status,
    after_image:amount::float       as amount,
    after_image:currency::string    as currency,
    after_image:placed_at::string   as placed_at_raw,
    before_image:status::string     as prev_status
from events
