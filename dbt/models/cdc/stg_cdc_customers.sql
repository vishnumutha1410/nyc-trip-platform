{{ config(materialized='view', tags=['cdc']) }}

-- One row per customer change event, typed out of the JSON envelope.
--
-- Thin on purpose, exactly like stg_trips: rename, cast, carry the before
-- image alongside the after image. No business logic, no filtering. Every
-- event that arrived is still here, including the no-op updates - deciding
-- what counts as a real change is a modelling decision, and it belongs in a
-- model where it can be seen and tested, not hidden in a staging view.

with events as (

    select
        event_uid,
        lsn,
        op,
        -- source_ts_ms is when Postgres committed the change, not when we
        -- happened to consume it. Using the consumption time would make your
        -- history depend on pipeline lag.
        to_timestamp_ntz(source_ts_ms, 3) as changed_at,
        try_parse_json(before_image)      as before_image,
        try_parse_json(after_image)       as after_image
    from {{ source('cdc', 'cdc_events') }}
    where source_table = 'customers'

)

select
    event_uid,
    lsn,
    op,
    changed_at,

    -- A delete has no after image, so the key has to come from the before
    -- image. This is the line that stops deleted customers vanishing from
    -- history entirely.
    coalesce(after_image:customer_id, before_image:customer_id)::number as customer_id,

    after_image:email::string      as email,
    after_image:full_name::string  as full_name,
    after_image:city::string       as city,
    after_image:tier::string       as tier,

    before_image:email::string     as prev_email,
    before_image:full_name::string as prev_full_name,
    before_image:city::string      as prev_city,
    before_image:tier::string      as prev_tier

from events
