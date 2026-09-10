{{ config(materialized='table', tags=['cdc']) }}

-- Slowly Changing Dimension, Type 2. One row per customer per version, with
-- the window of time that version was true.
--
-- WHY THIS EXISTS
--   Keep only the current tier and every historical order silently gets
--   re-attributed to it. Last quarter's "gold tier revenue" then changes every
--   time you rerun the report, and nobody can tell you why. A fact joined to
--   Type 2 history on the transaction date always sees the tier that was
--   actually in force when the order was placed.
--
-- WHY NOT dbt snapshot
--   A snapshot polls the source table on a schedule and records what it finds.
--   Anything that changed and changed back between two runs is invisible to
--   it, and the timestamps it records are run times, not commit times. CDC
--   gives us the actual commit sequence, so we can build history that is
--   correct rather than merely periodic.

with changes as (

    select * from {{ ref('stg_cdc_customers') }}

),

material as (

    -- Drop no-op updates. Postgres writes a WAL record for an UPDATE even when
    -- it changes nothing, so Debezium emits an event whose before and after are
    -- identical. Versioning those would create rows with a zero-length validity
    -- window and inflate every "how often do customers change tier" answer.
    --
    -- `is distinct from` rather than `<>` because either side can be null and
    -- null <> null is null, which would silently keep the row.
    select *
    from changes
    where op in ('r', 'c', 'd')
       or email     is distinct from prev_email
       or full_name is distinct from prev_full_name
       or city      is distinct from prev_city
       or tier      is distinct from prev_tier

),

windowed as (

    select
        *,
        -- The next event for this customer closes the current version. Ordered
        -- by LSN, never by changed_at: two commits inside the same millisecond
        -- would otherwise order arbitrarily and the windows could overlap.
        lead(changed_at) over (
            partition by customer_id order by lsn
        ) as next_changed_at
    from material

)

select
    {{ dbt_utils.generate_surrogate_key(['customer_id', 'lsn']) }} as customer_version_key,

    customer_id,
    email,
    full_name,
    city,
    tier,

    changed_at      as valid_from,
    next_changed_at as valid_to,

    -- A version is current when nothing came after it. A deleted customer's
    -- last real version is closed by the delete event, so it is correctly not
    -- current - and no separate "is_deleted" flag is needed to express that.
    next_changed_at is null as is_current,

    lsn,
    op as change_type

from windowed

-- Delete events close the previous window (via the lead above) but are not
-- themselves a version of the customer - there is no row to describe.
where op <> 'd'
