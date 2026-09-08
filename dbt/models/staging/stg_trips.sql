-- Staging does exactly three things and no business logic:
--   1. one row per source row (no aggregation)
--   2. consistent naming and types
--   3. a surrogate key so downstream models have something to join and test on
--
-- Everything interesting happens in marts. Keeping staging thin is what makes
-- it possible to re-point the warehouse at a new source without rewriting
-- your business logic.

with source as (

    select * from {{ source('raw', 'raw_trips') }}

),

renamed as (

    select
        -- surrogate key: deterministic, so re-running produces the same keys
        {{ dbt_utils.generate_surrogate_key([
            'pickup_datetime', 'dropoff_datetime', 'pickup_location_id',
            'dropoff_location_id', 'total_amount', 'trip_distance'
        ]) }}                                       as trip_key,

        vendor_id,
        rate_code_id,
        payment_type                                as payment_type_id,
        pickup_location_id,
        dropoff_location_id,

        cast(pickup_datetime as timestamp)          as pickup_at,
        cast(dropoff_datetime as timestamp)         as dropoff_at,
        cast(pickup_date as date)                   as pickup_date,
        trip_duration_seconds,

        passenger_count,
        trip_distance,

        fare_amount,
        extra,
        mta_tax,
        tip_amount,
        tolls_amount,
        improvement_surcharge,
        coalesce(congestion_surcharge, 0)           as congestion_surcharge,
        coalesce(airport_fee, 0)                    as airport_fee,
        coalesce(cbd_congestion_fee, 0)             as cbd_congestion_fee,
        total_amount,

        case when store_and_fwd_flag = 'Y' then true else false end as was_store_and_forward,

        _loaded_at

    from source

)

select * from renamed
