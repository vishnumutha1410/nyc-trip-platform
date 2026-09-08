-- Fact table at the grain of ONE COMPLETED TRIP.
--
-- Stating the grain explicitly is the single most useful sentence in a dbt
-- model, because every question about the table ("can I sum this?", "will
-- this join fan out?") is answered by it.
--
-- Foreign keys only, plus additive measures. No descriptive attributes -
-- those live on the dimensions, which is what makes this a star rather than
-- one wide table.

with trips as (

    select * from {{ ref('stg_trips') }}

),

final as (

    select
        t.trip_key,

        -- foreign keys
        t.pickup_date                                as pickup_date_key,
        t.pickup_location_id                         as pickup_zone_key,
        t.dropoff_location_id                        as dropoff_zone_key,
        t.vendor_id                                  as vendor_key,
        t.rate_code_id                               as rate_code_key,
        t.payment_type_id                            as payment_type_key,

        -- degenerate dimensions (belong on the fact, no dimension table earns its keep)
        t.pickup_at,
        t.dropoff_at,
        t.was_store_and_forward,

        -- additive measures
        t.passenger_count,
        t.trip_distance,
        t.trip_duration_seconds,
        t.fare_amount,
        t.extra,
        t.mta_tax,
        t.tip_amount,
        t.tolls_amount,
        t.improvement_surcharge,
        t.congestion_surcharge,
        t.airport_fee,
        t.cbd_congestion_fee,
        t.total_amount,

        -- non-additive, derived. Averaging these across rows is valid;
        -- summing them is not, which is why they are named as ratios.
        case when t.trip_distance > 0
             then t.fare_amount / t.trip_distance end        as fare_per_mile,
        case when t.trip_duration_seconds > 0
             then t.trip_distance / (t.trip_duration_seconds / 3600.0) end as avg_speed_mph,
        case when t.fare_amount > 0
             then t.tip_amount / t.fare_amount end           as tip_rate

    from trips t

)

select * from final
