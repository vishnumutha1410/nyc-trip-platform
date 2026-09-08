-- A proper date spine, not "distinct dates that happen to be in the facts".
-- The difference matters: a day with zero trips still needs a row, or your
-- time series silently skips it and a chart shows a line where there is a gap.

{% set start_date = "2015-01-01" %}

with spine as (

    {{ dbt_utils.date_spine(
        datepart="day",
        start_date="cast('" ~ start_date ~ "' as date)",
        end_date="dateadd(year, 1, current_date)"
    ) }}

)

select
    cast(date_day as date)                          as date_key,
    extract(year   from date_day)                   as year_number,
    extract(quarter from date_day)                  as quarter_number,
    extract(month  from date_day)                   as month_number,
    extract(day    from date_day)                   as day_of_month,
    extract(dayofweek from date_day)                as day_of_week,
    case when extract(dayofweek from date_day) in (0, 6)
         then true else false end                   as is_weekend
from spine
