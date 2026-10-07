select trip_date, count(*) as trips,
 sum(trip_total) as revenue_usd, sum(tips) as tips_usd,
 avg(trip_total) as average_trip_usd, avg(trip_miles) as average_miles,
 avg(trip_seconds)/60 as average_minutes
from {{ ref('fct_trips') }} group by trip_date
