select trip_date, trip_hour, count(*) as trips,
 sum(trip_total) as revenue_usd
from {{ ref('fct_trips') }} group by trip_date, trip_hour
