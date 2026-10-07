select trip_date, payment_type, count(*) as trips,
 sum(trip_total) as revenue_usd
from {{ ref('fct_trips') }} group by trip_date, payment_type
