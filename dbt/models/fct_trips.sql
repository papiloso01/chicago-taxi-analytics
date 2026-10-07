select *, started_at::date as trip_date,
  extract(hour from started_at)::integer as trip_hour
from {{ ref('stg_trips') }}
where started_at is not null
  and trip_total >= 0 and fare >= 0
  and trip_miles >= 0 and trip_seconds >= 0
