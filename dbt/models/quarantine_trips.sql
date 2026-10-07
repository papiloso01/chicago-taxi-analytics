select * from {{ ref('stg_trips') }}
where started_at is null or trip_total is null or fare is null
   or trip_miles is null or trip_seconds is null
   or trip_total < 0 or fare < 0 or trip_miles < 0 or trip_seconds < 0
