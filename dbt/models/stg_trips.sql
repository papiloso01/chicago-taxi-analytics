-- Invalid numeric values become NULL rather than crashing the entire batch.
select trip_id,
  case when pg_input_is_valid(payload->>'trip_start_timestamp', 'timestamp')
    then (payload->>'trip_start_timestamp')::timestamp end as started_at,
  {% for field in ['trip_seconds','trip_miles','fare','tips','trip_total'] %}
  case when payload->>'{{ field }}' ~ '^-?[0-9]+(\.[0-9]+)?$'
    then (payload->>'{{ field }}')::numeric end as {{ field }},
  {% endfor %}
  coalesce(nullif(payload->>'payment_type',''), 'Unknown') as payment_type,
  coalesce(nullif(payload->>'company',''), 'Unknown') as company,
  payload->>'pickup_community_area' as pickup_area
from {{ source('bronze','trips') }}
