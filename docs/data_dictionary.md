# Data dictionary

Generated from versioned metadata; edit contracts, then regenerate.

## bronze.community_area_raw

Community area raw.

Grain: One row per record_id.

| Column | Type | Nullable | Description |
|---|---|---|---|
| record_id | string | False | Source natural key; trip ID, community ID or UTC weather hour. |
| payload | string | False | Original source record encoded as JSON; measures are not coerced in bronze. |
| ingested_at_utc | string | False | UTC ingestion timestamp in ISO 8601 text. |
| run_id | string | False | Immutable ingestion run identifier. |
| sequence | bigint | False | Order within the source extraction; used to resolve repeated keys. |

## bronze.taxi_trip_raw

Taxi trip raw.

Grain: One row per record_id.

| Column | Type | Nullable | Description |
|---|---|---|---|
| record_id | string | False | Source natural key; trip ID, community ID or UTC weather hour. |
| payload | string | False | Original source record encoded as JSON; measures are not coerced in bronze. |
| ingested_at_utc | string | False | UTC ingestion timestamp in ISO 8601 text. |
| run_id | string | False | Immutable ingestion run identifier. |
| sequence | bigint | False | Order within the source extraction; used to resolve repeated keys. |

## bronze.weather_hour_raw

Weather hour raw.

Grain: One row per record_id.

| Column | Type | Nullable | Description |
|---|---|---|---|
| record_id | string | False | Source natural key; trip ID, community ID or UTC weather hour. |
| payload | string | False | Original source record encoded as JSON; measures are not coerced in bronze. |
| ingested_at_utc | string | False | UTC ingestion timestamp in ISO 8601 text. |
| run_id | string | False | Immutable ingestion run identifier. |
| sequence | bigint | False | Order within the source extraction; used to resolve repeated keys. |

## gold.mart_community_area_demand_daily

Mart community area demand daily.

Grain: One row per trip_date, area_id, activity.

| Column | Type | Nullable | Description |
|---|---|---|---|
| trip_date | date | False | Chicago local calendar date. |
| area_id | string | True | Chicago community area ID; nullable for unknown trip locations. |
| area_name | string | True | Community area label; Unknown where no source match exists. |
| activity | string | False | Pickup or Dropoff; filter to one activity to avoid counting each trip twice. |
| trips | bigint | True | Trip count at this table grain. |
| reported_total_usd | decimal(30,6) | True | Sum of reported trip charges in USD. |

## gold.mart_payment_demand_daily

Mart payment demand daily.

Grain: One row per trip_date, payment_type.

| Column | Type | Nullable | Description |
|---|---|---|---|
| trip_date | date | False | Chicago local calendar date. |
| payment_type | string | False | Source payment category with Unknown fallback. |
| trips | bigint | True | Trip count at this table grain. |
| revenue_usd | decimal(30,6) | True | Sum of reported trip charges in USD; not profit. |

## gold.mart_taxi_demand_daily

Mart taxi demand daily.

Grain: One row per trip_date.

| Column | Type | Nullable | Description |
|---|---|---|---|
| trip_date | date | False | Chicago local calendar date. |
| trips | bigint | True | Trip count at this table grain. |
| revenue_usd | decimal(30,6) | True | Sum of reported trip charges in USD; not profit. |
| tips_usd | decimal(30,6) | True | Reported tips in USD; missing values stay null. |
| average_distance_km | decimal(28,10) | True | Mean distance km at this reporting grain. |
| average_trip_usd | decimal(24,10) | True | Mean trip usd at this reporting grain. |

## gold.mart_taxi_demand_hourly

Mart taxi demand hourly.

Grain: One row per trip_date, trip_hour.

| Column | Type | Nullable | Description |
|---|---|---|---|
| trip_date | date | False | Chicago local calendar date. |
| trip_hour | int | False | Chicago local hour 0–23. |
| trips | bigint | True | Trip count at this table grain. |
| revenue_usd | decimal(30,6) | True | Sum of reported trip charges in USD; not profit. |

## gold.mart_taxi_revenue_daily

Mart taxi revenue daily.

Grain: One row per trip_date, taxi_key.

| Column | Type | Nullable | Description |
|---|---|---|---|
| trip_date | date | False | Chicago local calendar date. |
| taxi_key | string | False | Stable SHA-256 warehouse key for taxi. |
| taxi_label | string | True | Taxi label at this table grain. |
| trips | bigint | True | Trip count at this table grain. |
| reported_total_usd | decimal(30,6) | True | Sum of reported trip charges in USD. |

## gold.mart_taxi_trip_distance

Mart taxi trip distance.

Grain: One row per trip_id.

| Column | Type | Nullable | Description |
|---|---|---|---|
| trip_id | string | False | Unique source trip identifier. |
| trip_date | date | True | Chicago local calendar date. |
| taxi_id | string | True | Anonymised vehicle identifier; not a driver identity. |
| trip_distance_km | decimal(24,6) | True | Reported miles multiplied by 1.609344; not a reconstructed road route. |
| pickup_community_area | string | True | Source or enriched pickup community area; Unknown/null when unavailable. |
| dropoff_community_area | string | True | Source or enriched dropoff community area; Unknown/null when unavailable. |
| is_positive_distance | boolean | True | True for a reported trip distance greater than zero. |

## gold.mart_taxi_trip_enriched

Mart taxi trip enriched.

Grain: One row per trip_id.

| Column | Type | Nullable | Description |
|---|---|---|---|
| weather_local_hour | string | True | Chicago wall-clock hour string; repeated DST hours are flagged. |
| dropoff_area_id | string | True | Source or enriched dropoff area id; Unknown/null when unavailable. |
| pickup_area_id | string | True | Source or enriched pickup area id; Unknown/null when unavailable. |
| dropoff_community_area | string | True | Source or enriched dropoff community area; Unknown/null when unavailable. |
| pickup_community_area | string | True | Source or enriched pickup community area; Unknown/null when unavailable. |
| company | string | True | Source company label with Unknown fallback. |
| payment_type | string | True | Source payment category with Unknown fallback. |
| taxi_id | string | True | Anonymised vehicle identifier; not a driver identity. |
| trip_id | string | False | Unique source trip identifier. |
| trip_duration_seconds | decimal(20,6) | False | Reported trip duration in seconds. |
| trip_distance_miles | decimal(20,6) | True | Reported trip distance in miles. |
| fare_usd | decimal(20,6) | True | Reported fare in USD. |
| tips_usd | decimal(20,6) | True | Reported tips in USD; missing values stay null. |
| trip_total_usd | decimal(20,6) | False | Reported total trip charge in USD; not driver profit. |
| started_at | string | True | Chicago local trip start as text; rounded source time, no inferred UTC offset. |
| trip_date | date | False | Chicago local calendar date. |
| trip_hour | int | False | Chicago local hour 0–23. |
| trip_distance_km | decimal(24,6) | False | Reported miles multiplied by 1.609344; not a reconstructed road route. |
| taxi_key | string | False | Stable SHA-256 warehouse key for taxi. |
| payment_key | string | True | Stable SHA-256 warehouse key for payment. |
| company_key | string | True | Stable SHA-256 warehouse key for company. |
| pickup_area_key | string | False | Stable SHA-256 warehouse key for pickup area. |
| dropoff_area_key | string | False | Stable SHA-256 warehouse key for dropoff area. |
| pickup_area_name | string | True | Source or enriched pickup area name; Unknown/null when unavailable. |
| dropoff_area_name | string | True | Source or enriched dropoff area name; Unknown/null when unavailable. |
| source_hours | bigint | True | Number of UTC source hours mapping to this local hour. |
| temperature_c | double | True | City-level ERA5 temperature in Celsius. |
| precipitation_mm | double | True | City-level ERA5 hourly precipitation in millimetres; never sum duplicated trip-level weather. |
| snowfall_cm | double | True | City-level ERA5 hourly snowfall in centimetres. |
| weather_ambiguous | boolean | True | True when multiple UTC hours map to the same local hour. |
| weather_available | boolean | True | True only when all three weather measures exist. |
| weather_condition | string | True | Dry, Wet, Snow, Unavailable or Ambiguous DST hour. |

## gold.mart_weather_demand_hourly

Mart weather demand hourly.

Grain: One row per trip_date, trip_hour, weather_condition.

| Column | Type | Nullable | Description |
|---|---|---|---|
| trip_date | date | False | Chicago local calendar date. |
| trip_hour | int | False | Chicago local hour 0–23. |
| weather_condition | string | False | Dry, Wet, Snow, Unavailable or Ambiguous DST hour. |
| trips | bigint | True | Trip count at this table grain. |
| reported_total_usd | decimal(30,6) | True | Sum of reported trip charges in USD. |
| average_trip_duration_seconds | decimal(24,10) | True | Mean trip duration seconds at this reporting grain. |
| temperature_c | double | True | City-level ERA5 temperature in Celsius. |
| precipitation_mm | double | True | City-level ERA5 hourly precipitation in millimetres; never sum duplicated trip-level weather. |
| snowfall_cm | double | True | City-level ERA5 hourly snowfall in centimetres. |

## silver.dim_area

Dim area.

Grain: One row per area_key.

| Column | Type | Nullable | Description |
|---|---|---|---|
| area | string | True | Area at this table grain. |
| area_key | string | False | Stable SHA-256 warehouse key for area. |

## silver.dim_community_area

Dim community area.

Grain: One row per area_id.

| Column | Type | Nullable | Description |
|---|---|---|---|
| area_id | string | False | Chicago community area ID; nullable for unknown trip locations. |
| area_name | string | True | Community area label; Unknown where no source match exists. |
| geometry_geojson | string | True | Polygon/MultiPolygon geometry encoded as GeoJSON text. |

## silver.dim_company

Dim company.

Grain: One row per company_key.

| Column | Type | Nullable | Description |
|---|---|---|---|
| company | string | True | Source company label with Unknown fallback. |
| company_key | string | False | Stable SHA-256 warehouse key for company. |

## silver.dim_date

Dim date.

Grain: One row per trip_date.

| Column | Type | Nullable | Description |
|---|---|---|---|
| trip_date | date | False | Chicago local calendar date. |
| year | int | True | Year at this table grain. |
| month | int | True | Month at this table grain. |
| day_of_week | int | True | Day of week at this table grain. |

## silver.dim_payment

Dim payment.

Grain: One row per payment_key.

| Column | Type | Nullable | Description |
|---|---|---|---|
| payment_type | string | True | Source payment category with Unknown fallback. |
| payment_key | string | False | Stable SHA-256 warehouse key for payment. |

## silver.dim_taxi

Dim taxi.

Grain: One row per taxi_key.

| Column | Type | Nullable | Description |
|---|---|---|---|
| taxi_id | string | True | Anonymised vehicle identifier; not a driver identity. |
| taxi_key | string | False | Stable SHA-256 warehouse key for taxi. |

## silver.dim_weather_hour

Dim weather hour.

Grain: One row per weather_local_hour.

| Column | Type | Nullable | Description |
|---|---|---|---|
| weather_local_hour | string | False | Chicago wall-clock hour string; repeated DST hours are flagged. |
| source_hours | bigint | True | Number of UTC source hours mapping to this local hour. |
| temperature_c | double | True | City-level ERA5 temperature in Celsius. |
| precipitation_mm | double | True | City-level ERA5 hourly precipitation in millimetres; never sum duplicated trip-level weather. |
| snowfall_cm | double | True | City-level ERA5 hourly snowfall in centimetres. |
| weather_ambiguous | boolean | True | True when multiple UTC hours map to the same local hour. |

## silver.fact_taxi_trip

Fact taxi trip.

Grain: One row per trip_id.

| Column | Type | Nullable | Description |
|---|---|---|---|
| dropoff_community_area | string | True | Source or enriched dropoff community area; Unknown/null when unavailable. |
| pickup_community_area | string | True | Source or enriched pickup community area; Unknown/null when unavailable. |
| company | string | True | Source company label with Unknown fallback. |
| payment_type | string | True | Source payment category with Unknown fallback. |
| taxi_id | string | True | Anonymised vehicle identifier; not a driver identity. |
| trip_id | string | False | Unique source trip identifier. |
| trip_duration_seconds | decimal(20,6) | False | Reported trip duration in seconds. |
| trip_distance_miles | decimal(20,6) | True | Reported trip distance in miles. |
| fare_usd | decimal(20,6) | True | Reported fare in USD. |
| tips_usd | decimal(20,6) | True | Reported tips in USD; missing values stay null. |
| trip_total_usd | decimal(20,6) | False | Reported total trip charge in USD; not driver profit. |
| started_at | string | True | Chicago local trip start as text; rounded source time, no inferred UTC offset. |
| trip_date | date | False | Chicago local calendar date. |
| trip_hour | int | False | Chicago local hour 0–23. |
| trip_distance_km | decimal(24,6) | False | Reported miles multiplied by 1.609344; not a reconstructed road route. |
| taxi_key | string | False | Stable SHA-256 warehouse key for taxi. |
| payment_key | string | True | Stable SHA-256 warehouse key for payment. |
| company_key | string | True | Stable SHA-256 warehouse key for company. |
| pickup_area_key | string | False | Stable SHA-256 warehouse key for pickup area. |
| dropoff_area_key | string | False | Stable SHA-256 warehouse key for dropoff area. |

## silver.quarantine_taxi_trip

Quarantine taxi trip.

Grain: One row per trip_id.

| Column | Type | Nullable | Description |
|---|---|---|---|
| trip_id | string | False | Unique source trip identifier. |
| taxi_id | string | True | Anonymised vehicle identifier; not a driver identity. |
| trip_start_timestamp | string | True | Trip start timestamp at this table grain. |
| trip_duration_seconds | decimal(20,6) | True | Reported trip duration in seconds. |
| trip_distance_miles | decimal(20,6) | True | Reported trip distance in miles. |
| fare_usd | decimal(20,6) | True | Reported fare in USD. |
| tips_usd | decimal(20,6) | True | Reported tips in USD; missing values stay null. |
| trip_total_usd | decimal(20,6) | True | Reported total trip charge in USD; not driver profit. |
| payment_type | string | True | Source payment category with Unknown fallback. |
| company | string | True | Source company label with Unknown fallback. |
| pickup_community_area | string | True | Source or enriched pickup community area; Unknown/null when unavailable. |
| dropoff_community_area | string | True | Source or enriched dropoff community area; Unknown/null when unavailable. |
| started_at | string | True | Chicago local trip start as text; rounded source time, no inferred UTC offset. |
| rejection_reason | string | True | Semicolon-separated validation failure reasons. |
