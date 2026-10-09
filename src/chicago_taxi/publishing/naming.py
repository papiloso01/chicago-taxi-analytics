"""Explicit migration map: PostgreSQL names remain compatible; Athena names are canonical."""
TABLE_NAMES = {
 "silver.fct_trips": "silver.fact_taxi_trip",
 "silver.quarantine_trips": "silver.quarantine_taxi_trip",
 "gold.mart_daily": "gold.mart_taxi_demand_daily",
 "gold.mart_taxi_earnings": "gold.mart_taxi_revenue_daily",
 "gold.mart_trip_distances": "gold.mart_taxi_trip_distance",
 "gold.mart_hourly": "gold.mart_taxi_demand_hourly",
 "gold.mart_payment": "gold.mart_payment_demand_daily",
 "gold.mart_enriched_trips": "gold.mart_taxi_trip_enriched",
 "gold.mart_weather_demand": "gold.mart_weather_demand_hourly",
 "gold.mart_area_demand": "gold.mart_community_area_demand_daily",
}
FIELD_NAMES = {
 "trip_seconds": "trip_duration_seconds", "trip_miles": "trip_distance_miles",
 "distance_km": "trip_distance_km", "fare": "fare_usd", "tips": "tips_usd",
 "trip_total": "trip_total_usd", "temperature_2m": "temperature_c",
 "precipitation": "precipitation_mm", "snowfall": "snowfall_cm",
 "average_trip_seconds": "average_trip_duration_seconds",
}

def canonical_models(models):
    from pyspark.sql import functions as F, types as T
    result = {}
    for name, frame in models.items():
        # A taxi timestamp is a Chicago wall-clock value, without a known DST offset.
        # Store its text exactly; trip_date/hour drive reporting and weather joins.
        fields = [F.col(field.name).cast("string").alias(FIELD_NAMES.get(field.name, field.name))
                  if isinstance(field.dataType, T.TimestampType) else
                  F.col(field.name).alias(FIELD_NAMES.get(field.name, field.name))
                  for field in frame.schema]
        result[TABLE_NAMES.get(name, name)] = frame.select(*fields)
    return result
