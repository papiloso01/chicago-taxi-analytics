"""Blocking checks before any reporting tables are published."""
from pyspark.sql import functions as F

def assert_unique(frame, keys, name):
    if frame.groupBy(*keys).count().filter("count > 1").limit(1).count():
        raise ValueError(f"Duplicate grain: {name}")

def validate_models(bronze, models):
    fact = models["silver.fct_trips"]
    rejected = models["silver.quarantine_trips"]
    enriched = models["gold.mart_enriched_trips"]
    count = fact.count()
    if bronze.count() != count + rejected.count():
        raise ValueError("Bronze != silver accepted + quarantine")
    for name in ["silver.fct_trips", "silver.quarantine_trips", "gold.mart_enriched_trips"]:
        assert_unique(models[name], ["trip_id"], name)
    for name, key in [("silver.dim_taxi", "taxi_key"), ("silver.dim_company", "company_key"),
                      ("silver.dim_payment", "payment_key"), ("silver.dim_area", "area_key"),
                      ("silver.dim_date", "trip_date"), ("silver.dim_community_area", "area_id"),
                      ("silver.dim_weather_hour", "weather_local_hour")]:
        assert_unique(models[name], [key], name)
    total = fact.agg(F.sum("trip_total")).first()[0]
    if enriched.count() != count or enriched.agg(F.sum("trip_total")).first()[0] != total:
        raise ValueError("Enrichment changed trip counts or revenue")
    for name in ["gold.mart_daily", "gold.mart_taxi_earnings", "gold.mart_hourly", "gold.mart_payment", "gold.mart_weather_demand"]:
        frame = models[name]
        amount = next(c for c in ["revenue_usd", "reported_total_usd"] if c in frame.columns)
        row = frame.agg(F.sum("trips"), F.sum(amount)).first()
        if row[0] != (count or None) or row[1] != total:
            raise ValueError("Gold reconciliation failed: " + name)
    for activity in ["Pickup", "Dropoff"]:
        row = models["gold.mart_area_demand"].filter(F.col("activity") == activity).agg(F.sum("trips"), F.sum("reported_total_usd")).first()
        if row[0] != (count or None) or row[1] != total:
            raise ValueError("Area activity reconciliation failed")
    for role in ["taxi", "payment", "company", "pickup_area", "dropoff_area"]:
        if fact.filter(F.col(role + "_key").isNull()).limit(1).count():
            raise ValueError("Missing dimension key: " + role)
    return {"bronze_trips": bronze.count(), "accepted_trips": count,
            "quarantined_trips": rejected.count(), "reported_total_usd": str(total)}
