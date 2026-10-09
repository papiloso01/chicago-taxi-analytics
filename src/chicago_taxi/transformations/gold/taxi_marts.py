"""Build reporting grains from the silver trip fact."""
from pyspark.sql import functions as F

def build_marts(fact):
    joined=fact.withColumn("taxi_label", F.col("taxi_id"))
    daily=fact.groupBy("trip_date").agg(F.count("*").alias("trips"),F.sum("trip_total").alias("revenue_usd"),F.sum("tips").alias("tips_usd"),F.avg("distance_km").alias("average_distance_km"),F.avg("trip_total").alias("average_trip_usd"))
    taxi=joined.groupBy("trip_date","taxi_key","taxi_label").agg(F.count("*").alias("trips"),F.sum("trip_total").alias("reported_total_usd"))
    # Retain trip-level distances: BI can resolve extremes and all ties for any date filter.
    distances=fact.select("trip_id","trip_date","taxi_id","distance_km","pickup_community_area","dropoff_community_area").withColumn("is_positive_distance",F.col("distance_km")>0)
    hourly=fact.groupBy("trip_date","trip_hour").agg(F.count("*").alias("trips"),F.sum("trip_total").alias("revenue_usd"))
    payment=fact.groupBy("trip_date","payment_type").agg(F.count("*").alias("trips"),F.sum("trip_total").alias("revenue_usd"))
    return {"gold.mart_daily": daily, "gold.mart_taxi_earnings": taxi,
            "gold.mart_trip_distances": distances, "gold.mart_hourly": hourly,
            "gold.mart_payment": payment}
