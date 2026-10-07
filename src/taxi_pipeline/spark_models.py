"""Explicit source schema, silver star model and gold reporting datasets."""
from pyspark.sql import functions as F, types as T

SOURCE_FIELDS = ["trip_id", "taxi_id", "trip_start_timestamp", "trip_seconds",
                 "trip_miles", "fare", "tips", "trip_total", "payment_type",
                 "company", "pickup_community_area", "dropoff_community_area"]
SOURCE_SCHEMA = T.StructType([T.StructField(name, T.StringType(), True) for name in SOURCE_FIELDS])


def build_models(bronze):
    # Raw values arrive as strings. Explicit casts below define the warehouse schema.
    parsed = bronze.select("trip_id", F.from_json("payload", SOURCE_SCHEMA).alias("p"))
    clean = parsed.select("trip_id", *[F.col("p."+name).alias(name) for name in SOURCE_FIELDS if name != "trip_id"])
    clean = clean.withColumn("started_at", F.to_timestamp("trip_start_timestamp"))
    for field in ["trip_seconds", "trip_miles", "fare", "tips", "trip_total"]:
        clean = clean.withColumn(field, F.when(F.col(field).rlike(r"^-?[0-9]+(\.[0-9]+)?$"), F.col(field).cast(T.DecimalType(20,6))))
    for field in ["taxi_id", "payment_type", "company", "pickup_community_area", "dropoff_community_area"]:
        clean = clean.withColumn(field, F.when(F.col(field).isNull() | (F.trim(F.col(field)) == ""), F.lit("Unknown")).otherwise(F.trim(F.col(field))))
    rules = [F.when(F.col("started_at").isNull(), F.lit("invalid_timestamp"))]
    for field in ["trip_seconds", "trip_miles", "fare", "trip_total"]:
        rules.append(F.when(F.col(field).isNull() | (F.col(field)<0), F.lit("invalid_"+field)))
    rules.append(F.when(F.col("tips")<0,F.lit("negative_tips")))
    clean = clean.withColumn("rejection_reason",F.concat_ws(";",*rules))
    rejected = clean.filter(F.col("rejection_reason")!="")
    valid = clean.filter(F.col("rejection_reason")=="").drop("rejection_reason","trip_start_timestamp")
    valid = (valid.withColumn("trip_date",F.to_date("started_at"))
             .withColumn("trip_hour",F.hour("started_at"))
             .withColumn("distance_km",(F.col("trip_miles")*F.lit("1.609344").cast(T.DecimalType(10,6))).cast(T.DecimalType(24,6))))
    dims = {}
    for name, fields in {"taxi":["taxi_id"], "payment":["payment_type"], "company":["company"], "area":["area"]}.items():
        values = (valid.select(F.col("pickup_community_area").alias("area")).union(valid.select(F.col("dropoff_community_area").alias("area"))) if name=="area" else valid.select(*fields))
        dims[name] = values.distinct().withColumn(name+"_key",F.sha2(F.col(fields[0]),256))
    fact = valid
    for name, natural in [("taxi","taxi_id"),("payment","payment_type"),("company","company")]:
        fact = fact.join(dims[name],natural,"left")
    for role in ["pickup","dropoff"]:
        dim=dims["area"].select(F.col("area").alias(role+"_community_area"),F.col("area_key").alias(role+"_area_key"))
        fact=fact.join(dim,role+"_community_area","left")
    dates=valid.select("trip_date").distinct().withColumn("year",F.year("trip_date")).withColumn("month",F.month("trip_date")).withColumn("day_of_week",F.dayofweek("trip_date"))
    joined=fact.join(dims["taxi"].select("taxi_key",F.col("taxi_id").alias("taxi_label")),"taxi_key")
    daily=fact.groupBy("trip_date").agg(F.count("*").alias("trips"),F.sum("trip_total").alias("revenue_usd"),F.sum("tips").alias("tips_usd"),F.avg("distance_km").alias("average_distance_km"),F.avg("trip_total").alias("average_trip_usd"))
    taxi=joined.groupBy("trip_date","taxi_key","taxi_label").agg(F.count("*").alias("trips"),F.sum("trip_total").alias("reported_total_usd"))
    # Retain trip-level distances: BI can resolve extremes and all ties for any date filter.
    distances=fact.select("trip_id","trip_date","taxi_id","distance_km","pickup_community_area","dropoff_community_area").withColumn("is_positive_distance",F.col("distance_km")>0)
    hourly=fact.groupBy("trip_date","trip_hour").agg(F.count("*").alias("trips"),F.sum("trip_total").alias("revenue_usd"))
    payment=fact.groupBy("trip_date","payment_type").agg(F.count("*").alias("trips"),F.sum("trip_total").alias("revenue_usd"))
    models={"silver.fct_trips":fact,"silver.quarantine_trips":rejected,"silver.dim_date":dates,
            "gold.mart_daily":daily,"gold.mart_taxi_earnings":taxi,"gold.mart_trip_distances":distances,
            "gold.mart_hourly":hourly,"gold.mart_payment":payment}
    models["gold.mart_tableau_trips"] = fact.select("trip_id","trip_date","trip_hour","taxi_id","payment_type","company","pickup_community_area","dropoff_community_area","distance_km","trip_total","tips","trip_seconds")
    models.update({"silver.dim_"+k:v for k,v in dims.items()})
    return models
