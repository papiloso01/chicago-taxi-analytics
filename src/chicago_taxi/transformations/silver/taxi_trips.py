"""Parse and quarantine raw taxi trips; build the silver star schema."""
from pyspark.sql import functions as F, types as T
from chicago_taxi.schemas.bronze.taxi_trips import SOURCE_FIELDS, SOURCE_SCHEMA

def clean_trips(bronze):
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
    return valid, rejected

def build_warehouse(bronze):
    valid, rejected = clean_trips(bronze)
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
    result = {"silver.fct_trips": fact, "silver.quarantine_trips": rejected, "silver.dim_date": dates}
    result.update({"silver.dim_" + name: frame for name, frame in dims.items()})
    return result
