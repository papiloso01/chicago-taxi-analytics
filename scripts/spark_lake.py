"""Optional Spark lake export of raw database data using partitioned JDBC reads.
Requires Java 17, pip install '.[spark]', PostgreSQL JDBC JAR and a completed API load.
Example: spark-submit --jars /path/postgresql.jar scripts/spark_lake.py
Legacy raw-database export; the native Athena workflow is pipelines/run_lake.py.
"""
import os
from pyspark.sql import SparkSession, functions as F
spark = SparkSession.builder.appName("taxi-bronze-lake").getOrCreate()
# hash buckets allow parallel reads without requiring sequential numeric IDs.
query = "(select trip_id, payload::text as payload, ((hashtext(trip_id)::bigint + 2147483648) % 16)::int as bucket from bronze.trips) src"
raw = (spark.read.format("jdbc").option("url",os.environ["JDBC_URL"])
       .option("user",os.environ["POSTGRES_USER"]).option("password",os.environ["POSTGRES_PASSWORD"])
       .option("driver","org.postgresql.Driver").option("dbtable",query)
       .option("partitionColumn","bucket").option("lowerBound",0).option("upperBound",16)
       .option("numPartitions",16).option("fetchsize",10000).load())
(raw.withColumn("trip_date",F.substring(F.get_json_object("payload","$.trip_start_timestamp"),1,10))
    .drop("bucket").write.mode("overwrite").partitionBy("trip_date")
    .parquet(os.getenv("LAKE_PATH","data/legacy_lake/bronze")))
spark.stop()
