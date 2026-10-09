from pyspark.sql import types as T

SOURCE_FIELDS = ["trip_id", "taxi_id", "trip_start_timestamp", "trip_seconds",
                 "trip_miles", "fare", "tips", "trip_total", "payment_type",
                 "company", "pickup_community_area", "dropoff_community_area"]
SOURCE_SCHEMA = T.StructType([T.StructField(name, T.StringType(), True) for name in SOURCE_FIELDS])

