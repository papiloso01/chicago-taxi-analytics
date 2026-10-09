"""Latest source record wins; immutable batches preserve the original payload."""
from pyspark.sql import Window, functions as F, types as T
RAW_SCHEMA = T.StructType([
    T.StructField("record_id", T.StringType(), False),
    T.StructField("payload", T.StringType(), False),
    T.StructField("ingested_at_utc", T.StringType(), False),
    T.StructField("run_id", T.StringType(), False),
    T.StructField("sequence", T.LongType(), False),
])

def consolidate(frame, previous=None):
    if previous is not None:
        frame = previous.unionByName(frame)
    order = Window.partitionBy("record_id").orderBy(
        F.col("ingested_at_utc").desc(), F.col("run_id").desc(), F.col("sequence").desc())
    return frame.withColumn("_rank", F.row_number().over(order)).filter("_rank = 1").drop("_rank")
