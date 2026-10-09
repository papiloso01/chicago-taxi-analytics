from pyspark.sql import SparkSession
from chicago_taxi.transformations.bronze.records import RAW_SCHEMA, consolidate

def test_latest_correction_wins_without_duplicate_trip():
    spark = SparkSession.builder.master("local[2]").config("spark.sql.shuffle.partitions", "2").getOrCreate()
    try:
        old = spark.createDataFrame([("a", '{"fare":"10"}', "2024-01-01", "run_a", 0)], RAW_SCHEMA)
        new = spark.createDataFrame([("a", '{"fare":"12"}', "2024-01-02", "run_b", 0),
                                     ("b", '{}', "2024-01-02", "run_b", 1)], RAW_SCHEMA)
        result = consolidate(new, old)
        assert result.count() == 2
        assert result.filter("record_id = 'a'").first().payload == '{"fare":"12"}'
        assert consolidate(new, result).count() == 2
    finally:
        spark.stop()
