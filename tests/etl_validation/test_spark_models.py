import json
import unittest
from decimal import Decimal
from pyspark.sql import SparkSession
from chicago_taxi.transformations.models import build_models

class SparkTests(unittest.TestCase):
    @classmethod
    def setUpClass(cls):
        cls.spark=(SparkSession.builder.master("local[2]").appName("model-tests")
                   .config("spark.sql.shuffle.partitions","2").config("spark.sql.ansi.enabled","false")
                   .config("spark.sql.session.timeZone","America/Chicago").getOrCreate())
    @classmethod
    def tearDownClass(cls):cls.spark.stop()
    def test_clean_join_and_metrics(self):
        base=dict(trip_id="a",trip_start_timestamp="2025-01-01T10:00:00",taxi_id="taxi-1",trip_miles="1",trip_seconds="60",fare="10",trip_total="12",tips="2",payment_type="Cash",company="A",pickup_community_area="1",dropoff_community_area="2")
        rows=[base,dict(base,trip_id="missing-tips",tips=None),dict(base,trip_id="b",trip_miles="0",taxi_id=None),dict(base,trip_id="bad",fare="broken"),dict(base,trip_id="date",trip_start_timestamp="invalid"),dict(base,trip_id="negative",trip_total="-1")]
        models=build_models(self.spark.createDataFrame([(r["trip_id"],json.dumps(r)) for r in rows],["trip_id","payload"]))
        facts=models["silver.fct_trips"].collect()
        self.assertEqual(len(facts),3)
        self.assertEqual(models["silver.quarantine_trips"].count(),3)
        self.assertEqual(next(r for r in facts if r.trip_id=="a").distance_km,Decimal("1.609344"))
        self.assertTrue(all(r.taxi_key and r.pickup_area_key and r.dropoff_area_key for r in facts))
        self.assertEqual(models["gold.mart_daily"].first().revenue_usd,Decimal("36"))
        self.assertEqual(models["gold.mart_trip_distances"].filter("is_positive_distance").count(),2)
        self.assertEqual(models["silver.dim_area"].count(),2)
