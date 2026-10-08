import json,unittest
from decimal import Decimal
from datetime import date
from pyspark.sql import SparkSession
from taxi_pipeline.enrichment_models import build_enrichment
class EnrichmentSparkTests(unittest.TestCase):
    @classmethod
    def setUpClass(cls):
        cls.spark=SparkSession.builder.master('local[2]').appName('enrichment-test').config('spark.sql.shuffle.partitions','2').config('spark.sql.ansi.enabled','false').getOrCreate()
    @classmethod
    def tearDownClass(cls):cls.spark.stop()
    def test_missing_and_ambiguous_weather_preserve_trips(self):
        facts=self.spark.createDataFrame([('a',date(2024,11,3),1,'1','2',Decimal('12'),Decimal('60')),('b',date(2024,11,3),2,'99','Unknown',Decimal('8'),Decimal('90')),('c',date(2024,11,3),3,'1','1',Decimal('4'),Decimal('30'))],['trip_id','trip_date','trip_hour','pickup_community_area','dropoff_community_area','trip_total','trip_seconds'])
        area=self.spark.createDataFrame([('1',json.dumps({'community':'A','the_geom':{'type':'Polygon','coordinates':[[[0,0],[1,0],[0,1],[0,0]]]}}))],['area_id','payload'])
        raw=json.dumps({'local_hour':'2024-11-03T01:00:00','temperature_2m':5.0,'precipitation':1.0,'snowfall':0.0})
        snow=json.dumps({'local_hour':'2024-11-03T03:00:00','temperature_2m':-2.0,'precipitation':2.0,'snowfall':1.0})
        weather=self.spark.createDataFrame([(raw,),(raw,),(snow,)],['payload'])
        models=build_enrichment(facts,area,weather);rows={r.trip_id:r for r in models['gold.mart_enriched_trips'].collect()}
        self.assertEqual(len(rows),3);self.assertEqual(sum(r.trip_total for r in rows.values()),Decimal('24'))
        self.assertTrue(rows['a'].weather_ambiguous);self.assertFalse(rows['a'].weather_available)
        self.assertEqual(rows['a'].weather_condition,'Ambiguous DST hour')
        self.assertEqual(rows['b'].pickup_area_name,'Unknown');self.assertEqual(rows['b'].weather_condition,'Unavailable')
        self.assertEqual(rows['c'].weather_condition,'Snow');self.assertTrue(rows['c'].weather_available)
        self.assertEqual(models['silver.dim_weather_hour'].count(),2)
