import copy
import unittest
from datetime import date
from chicago_taxi.ingestion.weather_community import normalize_areas,normalize_weather,weather_batches
UNITS={'temperature_2m':'°C','precipitation':'mm','snowfall':'cm'}
def response(times,temps=None):return {'hourly_units':UNITS,'hourly':{'time':times,'temperature_2m':temps or [5.0]*len(times),'precipitation':[0.0]*len(times),'snowfall':[0.0]*len(times)}}
class EnrichmentTests(unittest.TestCase):
    def test_duplicate_area_rejected(self):
        row={'area_num_1':'1','community':'A','the_geom':{'type':'Polygon','coordinates':[[[0,0],[1,1],[0,0]]]}}
        with self.assertRaises(ValueError):normalize_areas([row,row])
    def test_utc_margin_and_exclusive_end(self):
        rows=normalize_weather(response(['2024-01-01T05:00','2024-01-01T06:00','2024-01-02T05:00','2024-01-02T06:00']),date(2024,1,1),date(2024,1,2))
        self.assertEqual(len(rows),2)
        self.assertEqual(rows[0][1]['local_hour'],'2024-01-01T00:00:00')
        self.assertEqual(rows[1][1]['local_hour'],'2024-01-01T23:00:00')
    def test_dst_fold_preserves_distinct_utc_rows(self):
        rows=normalize_weather(response(['2024-11-03T06:00','2024-11-03T07:00']),date(2024,11,3),date(2024,11,4))
        self.assertEqual(len(rows),2)
        self.assertNotEqual(rows[0][0],rows[1][0]);self.assertEqual(rows[0][1]['local_hour'],rows[1][1]['local_hour'])
    def test_duplicate_utc_rejected(self):
        with self.assertRaises(ValueError):normalize_weather(response(['2024-01-01T06:00']*2),date(2024,1,1),date(2024,1,2))
    def test_bad_units_and_lengths(self):
        bad=response(['2024-01-01T06:00']);bad['hourly_units']=dict(UNITS,precipitation='inch')
        with self.assertRaises(ValueError):normalize_weather(bad,date(2024,1,1),date(2024,1,2))
        bad=response(['2024-01-01T06:00']);bad['hourly']['snowfall']=[]
        with self.assertRaises(ValueError):normalize_weather(bad,date(2024,1,1),date(2024,1,2))
    def test_null_measurement_is_not_zero(self):
        bad=response(['2024-01-01T06:00']);bad['hourly']['precipitation']=[None]
        self.assertIsNone(normalize_weather(bad,date(2024,1,1),date(2024,1,2))[0][1]['precipitation'])
    def test_chunking_bounds(self):
        calls=[]
        def fetch(url):calls.append(url);return response([])
        list(weather_batches(date(2024,1,1),date(2024,3,2),fetch))
        self.assertEqual(len(calls),3)
        self.assertTrue(all('timezone=GMT' in u and 'models=era5' in u for u in calls))
