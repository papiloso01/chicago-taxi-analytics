import unittest
from urllib.parse import urlparse, parse_qs
from taxi_pipeline.api import pages

class ApiTests(unittest.TestCase):
    def test_keyset_pagination(self):
        calls=[]
        responses=iter([[{"trip_id":"a"},{"trip_id":"b"}],[{"trip_id":"c"}]])
        def fetch(url,token):
            calls.append(parse_qs(urlparse(url).query))
            return next(responses)
        result=list(pages("2024-01-01","2024-01-02",page_size=2,fetch=fetch))
        self.assertEqual(sum(map(len,result)),3)
        self.assertIn("trip_id > 'b'",calls[1]["$where"][0])
        self.assertIn("< '2024-01-02",calls[0]["$where"][0])

    def test_empty_source(self):
        self.assertEqual(list(pages("2024-01-01","2024-01-02",fetch=lambda *_: [])),[])

    def test_invalid_range(self):
        with self.assertRaises(ValueError):
            list(pages("2024-01-03","2024-01-02"))

    def test_stuck_cursor(self):
        with self.assertRaises(ValueError):
            list(pages("2024-01-01","2024-01-02",page_size=1,fetch=lambda *_:[{"trip_id":"a"}]))

    def test_missing_identity(self):
        with self.assertRaises(ValueError):
            list(pages("2024-01-01","2024-01-02",fetch=lambda *_:[{}]))
