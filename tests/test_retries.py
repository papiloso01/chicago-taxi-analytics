import io
import unittest
from unittest.mock import patch
from urllib.error import HTTPError
from taxi_pipeline.api import request_json
class RetryTests(unittest.TestCase):
    def test_temporary_failure_retries(self):
        with patch('taxi_pipeline.api.urlopen',side_effect=[HTTPError('x',429,'limit',{},None),io.BytesIO(b'[]')]) as call, patch('taxi_pipeline.api.time.sleep'):
            self.assertEqual(request_json('https://example.invalid',''),[])
            self.assertEqual(call.call_count,2)
    def test_permanent_failure_does_not_retry(self):
        with patch('taxi_pipeline.api.urlopen',side_effect=HTTPError('x',403,'forbidden',{},None)) as call:
            with self.assertRaises(HTTPError):request_json('https://example.invalid','')
            self.assertEqual(call.call_count,1)
    def test_exhausted_retries(self):
        with patch('taxi_pipeline.api.urlopen',side_effect=HTTPError('x',503,'unavailable',{},None)) as call, patch('taxi_pipeline.api.time.sleep'):
            with self.assertRaises(HTTPError):request_json('https://example.invalid','')
            self.assertEqual(call.call_count,5)
