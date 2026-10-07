import unittest
from datetime import date
from taxi_pipeline.windows import daily_window,year_window
class WindowTests(unittest.TestCase):
    def test_daily(self):
        self.assertEqual(daily_window(date(2026,1,1)),("2025-12-31","2026-01-01"))
    def test_leap(self):
        self.assertEqual(daily_window(date(2024,3,1)),("2024-02-29","2024-03-01"))
    def test_year(self):
        self.assertEqual(year_window(2025),("2025-01-01","2026-01-01"))
    def test_invalid_year(self):
        with self.assertRaises(ValueError):year_window(2023)
