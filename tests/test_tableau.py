import csv
import importlib.util
import unittest
from decimal import Decimal
from pathlib import Path
import xml.etree.ElementTree as E
ROOT=Path(__file__).resolve().parents[1]
class TableauTests(unittest.TestCase):
    def test_workbook_references_and_sample_connection(self):
        root=E.parse(ROOT/'tableau/chicago-taxi.twb').getroot()
        names={w.attrib['name'] for w in root.findall('worksheets/worksheet')}
        self.assertEqual(len(names),11)
        for zone in root.findall('dashboards/dashboard/zones/zone'):
            self.assertIn(zone.attrib['name'],names)
        self.assertEqual(len(root.findall('dashboards/dashboard/zones/zone')),11)
        conn=root.find('datasources/datasource/connection')
        self.assertEqual(conn.attrib['class'],'textscan')
        self.assertNotIn('password',conn.attrib)
        self.assertTrue((ROOT/'tableau'/conn.attrib['directory']/conn.attrib['filename']).is_file())
        formula=root.find("datasources/datasource/column[@name='[Shortest positive trip km]']/calculation").attrib['formula']
        self.assertIn('[distance_km] > 0',formula)
    def test_sample_grain_and_totals(self):
        with (ROOT/'tableau/sample/trips.csv').open() as f:rows=list(csv.DictReader(f))
        self.assertEqual(len(rows),120)
        self.assertEqual(len({r['trip_id'] for r in rows}),120)
        self.assertEqual(sum(Decimal(r['trip_total']) for r in rows),Decimal('2452'))
        self.assertEqual(Decimal(rows[0]['distance_km']),Decimal('1.609344'))
        self.assertTrue(all(r['trip_id'].startswith('sample-') for r in rows))
    def test_source_export_matches_fixture(self):
        spec=importlib.util.spec_from_file_location('export',ROOT/'tableau/export_data.py');module=importlib.util.module_from_spec(spec);spec.loader.exec_module(module)
        rows=list(module.sample_rows(ROOT/'data/sample/trips.json'))
        self.assertEqual(len(rows),120)
        self.assertEqual(set(rows[0]),set(module.FIELDS))
