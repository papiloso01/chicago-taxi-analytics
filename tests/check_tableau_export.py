import csv,sys
from decimal import Decimal
with open(sys.argv[1],newline='',encoding='utf-8') as f:rows=list(csv.DictReader(f))
assert len(rows)==120
assert len({r['trip_id'] for r in rows})==120
assert sum(Decimal(r['trip_total']) for r in rows)==Decimal('2452')
assert all(Decimal(r['distance_km'])>0 for r in rows)
print('Gold-to-Tableau export grain and values passed')
