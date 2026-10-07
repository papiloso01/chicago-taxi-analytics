"""Export trip-grain gold data for Tableau without embedding database credentials."""
import argparse,csv,json,os
from pathlib import Path
from decimal import Decimal,InvalidOperation
FIELDS=['trip_id','trip_date','trip_hour','taxi_id','payment_type','company','pickup_community_area','dropoff_community_area','distance_km','trip_total','tips','trip_seconds']

def sample_rows(path):
    from datetime import datetime
    for raw in json.loads(Path(path).read_text()):
        try:
            started=datetime.fromisoformat(raw['trip_start_timestamp'])
            required={name:Decimal(raw[name]) for name in ['trip_miles','fare','trip_total','trip_seconds']}
            if any(not v.is_finite() or v<0 for v in required.values()):continue
            tips=Decimal(raw['tips']) if raw.get('tips') else None
            if tips is not None and (not tips.is_finite() or tips<0):continue
        except (KeyError,ValueError,InvalidOperation):continue
        row={k:(raw.get(k) or 'Unknown') for k in ['taxi_id','payment_type','company','pickup_community_area','dropoff_community_area']}
        row.update(trip_id=raw['trip_id'],trip_date=started.date(),trip_hour=started.hour,distance_km=(required['trip_miles']*Decimal('1.609344')).quantize(Decimal('0.000001')),trip_total=required['trip_total'],tips=tips,trip_seconds=required['trip_seconds'])
        yield row

def main():
    p=argparse.ArgumentParser();p.add_argument('--sample',type=Path);p.add_argument('--output',type=Path,default=Path('tableau/sample/trips.csv'));a=p.parse_args()
    a.output.parent.mkdir(parents=True,exist_ok=True)
    if a.sample:
        rows=sample_rows(a.sample)
        with a.output.open('w',newline='',encoding='utf-8') as f:
            writer=csv.DictWriter(f,fieldnames=FIELDS);writer.writeheader();writer.writerows(rows)
        print('SYNTHETIC sample exported:',a.output)
    else:
        import psycopg
        # Server-side COPY streams rows; it does not collect the year into Python memory.
        with psycopg.connect(os.environ['DATABASE_URL']) as conn:
            with conn.cursor() as cur, a.output.open('wb') as f:
                with cur.copy('COPY (SELECT '+','.join(FIELDS)+' FROM gold.mart_tableau_trips ORDER BY trip_date,trip_id) TO STDOUT WITH (FORMAT CSV, HEADER TRUE)') as copy:
                    for chunk in copy:f.write(chunk)
        print('Gold dataset exported:',a.output)
if __name__=='__main__':main()
