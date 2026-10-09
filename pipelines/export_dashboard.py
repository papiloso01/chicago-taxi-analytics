"""Stream local gold Parquet to CSV using DuckDB; no driver-wide row collection."""
import argparse
import json
from pathlib import Path
import duckdb


def export(root='data/lake', output='data/exports'):
    root, output = Path(root).resolve(), Path(output).resolve()
    output.mkdir(parents=True, exist_ok=True)
    manifest = json.loads((root / 'current.json').read_text())
    folder = root / 'snapshots' / manifest['run_id']
    # Keep existing Tableau filenames and fields compatible during migration.
    names = {'mart_taxi_trip_enriched': 'mart_enriched_trips',
             'mart_weather_demand_hourly': 'mart_weather_demand',
             'mart_community_area_demand_daily': 'mart_area_demand'}
    aliases = {'trip_duration_seconds':'trip_seconds','trip_distance_miles':'trip_miles',
               'trip_distance_km':'distance_km','trip_total_usd':'trip_total','fare_usd':'fare','tips_usd':'tips',
               'temperature_c':'temperature_2m','precipitation_mm':'precipitation','snowfall_cm':'snowfall'}
    with duckdb.connect() as conn:
        for table, filename in names.items():
            files = str(folder / 'gold' / table / '**/*.parquet')
            frame = conn.read_parquet(files, hive_partitioning=True)
            table_aliases = aliases if table == 'mart_taxi_trip_enriched' else {'average_trip_duration_seconds': 'average_trip_seconds'}
            projection = ', '.join('"' + c + '" AS "' + table_aliases.get(c, c) + '"' for c in frame.columns)
            frame.project(projection).write_csv(str(output / (filename + '.csv')), header=True)
        features = []
        rows = conn.read_parquet(str(folder / 'silver/dim_community_area/*.parquet')).fetchall()
        columns = conn.read_parquet(str(folder / 'silver/dim_community_area/*.parquet')).columns
        for values in rows:
            row = dict(zip(columns, values))
            features.append(dict(type='Feature', properties=dict(area_id=row['area_id'], area_name=row['area_name']), geometry=json.loads(row['geometry_geojson'])))
        (output / 'community_areas.geojson').write_text(json.dumps(dict(type='FeatureCollection', features=features)))
    return output

if __name__ == '__main__':
    parser = argparse.ArgumentParser()
    parser.add_argument('--root', default='data/lake')
    parser.add_argument('--output', default='data/exports')
    args = parser.parse_args()
    print(export(args.root, args.output))
