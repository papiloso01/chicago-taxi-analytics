"""Export map polygons and BI marts. Outputs are ignored by git."""
import json,os
from pathlib import Path
import psycopg
output=Path('data/exports');output.mkdir(parents=True,exist_ok=True)
with psycopg.connect(os.environ['DATABASE_URL']) as conn:
    rows=conn.execute('SELECT area_id,area_name,geometry_geojson FROM silver.dim_community_area ORDER BY area_id').fetchall()
    features=[dict(type='Feature',properties=dict(area_id=key,area_name=name),geometry=json.loads(geometry)) for key,name,geometry in rows]
    (output/'community_areas.geojson').write_text(json.dumps(dict(type='FeatureCollection',features=features)))
    for table in ['mart_area_demand','mart_weather_demand','mart_enriched_trips']:
        with conn.cursor() as cur,(output/(table+'.csv')).open('wb') as f:
            with cur.copy('COPY (SELECT * FROM gold.'+table+') TO STDOUT WITH (FORMAT CSV, HEADER TRUE)') as chunks:
                for chunk in chunks:f.write(chunk)
print(output.resolve())
