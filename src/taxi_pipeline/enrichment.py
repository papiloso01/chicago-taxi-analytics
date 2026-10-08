"""Versioned geography and historical weather ingestion; all dates end-exclusive."""
import argparse
import json
import math
import os
import time
import uuid
from datetime import datetime, date, timedelta, timezone
from zoneinfo import ZoneInfo
from urllib.request import Request, urlopen
from urllib.parse import urlencode
from urllib.error import HTTPError, URLError

ZONE=ZoneInfo("America/Chicago")
VARIABLES=["temperature_2m","precipitation","snowfall"]
SCHEMA="""
CREATE SCHEMA IF NOT EXISTS bronze;
CREATE TABLE IF NOT EXISTS bronze.community_areas(area_id text PRIMARY KEY,payload jsonb NOT NULL,ingested_at timestamptz DEFAULT now());
CREATE TABLE IF NOT EXISTS bronze.weather_hours(hour_utc timestamptz PRIMARY KEY,payload jsonb NOT NULL,ingested_at timestamptz DEFAULT now());
CREATE TABLE IF NOT EXISTS bronze.enrichment_runs(run_id uuid PRIMARY KEY,started_at timestamptz DEFAULT now(),completed_at timestamptz,status text,start_date date,end_date date,community_rows integer DEFAULT 0,weather_rows integer DEFAULT 0,error text);
"""

def get_json(url):
    for attempt in range(5):
        try:
            with urlopen(Request(url,headers={"User-Agent":"chicago-taxi-analytics/1.0"}),timeout=90) as response:return json.load(response)
        except (HTTPError,URLError,TimeoutError) as exc:
            if isinstance(exc,HTTPError) and exc.code not in (429,500,502,503,504):raise
            if attempt==4:raise
            time.sleep(2**attempt)


def normalize_areas(rows):
    if not rows:raise ValueError("Empty community area snapshot")
    result=[];seen=set()
    for row in rows:
        key=str(int(row["area_num_1"]))
        geometry=row.get("the_geom",{})
        if key in seen or not 1<=int(key)<=77:raise ValueError("Duplicate/invalid community ID")
        if not row.get("community") or geometry.get("type") not in ("Polygon","MultiPolygon") or not geometry.get("coordinates"):
            raise ValueError("Missing community name or polygon")
        seen.add(key);result.append((key,row))
    return result


def normalize_weather(response,start,end):
    hourly=response["hourly"];units=response["hourly_units"]
    for key,unit in {"temperature_2m":"°C","precipitation":"mm","snowfall":"cm"}.items():
        if units.get(key)!=unit:raise ValueError("Unexpected weather units: "+key)
    times=hourly["time"]
    if any(len(hourly[k])!=len(times) for k in VARIABLES):raise ValueError("Weather array length mismatch")
    result=[];seen=set()
    for i,ts in enumerate(times):
        utc=datetime.fromisoformat(ts)
        utc=utc.replace(tzinfo=timezone.utc) if utc.tzinfo is None else utc.astimezone(timezone.utc)
        if utc in seen or utc.minute or utc.second:raise ValueError("Duplicate/non-hourly weather timestamp")
        seen.add(utc);local=utc.astimezone(ZONE)
        values={key:hourly[key][i] for key in VARIABLES}
        for key,value in values.items():
            if value is not None and (not isinstance(value,(int,float)) or not math.isfinite(value) or (key!='temperature_2m' and value<0)):
                raise ValueError("Invalid weather measurement")
        if not start<=local.date()<end:continue
        payload=dict(values,hour_utc=utc.isoformat(),local_hour=local.strftime("%Y-%m-%dT%H:00:00"),model="era5",latitude=41.8781,longitude=-87.6298)
        result.append((utc,payload))
    return result


def weather_batches(start,end,fetch=get_json):
    # Request UTC margins so local end-day evening hours are included.
    cursor=start
    while cursor<end:
        next_date=min(cursor+timedelta(days=30),end)
        params=dict(latitude=41.8781,longitude=-87.6298,start_date=str(cursor-timedelta(days=1)),end_date=str(next_date),hourly=','.join(VARIABLES),timezone='GMT',models='era5')
        yield normalize_weather(fetch('https://archive-api.open-meteo.com/v1/archive?'+urlencode(params)),cursor,next_date)
        cursor=next_date


def main():
    p=argparse.ArgumentParser();p.add_argument('--start');p.add_argument('--end');p.add_argument('--sample');args=p.parse_args()
    today=datetime.now(ZONE).date();end=date.fromisoformat(args.end) if args.end else today-timedelta(days=5)
    start=date.fromisoformat(args.start) if args.start else end-timedelta(days=14)
    if start>=end:p.error('start must precede exclusive end')
    if args.sample:
        sample=json.load(open(args.sample));areas=normalize_areas(sample['areas']);batches=[normalize_weather(sample['weather'],start,end)]
    else:
        areas=normalize_areas(get_json('https://data.cityofchicago.org/resource/igwz-8jzy.json?$limit=1000'))
        if len(areas)!=77:raise ValueError('Expected complete 77-area snapshot')
        batches=weather_batches(start,end)
    import psycopg
    from psycopg.types.json import Jsonb
    run_id=uuid.uuid4();count=0
    with psycopg.connect(os.environ['DATABASE_URL']) as conn:
        conn.execute(SCHEMA);conn.commit()
        if not conn.execute('SELECT pg_try_advisory_lock(773321)').fetchone()[0]:raise RuntimeError('Another pipeline job is active')
        conn.execute("INSERT INTO bronze.enrichment_runs(run_id,status,start_date,end_date) VALUES(%s,'running',%s,%s)",(run_id,start,end));conn.commit()
        try:
            with conn.cursor() as cur:
                cur.executemany('INSERT INTO bronze.community_areas(area_id,payload) VALUES(%s,%s) ON CONFLICT(area_id) DO UPDATE SET payload=excluded.payload,ingested_at=now() WHERE bronze.community_areas.payload IS DISTINCT FROM excluded.payload',[(key,Jsonb(row)) for key,row in areas])
            conn.execute('UPDATE bronze.enrichment_runs SET community_rows=%s WHERE run_id=%s',(len(areas),run_id));conn.commit()
            for batch in batches:
                with conn.cursor() as cur:
                    cur.executemany('INSERT INTO bronze.weather_hours(hour_utc,payload) VALUES(%s,%s) ON CONFLICT(hour_utc) DO UPDATE SET payload=excluded.payload,ingested_at=now() WHERE bronze.weather_hours.payload IS DISTINCT FROM excluded.payload',[(ts,Jsonb(row)) for ts,row in batch])
                count+=len(batch);conn.execute('UPDATE bronze.enrichment_runs SET weather_rows=%s WHERE run_id=%s',(count,run_id));conn.commit()
            conn.execute("UPDATE bronze.enrichment_runs SET status='success',community_rows=%s,weather_rows=%s,completed_at=now() WHERE run_id=%s",(len(areas),count,run_id));conn.commit()
        except Exception as exc:
            conn.rollback();conn.execute("UPDATE bronze.enrichment_runs SET status='failed',error=%s,completed_at=now() WHERE run_id=%s",(str(exc)[:1000],run_id));conn.commit();raise
    print('Enrichment loaded:',len(areas),'areas,',count,'weather hours')
if __name__=='__main__':main()
