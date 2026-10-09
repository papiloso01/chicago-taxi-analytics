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

from chicago_taxi.ingestion.community_areas import normalize_areas
from chicago_taxi.ingestion.weather import get_json, normalize_weather, weather_batches


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
