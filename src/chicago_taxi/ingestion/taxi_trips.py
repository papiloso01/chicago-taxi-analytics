import argparse
import json
import os
import logging
import uuid
from datetime import date
from .windows import daily_window, year_window
from pathlib import Path
from .taxi_api import pages

SCHEMA = """
CREATE SCHEMA IF NOT EXISTS bronze;
CREATE TABLE IF NOT EXISTS bronze.trips (
 trip_id text PRIMARY KEY, payload jsonb NOT NULL,
 ingested_at timestamptz NOT NULL DEFAULT now());
CREATE TABLE IF NOT EXISTS bronze.pipeline_runs (
 run_id uuid PRIMARY KEY, started_at timestamptz DEFAULT now(),
 completed_at timestamptz, start_date date, end_date date,
 status text NOT NULL, extracted_rows bigint DEFAULT 0, error text);
"""


def main():
    parser = argparse.ArgumentParser(description="Extract Chicago taxi trips into PostgreSQL")
    parser.add_argument("--start", default=os.getenv("START_DATE"))
    parser.add_argument("--end", default=os.getenv("END_DATE"))
    parser.add_argument("--sample", type=Path)
    parser.add_argument("--year", type=int, help="Initial completed-calendar-year backfill")
    args = parser.parse_args()
    if args.year and (args.start or args.end):
        parser.error("Use --year OR explicit dates")
    default_start, default_end = year_window(args.year) if args.year else daily_window()
    start = args.start or default_start
    end = args.end or default_end
    if date.fromisoformat(start) >= date.fromisoformat(end):
        parser.error("start must precede end")
    import psycopg
    from psycopg.types.json import Jsonb
    logging.basicConfig(level=logging.INFO, format="%(asctime)s %(levelname)s %(message)s")
    run_id = uuid.uuid4()
    count = 0
    with psycopg.connect(os.environ["DATABASE_URL"]) as conn:
        conn.execute(SCHEMA)
        conn.commit()
        # One writer at a time; session lock also covers independently committed batches.
        locked = conn.execute("SELECT pg_try_advisory_lock(773321)").fetchone()[0]
        if not locked:
            raise RuntimeError("Another ingestion run is active")
        conn.execute("INSERT INTO bronze.pipeline_runs(run_id,start_date,end_date,status) VALUES (%s,%s,%s,'running')", (run_id,start,end))
        conn.commit()
        try:
            batches = ([json.loads(args.sample.read_text())] if args.sample else
                       pages(start,end,os.getenv("DATASET_ID","ajtu-isnz"),os.getenv("SOCRATA_APP_TOKEN","")))
            for batch in batches:
                with conn.cursor() as cur:
                    cur.executemany("""INSERT INTO bronze.trips(trip_id,payload) VALUES (%s,%s)
                    ON CONFLICT(trip_id) DO UPDATE SET payload=excluded.payload, ingested_at=now()
                    WHERE bronze.trips.payload IS DISTINCT FROM excluded.payload""",
                    [(row["trip_id"],Jsonb(row)) for row in batch])
                count += len(batch)
                conn.execute("UPDATE bronze.pipeline_runs SET extracted_rows=%s WHERE run_id=%s", (count,run_id))
                conn.commit()
                logging.info("run=%s extracted_rows=%s",run_id,count)
            conn.execute("UPDATE bronze.pipeline_runs SET status='success', completed_at=now() WHERE run_id=%s",(run_id,))
            conn.commit()
        except Exception as exc:
            conn.rollback()
            conn.execute("UPDATE bronze.pipeline_runs SET status='failed', error=%s, completed_at=now() WHERE run_id=%s",(str(exc)[:1000],run_id))
            conn.commit()
            raise


if __name__ == "__main__":
    main()
