"""Dependency-free local reporting demo. Synthetic data, not an API run."""
import json
import sqlite3
from pathlib import Path
root = Path(__file__).resolve().parents[1]
rows = json.loads((root / "data/sample/trips.json").read_text())
with sqlite3.connect(root / "data/demo.db") as conn:
    conn.executescript("DROP TABLE IF EXISTS trips; CREATE TABLE trips(trip_id TEXT PRIMARY KEY,trip_date TEXT,trip_hour INTEGER,trip_total REAL,tips REAL,trip_miles REAL,trip_seconds REAL,payment_type TEXT);")
    for row in rows:
        try:
            total,fare,miles,seconds = [float(row[k]) for k in ("trip_total","fare","trip_miles","trip_seconds")]
            if min(total,fare,miles,seconds) < 0:
                continue
            conn.execute("INSERT OR REPLACE INTO trips VALUES(?,?,?,?,?,?,?,?)",(row["trip_id"],row["trip_start_timestamp"][:10],int(row["trip_start_timestamp"][11:13]),total,float(row["tips"]),miles,seconds,row["payment_type"]))
        except (ValueError, KeyError):
            continue
    conn.executescript("""
    DROP TABLE IF EXISTS mart_daily;
    CREATE TABLE mart_daily AS SELECT trip_date,count(*) trips,sum(trip_total) revenue_usd,sum(tips) tips_usd,avg(trip_total) average_trip_usd,avg(trip_miles) average_miles,avg(trip_seconds)/60 average_minutes FROM trips GROUP BY trip_date;
    DROP TABLE IF EXISTS mart_payment;
    CREATE TABLE mart_payment AS SELECT trip_date,payment_type,count(*) trips,sum(trip_total) revenue_usd FROM trips GROUP BY trip_date,payment_type;
    DROP TABLE IF EXISTS mart_hourly;
    CREATE TABLE mart_hourly AS SELECT trip_date,trip_hour,count(*) trips,sum(trip_total) revenue_usd FROM trips GROUP BY trip_date,trip_hour;
    """)
    print("Synthetic demo report:",conn.execute("SELECT sum(trips),round(sum(revenue_usd),2) FROM mart_daily").fetchone())
print("Dashboard: DEMO_DB=data/demo.db streamlit run dashboard/app.py")
