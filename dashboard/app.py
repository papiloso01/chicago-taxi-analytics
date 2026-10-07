import os
import sqlite3
from pathlib import Path
import pandas as pd
import streamlit as st

st.set_page_config(page_title="Chicago Taxi Analytics", layout="wide")
st.title("Chicago Taxi Analytics")

@st.cache_data(ttl=60)
def load(table):
    allowed = {"mart_daily", "mart_payment", "mart_hourly"}
    if table not in allowed:
        raise ValueError("Unknown reporting table")
    demo = os.getenv("DEMO_DB")
    if demo:
        with sqlite3.connect(demo) as conn:
            return pd.read_sql_query(f"SELECT * FROM {table}", conn)
    import psycopg
    with psycopg.connect(os.environ["DATABASE_URL"]) as conn:
        with conn.cursor() as cur:
            cur.execute(f"SELECT * FROM analytics.{table}")
            return pd.DataFrame(cur.fetchall(), columns=[col.name for col in cur.description])

try:
    daily = load("mart_daily")
    payment = load("mart_payment")
    hourly = load("mart_hourly")
except Exception as exc:
    st.error("Reporting tables are unavailable. Run ingestion and dbt build first.")
    st.stop()
if daily.empty:
    st.info("No trips loaded yet.")
    st.stop()
st.caption("Synthetic sample data" if os.getenv("DEMO_DB") else "USD · Chicago local timestamps · Source depends on ingestion run")
daily["trip_date"] = pd.to_datetime(daily["trip_date"]).dt.date
selected = st.sidebar.date_input("Reporting window", value=(daily.trip_date.min(), daily.trip_date.max()))
if len(selected) != 2:
    st.info("Select a start and end date.")
    st.stop()
start, end = selected
daily = daily[(daily.trip_date >= start) & (daily.trip_date <= end)]
for frame in (payment, hourly):
    frame["trip_date"] = pd.to_datetime(frame["trip_date"]).dt.date
payment = payment[(payment.trip_date >= start) & (payment.trip_date <= end)]
hourly = hourly[(hourly.trip_date >= start) & (hourly.trip_date <= end)]
trips = int(daily.trips.sum())
revenue = float(daily.revenue_usd.sum())
a,b,c = st.columns(3)
a.metric("Trips", f"{trips:,}")
b.metric("Reported trip total", f"${revenue:,.2f}")
c.metric("Average trip total", f"${revenue/trips:,.2f}" if trips else "$0.00")
st.subheader("Revenue by day")
st.line_chart(daily.set_index("trip_date")[["revenue_usd"]])
a,b = st.columns(2)
with a:
    st.subheader("Trips by payment type")
    st.bar_chart(payment.groupby("payment_type")["trips"].sum())
with b:
    st.subheader("Demand by hour")
    st.bar_chart(hourly.groupby("trip_hour")["trips"].sum())
st.dataframe(daily, use_container_width=True, hide_index=True)
st.download_button("Download reporting data",daily.to_csv(index=False),"daily_report.csv","text/csv")
