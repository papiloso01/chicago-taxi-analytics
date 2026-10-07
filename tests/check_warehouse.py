"""Database integration assertions after repeated loads and Spark publication."""
import os
import psycopg
with psycopg.connect(os.environ["DATABASE_URL"]) as conn:
    def scalar(q):return conn.execute(q).fetchone()[0]
    assert scalar("select count(*) from bronze.trips")==121
    assert scalar("select count(*) from silver.fct_trips")==120
    assert scalar("select count(*) from silver.quarantine_trips")==1
    assert scalar("select count(*) from silver.fct_trips")+scalar("select count(*) from silver.quarantine_trips")==scalar("select count(*) from bronze.trips")
    assert scalar("select count(distinct trip_id) from silver.fct_trips")==120
    assert scalar("select sum(trips) from gold.mart_daily")==120
    for mart in ["mart_hourly", "mart_payment"]:
        assert scalar(f"select sum(trips) from gold.{mart}")==120
        assert scalar(f"select sum(revenue_usd) from gold.{mart}")==scalar("select sum(trip_total) from silver.fct_trips")
    assert scalar("select sum(revenue_usd) from gold.mart_daily")==scalar("select sum(trip_total) from silver.fct_trips")
    assert scalar("select sum(reported_total_usd) from gold.mart_taxi_earnings")==scalar("select sum(trip_total) from silver.fct_trips")
    assert scalar("select count(*) from gold.mart_trip_distances")==120
    assert scalar("select count(*) from gold.mart_tableau_trips")==120
    assert scalar("select count(distinct trip_id) from gold.mart_tableau_trips")==120
    assert scalar("select sum(trip_total) from gold.mart_tableau_trips")==scalar("select sum(revenue_usd) from gold.mart_daily")
    assert scalar("select count(*) from silver.fct_trips where abs(distance_km-trip_miles*1.609344)>0.000001")==0
    for name,natural in [("taxi","taxi_id"),("payment","payment_type"),("company","company"),("area","area")]:
        assert scalar(f"select count(*)-count(distinct {name}_key) from silver.dim_{name}")==0
    for dim,key in [("taxi","taxi_key"),("payment","payment_key"),("company","company_key"),("area","pickup_area_key"),("area","dropoff_area_key")]:
        assert scalar(f"select count(*) from silver.fct_trips f left join silver.dim_{dim} d on f.{key}=d.{dim}_key where d.{dim}_key is null")==0
    assert scalar("select count(*) from silver.fct_trips f left join silver.dim_date d using(trip_date) where d.trip_date is null")==0
    assert scalar("select count(*) from bronze.pipeline_runs where status='success'")==2
print("Bronze/silver/gold grain, reconciliation, dimensions and conversion passed")
