import os,json
import psycopg
with psycopg.connect(os.environ['DATABASE_URL']) as c:
    def scalar(q):return c.execute(q).fetchone()[0]
    assert scalar('select count(*) from gold.mart_enriched_trips')==120
    assert scalar('select count(distinct trip_id) from gold.mart_enriched_trips')==120
    assert scalar('select sum(trip_total) from gold.mart_enriched_trips')==scalar('select sum(trip_total) from silver.fct_trips')
    assert scalar('select sum(trips) from gold.mart_weather_demand')==120
    for role in ['Pickup','Dropoff']:
        assert scalar("select sum(trips) from gold.mart_area_demand where activity='"+role+"'")==120
        assert scalar("select sum(reported_total_usd) from gold.mart_area_demand where activity='"+role+"'")==scalar('select sum(trip_total) from silver.fct_trips')
    assert scalar('select count(*) from bronze.community_areas')==3
    assert scalar('select count(*) from silver.dim_community_area')==3
    assert scalar('select count(*)-count(distinct weather_local_hour) from silver.dim_weather_hour')==0
    assert scalar("select count(*) from gold.mart_enriched_trips where pickup_area_name='Unknown'")>0
    assert scalar('select count(*) from gold.mart_enriched_trips where not weather_available')>0
    assert scalar("select count(*) from bronze.enrichment_runs where status='success'")==2
print('Enrichment replay, count/total reconciliation, missing coverage and unique dimensions passed')
