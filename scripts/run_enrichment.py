"""Build and atomically publish the enrichment datasets after run_warehouse.py."""
import os,uuid
import psycopg
from psycopg import sql
from pyspark.sql import SparkSession
from taxi_pipeline.enrichment_models import build_enrichment
spark=(SparkSession.builder.master(os.getenv('SPARK_MASTER','local[2]')).appName('TaxiEnrichment').config('spark.sql.shuffle.partitions','4').config('spark.sql.session.timeZone','America/Chicago').config('spark.sql.ansi.enabled','false').getOrCreate())
stage='enrich_'+uuid.uuid4().hex
try:
    with psycopg.connect(os.environ['DATABASE_URL']) as conn:
        if not conn.execute('SELECT pg_try_advisory_lock(773321)').fetchone()[0]:raise RuntimeError('Another pipeline job is active')
        options=dict(url=os.environ['JDBC_URL'],user=os.environ['POSTGRES_USER'],password=os.environ['POSTGRES_PASSWORD'],driver='org.postgresql.Driver')
        def read(query):return spark.read.format('jdbc').options(**options).option('dbtable',query).option('fetchsize',5000).load()
        fact=read('silver.fct_trips').cache()
        models=build_enrichment(fact,read('(SELECT area_id,payload::text payload FROM bronze.community_areas) a'),read('(SELECT hour_utc,payload::text payload FROM bronze.weather_hours) w'))
        for name,key in [('silver.dim_community_area','area_id'),('silver.dim_weather_hour','weather_local_hour')]:
            if models[name].groupBy(key).count().filter('count>1').limit(1).count():raise ValueError('Nonunique dimension '+name)
        enriched=models['gold.mart_enriched_trips'].cache()
        if enriched.count()!=fact.count() or enriched.groupBy('trip_id').count().filter('count>1').limit(1).count():raise ValueError('Enrichment changed trip grain')
        if enriched.agg({'trip_total':'sum'}).first()[0]!=fact.agg({'trip_total':'sum'}).first()[0]:raise ValueError('Enrichment changed reported totals')
        conn.execute(sql.SQL('CREATE SCHEMA {}').format(sql.Identifier(stage)));conn.commit()
        try:
            for name,df in models.items():df.coalesce(2).write.format('jdbc').options(**options).option('dbtable',stage+'.'+name.replace('.','_')).mode('overwrite').save()
            for name in models:
                schema,table=name.split('.');temp=name.replace('.','_')
                conn.execute(sql.SQL('CREATE SCHEMA IF NOT EXISTS {}').format(sql.Identifier(schema)))
                conn.execute(sql.SQL('CREATE TABLE IF NOT EXISTS {}.{} (LIKE {}.{} INCLUDING ALL)').format(sql.Identifier(schema),sql.Identifier(table),sql.Identifier(stage),sql.Identifier(temp)))
                conn.execute(sql.SQL('DELETE FROM {}.{}').format(sql.Identifier(schema),sql.Identifier(table)))
                conn.execute(sql.SQL('INSERT INTO {}.{} SELECT * FROM {}.{}').format(sql.Identifier(schema),sql.Identifier(table),sql.Identifier(stage),sql.Identifier(temp)))
            conn.execute(sql.SQL('DROP SCHEMA {} CASCADE').format(sql.Identifier(stage)));conn.commit()
        except Exception:
            conn.rollback();conn.execute(sql.SQL('DROP SCHEMA IF EXISTS {} CASCADE').format(sql.Identifier(stage)));conn.commit();raise
        finally:fact.unpersist();enriched.unpersist()
finally:spark.stop()
