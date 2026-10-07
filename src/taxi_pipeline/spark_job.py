"""Build a complete silver/gold snapshot and publish atomically in PostgreSQL.
The first version reads the complete bronze history; ingestion is incremental,
warehouse transformations are full rebuilds. Spark output is staged before publication.
"""
import os
import uuid
import psycopg
from psycopg import sql
from pyspark.sql import SparkSession, functions as F
from .spark_models import build_models


def run():
    spark=(SparkSession.builder.master(os.getenv("SPARK_MASTER", "local[2]")).appName("ChicagoTaxiWarehouse")
           .config("spark.sql.shuffle.partitions", "8")
           .config("spark.sql.session.timeZone","America/Chicago")
           .config("spark.sql.ansi.enabled","false")
           .config("spark.sql.legacy.timeParserPolicy","CORRECTED").getOrCreate())
    stage="stage_"+uuid.uuid4().hex
    try:
        with psycopg.connect(os.environ["DATABASE_URL"]) as conn:
            if not conn.execute("SELECT pg_try_advisory_lock(773321)").fetchone()[0]:
                raise RuntimeError("Another ingestion or transformation is active")
            conn.execute(sql.SQL("CREATE SCHEMA {}").format(sql.Identifier(stage)))
            conn.commit()
            options={"url":os.environ["JDBC_URL"],"user":os.environ["POSTGRES_USER"],"password":os.environ["POSTGRES_PASSWORD"],"driver":"org.postgresql.Driver"}
            source="(SELECT trip_id,payload::text payload, ((hashtext(trip_id)::bigint+2147483648)%8)::int bucket FROM bronze.trips) src"
            bronze=(spark.read.format("jdbc").options(**options).option("dbtable",source)
                    .option("partitionColumn","bucket").option("lowerBound",0).option("upperBound",8)
                    .option("numPartitions",8).option("fetchsize",5000).load().drop("bucket").cache())
            models=build_models(bronze)
            try:
                count=bronze.count()
                fact=models["silver.fct_trips"].cache()
                if count != fact.count()+models["silver.quarantine_trips"].count():
                    raise ValueError("Bronze/silver reconciliation failed")
                if fact.groupBy("trip_id").count().filter("count != 1").limit(1).count():
                    raise ValueError("Fact grain is not unique")
                if fact.filter("taxi_key IS NULL OR payment_key IS NULL OR company_key IS NULL OR pickup_area_key IS NULL OR dropoff_area_key IS NULL").limit(1).count():
                    raise ValueError("Dimension join left orphaned fact keys")
                for name,df in models.items():
                    target=name.replace(".","_")
                    df.coalesce(2).write.format("jdbc").options(**options).option("dbtable",stage+"."+target).option("batchsize",5000).mode("overwrite").save()
                # All live table refreshes are one transaction. A failed refresh rolls back.
                for name in models:
                    schema,table=name.split(".")
                    conn.execute(sql.SQL("CREATE SCHEMA IF NOT EXISTS {}").format(sql.Identifier(schema)))
                    conn.execute(sql.SQL("CREATE TABLE IF NOT EXISTS {}.{} (LIKE {}.{} INCLUDING ALL)").format(sql.Identifier(schema),sql.Identifier(table),sql.Identifier(stage),sql.Identifier(name.replace(".","_"))))
                    conn.execute(sql.SQL("DELETE FROM {}.{}").format(sql.Identifier(schema),sql.Identifier(table)))
                    conn.execute(sql.SQL("INSERT INTO {}.{} SELECT * FROM {}.{}").format(sql.Identifier(schema),sql.Identifier(table),sql.Identifier(stage),sql.Identifier(name.replace(".","_"))))
                conn.execute(sql.SQL("DROP SCHEMA {} CASCADE").format(sql.Identifier(stage)))
                conn.commit()
            except Exception:
                conn.rollback()
                conn.execute(sql.SQL("DROP SCHEMA IF EXISTS {} CASCADE").format(sql.Identifier(stage)))
                conn.commit()
                raise
            finally:
                bronze.unpersist()
    finally:
        spark.stop()

if __name__=="__main__":
    run()
