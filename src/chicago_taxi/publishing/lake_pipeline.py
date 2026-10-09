"""Native local/S3 lake orchestration; no PostgreSQL dependency."""
import argparse
import json
import os
import uuid
from contextlib import nullcontext
from datetime import date, datetime, timedelta, timezone
from pathlib import Path
from zoneinfo import ZoneInfo

from chicago_taxi.ingestion.lake_batches import extract
from chicago_taxi.ingestion.windows import daily_window, year_window


def run(root, start, end, weather_start, weather_end, sample_trips=None,
        sample_enrichment=None, publisher=None, check_contracts=True):
    from pyspark.sql import SparkSession, functions as F
    from chicago_taxi.transformations.bronze.records import RAW_SCHEMA, consolidate
    from chicago_taxi.transformations.models import build_models
    from chicago_taxi.transformations.gold.enrichment import build_enrichment
    from chicago_taxi.publishing.naming import canonical_models
    from chicago_taxi.quality.lake import validate_models
    from chicago_taxi.quality.contracts import load_contracts, validate_frame
    root = Path(root).resolve()
    root.mkdir(parents=True, exist_ok=True)
    run_id = "run_" + uuid.uuid4().hex
    folder = root / "snapshots" / run_id
    folder.mkdir(parents=True)
    report = dict(run_id=run_id, status="running", start=start, end_exclusive=end,
                  weather_start=weather_start, weather_end_exclusive=weather_end,
                  started_at_utc=datetime.now(timezone.utc).isoformat(), synthetic=bool(sample_trips))
    spark = None
    lock = publisher.writer_lock(run_id) if publisher else nullcontext()
    with lock:
        try:
            # The local lock prevents two writers from racing current.json.
            lock_path = root / "writer.lock"
            with lock_path.open("x") as stream:
                stream.write(run_id)
        except FileExistsError as error:
            raise RuntimeError("Local lake writer already active") from error
        try:
            current_path = root / "current.json"
            previous = publisher.current() if publisher else (json.loads(current_path.read_text()) if current_path.exists() else None)
            previous_folder = None
            if previous:
                previous_folder = root / "previous" / run_id if publisher else root / "snapshots" / previous["run_id"]
                if publisher:
                    publisher.hydrate_bronze(previous, previous_folder)
            counts = extract(folder / "raw", start, end, weather_start, weather_end, run_id,
                             report["started_at_utc"], sample_trips, sample_enrichment,
                             os.getenv("SOCRATA_APP_TOKEN", ""), os.getenv("DATASET_ID", "ajtu-isnz"))
            report["extracted_rows"] = counts
            spark = (SparkSession.builder.master(os.getenv("SPARK_MASTER", "local[2]"))
                     .appName("ChicagoTaxiLake").config("spark.sql.shuffle.partitions", "4")
                     .config("spark.sql.session.timeZone", "America/Chicago")
                     .config("spark.sql.ansi.enabled", "false").getOrCreate())
            bronze = {}
            for name in counts:
                files = sorted((folder / "raw" / name).glob("*.json"))
                incoming = spark.read.schema(RAW_SCHEMA).json([str(p) for p in files]) if files else spark.createDataFrame([], RAW_SCHEMA)
                prior = spark.read.parquet(str(previous_folder / "bronze" / name)) if previous_folder else None
                frame = consolidate(incoming, prior).cache()
                bronze[name] = frame
            # Persist consolidated bronze separately before using it as a transformation source.
            for name, frame in bronze.items():
                frame.write.mode("error").parquet(str(folder / "bronze" / name))
            taxi = bronze["taxi_trip_raw"].select(F.col("record_id").alias("trip_id"), "payload")
            models = build_models(taxi)
            models["silver.fct_trips"].cache()
            models.update(build_enrichment(models["silver.fct_trips"],
                bronze["community_area_raw"].select(F.col("record_id").alias("area_id"), "payload"),
                bronze["weather_hour_raw"].select("payload")))
            models["gold.mart_enriched_trips"].cache()
            report["quality"] = validate_models(taxi, models)
            canonical = canonical_models(models)
            canonical.update({"bronze." + name: frame for name, frame in bronze.items()})
            contracts = load_contracts() if check_contracts else {}
            if check_contracts and set(canonical) != set(contracts):
                raise ValueError("Published table set differs from metadata contracts")
            tables = []
            for name, frame in sorted(canonical.items()):
                if check_contracts:
                    validate_frame(name, frame, contracts[name])
                layer, table = name.split(".")
                partition = ["trip_date"] if "trip_date" in frame.columns and table != "quarantine_taxi_trip" else []
                columns = [dict(name=field.name, type=field.dataType.simpleString()) for field in frame.schema]
                contract = contracts.get(name, {})
                descriptions = {c["name"]: c["description"] for c in contract.get("columns", [])}
                for column in columns:
                    column["description"] = descriptions.get(column["name"], column["name"])
                tables.append(dict(layer=layer, name=table, columns=columns, partition_by=partition,
                    description=contract.get("description", table), grain=contract.get("grain", ""),
                    owner=contract.get("owner", ""), contract_version=str(contract.get("version", 1))))
                if layer != "bronze":
                    writer = frame.write.mode("error").option("compression", "snappy")
                    if partition:
                        writer = writer.partitionBy(*partition)
                    writer.parquet(str(folder / layer / table))
            report.update(status="success", completed_at_utc=datetime.now(timezone.utc).isoformat())
            manifest = dict(run_id=run_id, tables=tables, report=report)
            (folder / "manifest.json").write_text(json.dumps(manifest, indent=2))
            if publisher:
                publisher.publish(folder, manifest)
            temporary = root / (run_id + ".json")
            temporary.write_text(json.dumps(manifest, indent=2))
            temporary.replace(current_path)
            return manifest
        except Exception as error:
            report.update(status="failed", error=str(error)[:1000], completed_at_utc=datetime.now(timezone.utc).isoformat())
            raise
        finally:
            (folder / "audit.json").write_text(json.dumps(report, indent=2))
            try:
                if publisher:
                    publisher.audit(run_id, report)
                    if report["status"] == "failed" and (folder / "raw").exists():
                        publisher.upload_tree(folder / "raw", f"{publisher.prefix}/failed/{run_id}/raw/")
            finally:
                if spark:
                    spark.stop()
                lock_path.unlink()


def main():
    parser = argparse.ArgumentParser(description="Run API → bronze → silver → gold lake tables")
    parser.add_argument("--root", default="data/lake")
    parser.add_argument("--start", default=os.getenv("START_DATE") or None)
    parser.add_argument("--end", default=os.getenv("END_DATE") or None)
    parser.add_argument("--year", type=int)
    parser.add_argument("--weather-start")
    parser.add_argument("--weather-end")
    parser.add_argument("--sample", action="store_true")
    parser.add_argument("--publish", action="store_true", help="Upload snapshot and register Athena/Glue tables")
    args = parser.parse_args()
    if args.year and (args.start or args.end or args.sample):
        parser.error("Use --year OR dates/sample")
    if args.sample:
        start, end = "2024-01-01", "2024-01-04"
    else:
        start, end = year_window(args.year) if args.year else daily_window()
        start, end = args.start or start, args.end or end
    if date.fromisoformat(start) >= date.fromisoformat(end):
        parser.error("Start must precede exclusive end")
    weather_end = (datetime.now(ZoneInfo("America/Chicago")).date() - timedelta(days=5)).isoformat()
    weather_start = (date.fromisoformat(weather_end) - timedelta(days=14)).isoformat()
    if args.year or args.sample or (args.start and args.end):
        weather_start, weather_end = start, end
    weather_start, weather_end = args.weather_start or weather_start, args.weather_end or weather_end
    if date.fromisoformat(weather_start) >= date.fromisoformat(weather_end):
        parser.error("Invalid weather date window")
    publisher = None
    if args.publish:
        from chicago_taxi.publishing.athena import AthenaPublisher
        publisher = AthenaPublisher(os.environ["LAKE_BUCKET"], os.getenv("LAKE_PREFIX", "chicago_taxi"),
                                    os.getenv("GLUE_DATABASE_PREFIX", "chicago_taxi"), os.getenv("AWS_REGION", "eu-west-2"))
    result = run(args.root, start, end, weather_start, weather_end,
                 "data/sample/trips.json" if args.sample else None,
                 "data/sample/enrichment.json" if args.sample else None, publisher)
    print(json.dumps(result["report"], indent=2))

if __name__ == "__main__":
    main()
