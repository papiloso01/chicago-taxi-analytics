"""Full deterministic fixture → bronze → silver → gold → CSV and replay."""
import csv
import importlib.util
import json
from pathlib import Path
from decimal import Decimal
from unittest.mock import patch
import pytest
from chicago_taxi.publishing.lake_pipeline import run

ARGS = ("2024-01-01", "2024-01-04", "2024-01-01", "2024-01-04",
        "data/sample/trips.json", "data/sample/enrichment.json")

def test_pipeline_export_and_replay(tmp_path):
    root = tmp_path / "lake"
    with patch("chicago_taxi.ingestion.lake_batches.pages", side_effect=AssertionError("No live API in CI")):
        first = run(root, *ARGS)
        second = run(root, *ARGS)
    assert first["report"]["quality"] == second["report"]["quality"]
    assert second["report"]["quality"]["accepted_trips"] == 120
    assert second["report"]["quality"]["quarantined_trips"] == 1
    spec = importlib.util.spec_from_file_location("export", "pipelines/export_dashboard.py")
    module = importlib.util.module_from_spec(spec)
    spec.loader.exec_module(module)
    output = module.export(root, tmp_path / "exports")
    with (output / "mart_enriched_trips.csv").open() as stream:
        rows = list(csv.DictReader(stream))
    assert len(rows) == len({r["trip_id"] for r in rows}) == 120
    assert sum(Decimal(r["trip_total"]) for r in rows) == Decimal(second["report"]["quality"]["reported_total_usd"])
    assert len(json.loads((output / "community_areas.geojson").read_text())["features"]) == 3
    # A failed run must leave the last good snapshot current and release its lock.
    with patch("chicago_taxi.publishing.lake_pipeline.extract", side_effect=RuntimeError("injected source failure")):
        with pytest.raises(RuntimeError, match="injected"):
            run(root, *ARGS)
    assert json.loads((root / "current.json").read_text())["run_id"] == second["run_id"]
    assert not (root / "writer.lock").exists()
    audits = [json.loads(p.read_text()) for p in root.glob("snapshots/*/audit.json")]
    assert len([a for a in audits if a["status"] == "failed"]) == 1


def test_aws_publication_and_fresh_runner_bronze_replay(tmp_path):
    import boto3
    from moto import mock_aws
    from chicago_taxi.publishing.athena import AthenaPublisher
    with mock_aws():
        s3 = boto3.client("s3", region_name="us-east-1")
        s3.create_bucket(Bucket="taxi-e2e-lake")
        publisher = AthenaPublisher("taxi-e2e-lake", region="us-east-1", s3=s3,
            glue=boto3.client("glue", region_name="us-east-1"))
        first = run(tmp_path / "runner_one", *ARGS, publisher=publisher)
        second = run(tmp_path / "runner_two", *ARGS, publisher=publisher)
        assert first["report"]["quality"] == second["report"]["quality"]
        assert publisher.current()["run_id"] == second["run_id"]
        tables = publisher.glue.get_tables(DatabaseName="chicago_taxi_gold")["TableList"]
        assert len(tables) == 8
        assert all(table["Parameters"]["contract_version"] == "1" for table in tables)
        assert all(second["run_id"] in table["StorageDescriptor"]["Location"] for table in tables)
