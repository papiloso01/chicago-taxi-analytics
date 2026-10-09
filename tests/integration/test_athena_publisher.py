"""S3/Glue adapter contract tests against Moto; no real AWS credentials or costs."""
import json
import tempfile
from pathlib import Path
from unittest.mock import patch
import boto3
import pytest
from moto import mock_aws
from chicago_taxi.publishing.athena import AthenaPublisher, table_input

TABLE = dict(layer="gold", name="mart_test", columns=[dict(name="trips", type="bigint"),
             dict(name="trip_date", type="date")], partition_by=["trip_date"])

@pytest.fixture
def publisher():
    with mock_aws():
        s3 = boto3.client("s3", region_name="us-east-1")
        s3.create_bucket(Bucket="taxi-test-lake")
        yield AthenaPublisher("taxi-test-lake", region="us-east-1", s3=s3,
            glue=boto3.client("glue", region_name="us-east-1"))

def test_projection_excludes_partition_from_payload():
    table = table_input(TABLE, "s3://bucket/snapshot/")
    assert table["PartitionKeys"] == [dict(Name="trip_date", Type="date")]
    assert table["StorageDescriptor"]["Columns"] == [dict(Name="trips", Type="bigint", Comment="trips")]
    assert table["Parameters"]["storage.location.template"].endswith("trip_date=${trip_date}/")

def test_publish_advances_pointer_and_changes_location(publisher, tmp_path):
    file = tmp_path / "gold/mart_test/trip_date=2024-01-01/part.parquet"
    file.parent.mkdir(parents=True)
    file.write_bytes(b"fixture")
    for run_id in ["run_one", "run_two"]:
        manifest = dict(run_id=run_id, tables=[dict(TABLE)])
        with publisher.writer_lock(run_id):
            publisher.publish(tmp_path, manifest)
        assert publisher.current()["run_id"] == run_id
    table = publisher.glue.get_table(DatabaseName="chicago_taxi_gold", Name="mart_test")["Table"]
    assert "/run_two/" in table["StorageDescriptor"]["Location"]

def test_lock_rejects_concurrent_writer_and_releases(publisher):
    with publisher.writer_lock("one"):
        with pytest.raises(RuntimeError, match="Another lake writer"):
            with publisher.writer_lock("two"):
                pass
    with publisher.writer_lock("three"):
        pass

def test_failed_catalog_update_restores_previous_pointer(publisher, tmp_path):
    second = dict(TABLE, name="mart_other")
    publisher.publish(tmp_path, dict(run_id="run_one", tables=[dict(TABLE), second]))
    original = publisher.glue.update_table
    calls = []
    def fail_once(**kwargs):
        calls.append(kwargs)
        if len(calls) == 2:
            raise RuntimeError("injected catalog failure")
        return original(**kwargs)
    with patch.object(publisher.glue, "update_table", side_effect=fail_once):
        with pytest.raises(RuntimeError, match="injected"):
            publisher.publish(tmp_path, dict(run_id="run_two", tables=[dict(TABLE), second]))
    assert publisher.current()["run_id"] == "run_one"
    table = publisher.glue.get_table(DatabaseName="chicago_taxi_gold", Name="mart_test")["Table"]
    assert "/run_one/" in table["StorageDescriptor"]["Location"]


def test_remote_bronze_hydration_uses_current_snapshot(publisher, tmp_path):
    folder = tmp_path / "source"
    file = folder / "bronze/taxi_trip_raw/part.parquet"
    file.parent.mkdir(parents=True)
    file.write_bytes(b"original source snapshot")
    table = dict(layer="bronze", name="taxi_trip_raw", columns=[dict(name="payload", type="string")], partition_by=[])
    publisher.publish(folder, dict(run_id="run_one", tables=[table]))
    publisher.hydrate_bronze(publisher.current(), tmp_path / "restored")
    assert (tmp_path / "restored/bronze/taxi_trip_raw/part.parquet").read_bytes() == file.read_bytes()
