import json
from pathlib import Path
from unittest.mock import MagicMock

import boto3
from botocore.exceptions import ClientError
from moto import mock_aws
import pytest

from lending_cdc.exporter import (
    Config,
    ACK_SQL,
    export_batch,
    object_key,
    put_event,
    run,
)


def example():
    body = Path("tests/fixtures/cdc/event.json").read_text()
    data = json.loads(body)
    return {
        **{k: data[k] for k in ["event_id", "source_schema", "source_table"]},
        "event_json": body,
    }


def configuration(**changes):
    return Config(pg_host="example.rds.amazonaws.com", sslrootcert="ca.pem", **changes)


@pytest.fixture
def s3():
    with mock_aws():
        client = boto3.client("s3", region_name="eu-north-1")
        client.create_bucket(
            Bucket="cdc-lakehouse-raw-ul16",
            CreateBucketConfiguration={"LocationConstraint": "eu-north-1"},
        )
        yield client


def test_upload_and_retry_reuse_one_unchanged_object(s3):
    event, config = example(), configuration()
    assert put_event(s3, config, event) == "uploaded"
    assert put_event(s3, config, event) == "reused"
    objects = s3.list_objects_v2(Bucket=config.bucket)["Contents"]
    assert len(objects) == 1
    stored = s3.get_object(Bucket=config.bucket, Key=object_key(config, event))
    assert stored["Body"].read() == event["event_json"].encode()
    assert stored["ServerSideEncryption"] == "AES256"


def test_existing_object_mismatch_is_rejected(s3):
    event, config = example(), configuration()
    s3.put_object(Bucket=config.bucket, Key=object_key(config, event), Body=b"wrong")
    with pytest.raises(ValueError, match="differs"):
        put_event(s3, config, event)


@pytest.mark.parametrize(
    "status,code", [(403, "AccessDenied"), (409, "ConditionalRequestConflict")]
)
def test_failed_upload_is_not_mistaken_for_existing_object(status, code):
    client = MagicMock()
    client.put_object.side_effect = ClientError(
        {"Error": {"Code": code}, "ResponseMetadata": {"HTTPStatusCode": status}},
        "PutObject",
    )
    with pytest.raises(ClientError):
        put_event(client, configuration(), example())
    client.get_object.assert_not_called()


def test_upload_failure_prevents_acknowledgement():
    connection, client = MagicMock(), MagicMock()
    cursor = connection.cursor.return_value.__enter__.return_value
    cursor.fetchall.return_value = [example()]
    client.put_object.side_effect = RuntimeError("failed upload")
    with pytest.raises(RuntimeError):
        export_batch(connection, client, configuration())
    assert len(cursor.execute.call_args_list) == 1
    transaction = connection.transaction.return_value
    assert transaction.__exit__.call_args.args[0] is RuntimeError


def test_dry_run_does_not_write_or_acknowledge():
    connection, client = MagicMock(), MagicMock()
    cursor = connection.cursor.return_value.__enter__.return_value
    cursor.fetchall.return_value = [example()]
    result = run(connection, client, configuration(), dry_run=True)
    assert result == dict(
        status="dry_run", selected=1, uploaded=0, reused=0, acknowledged=0
    )
    assert all(call.args[0] != ACK_SQL for call in cursor.execute.call_args_list)
    assert not client.mock_calls


def test_region_mismatch_fails_before_reading_pending_rows():
    connection, client = MagicMock(), MagicMock()
    client.get_bucket_location.return_value = {"LocationConstraint": "us-east-2"}
    with pytest.raises(ValueError, match="region"):
        run(connection, client, configuration())
    connection.transaction.assert_not_called()


@pytest.mark.parametrize(
    "changes",
    [
        dict(batch_size=0),
        dict(max_batches=101),
        dict(prefix="../escape"),
        dict(source_system="a/b"),
        dict(batch_size=True),
        dict(pg_port=65536),
    ],
)
def test_invalid_configuration_rejected(changes):
    with pytest.raises(ValueError):
        configuration(**changes)
