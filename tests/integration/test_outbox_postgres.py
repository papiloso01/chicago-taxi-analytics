"""Real PostgreSQL transactions plus emulated S3; CI uses an isolated cdc_test DB."""

from decimal import Decimal
import json
import os
from pathlib import Path
from unittest.mock import patch

import boto3
from moto import mock_aws
import psycopg
import pytest

from lending_cdc.exporter import Config, export_batch, run

DSN = os.getenv("CDC_TEST_DSN")
pytestmark = pytest.mark.skipif(
    not DSN, reason="Requires isolated CDC_TEST_DSN database named cdc_test"
)


@pytest.fixture
def database():
    with psycopg.connect(DSN, autocommit=True) as connection:
        if connection.info.dbname != "cdc_test":
            pytest.fail(
                "Refusing destructive fixtures outside the isolated cdc_test database"
            )
        connection.execute(
            "DROP SCHEMA IF EXISTS lending CASCADE; DROP SCHEMA IF EXISTS cdc CASCADE"
        )
        for filename in ["001_source.sql", "002_outbox.sql", "003_snapshot.sql"]:
            connection.execute(Path("sql/lending", filename).read_text(), prepare=False)
        yield connection
        connection.execute("DROP SCHEMA lending CASCADE; DROP SCHEMA cdc CASCADE")


@pytest.fixture
def s3():
    with mock_aws():
        client = boto3.client("s3", region_name="eu-north-1")
        client.create_bucket(
            Bucket="cdc-lakehouse-raw-ul16",
            CreateBucketConfiguration={"LocationConstraint": "eu-north-1"},
        )
        yield client


def configuration(**changes):
    return Config(pg_host="unused", sslrootcert="unused", **changes)


def pending(connection):
    return connection.execute(
        "SELECT count(*) FROM cdc.outbox_events WHERE exported_at IS NULL"
    ).fetchone()[0]


def test_snapshot_is_idempotent_and_triggers_roll_back_with_source(database):
    database.execute(Path("sql/lending/003_snapshot.sql").read_text(), prepare=False)
    assert pending(database) == 5
    with psycopg.connect(DSN, autocommit=True) as writer:
        with pytest.raises(RuntimeError):
            with writer.transaction():
                writer.execute(
                    "INSERT INTO lending.loan_transactions VALUES (5998,1001,'repayment',50,now(),now())"
                )
                raise RuntimeError("rollback")
    assert pending(database) == 5
    assert (
        database.execute(
            "SELECT count(*) FROM lending.loan_transactions WHERE transaction_id=5998"
        ).fetchone()[0]
        == 0
    )


def test_insert_update_delete_produces_three_events(database):
    with database.transaction():
        database.execute(
            "INSERT INTO lending.loan_transactions VALUES (5999,1001,'repayment',25,now(),now())"
        )
        database.execute(
            "UPDATE lending.loan_transactions SET amount=30 WHERE transaction_id=5999"
        )
        database.execute(
            "DELETE FROM lending.loan_transactions WHERE transaction_id=5999"
        )
    result = database.execute("""SELECT operation, record_data->>'amount' FROM cdc.outbox_events
        WHERE record_key->>'transaction_id'='5999' ORDER BY event_sequence""").fetchall()
    assert result == [("I", "25.00"), ("U", "30.00"), ("D", "30.00")]


def test_real_export_and_second_run_has_no_new_events(database, s3):
    first = run(database, s3, configuration())
    assert first["uploaded"] == first["acknowledged"] == 5
    assert pending(database) == 0
    second = run(database, s3, configuration())
    assert second["selected"] == second["uploaded"] == 0
    assert len(s3.list_objects_v2(Bucket=configuration().bucket)["Contents"]) == 5


def test_upload_then_db_failure_is_recovered_without_duplicate_objects(database, s3):
    # Simulate failure after an S3 upload but before the DB can commit its acknowledgement.
    from lending_cdc.exporter import put_event as actual_put

    calls = 0

    def fail_second(client, config, event):
        nonlocal calls
        calls += 1
        if calls == 2:
            raise RuntimeError("interrupted batch")
        return actual_put(client, config, event)

    with patch("lending_cdc.exporter.put_event", side_effect=fail_second):
        with pytest.raises(RuntimeError):
            export_batch(database, s3, configuration())
    assert pending(database) == 5
    result = run(database, s3, configuration())
    assert result["reused"] == 1 and result["uploaded"] == 4
    assert pending(database) == 0
    assert len(s3.list_objects_v2(Bucket=configuration().bucket)["Contents"]) == 5


def test_earlier_sequence_committed_later_is_not_skipped(database, s3):
    with psycopg.connect(DSN, autocommit=True) as late_writer:
        with late_writer.transaction():
            late_writer.execute(
                "INSERT INTO lending.loans (loan_id,principal_amount,status) VALUES (1003,30,'active')"
            )
            database.execute(
                "INSERT INTO lending.loans (loan_id,principal_amount,status) VALUES (1004,40,'active')"
            )
            assert run(database, s3, configuration())["acknowledged"] == 6
        assert run(database, s3, configuration())["acknowledged"] == 1
    assert pending(database) == 0
    assert len(s3.list_objects_v2(Bucket=configuration().bucket)["Contents"]) == 7


def test_locked_rows_are_revisited_after_other_worker_finishes(database, s3):
    with psycopg.connect(DSN, autocommit=True) as other_worker:
        with other_worker.transaction():
            other_worker.execute(
                "SELECT event_id FROM cdc.outbox_events ORDER BY event_sequence LIMIT 1 FOR UPDATE"
            )
            assert run(database, s3, configuration())["acknowledged"] == 4
    assert run(database, s3, configuration())["acknowledged"] == 1
    assert pending(database) == 0


def test_raw_json_preserves_decimal_precision(database, s3):
    database.execute(
        "INSERT INTO lending.loan_transactions (transaction_id,loan_id,transaction_type,amount) VALUES (6000,1001,'repayment',9007199254.99)"
    )
    run(database, s3, configuration())
    payloads = [
        json.loads(
            s3.get_object(Bucket=configuration().bucket, Key=obj["Key"])["Body"].read(),
            parse_float=Decimal,
        )
        for obj in s3.list_objects_v2(Bucket=configuration().bucket)["Contents"]
    ]
    target = next(
        row for row in payloads if row["record_key"].get("transaction_id") == 6000
    )
    assert target["record_data"]["amount"] == Decimal("9007199254.99")


def test_bounded_batches_leave_remaining_events_pending(database, s3):
    assert run(database, s3, configuration(batch_size=2))["acknowledged"] == 2
    assert pending(database) == 3


def test_restricted_exporter_role_can_ack_but_not_rewrite_events(database, s3):
    permissions = Path("sql/lending/004_exporter_permissions.sql").read_text()
    permissions = permissions.replace("DATABASE transactions", "DATABASE cdc_test")
    database.execute(permissions, prepare=False)
    with database.transaction():
        database.execute("SET LOCAL ROLE cdc_exporter")
        assert run(database, s3, configuration())["acknowledged"] == 5
        with pytest.raises(psycopg.errors.InsufficientPrivilege):
            with database.transaction():
                database.execute("UPDATE cdc.outbox_events SET operation='D'")
