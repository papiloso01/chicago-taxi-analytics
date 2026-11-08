"""At-least-once PostgreSQL outbox delivery with idempotent raw S3 objects."""

from dataclasses import dataclass
import argparse
import getpass
import hashlib
import json
from pathlib import Path
import re
from uuid import UUID

from botocore.exceptions import ClientError

# JSON is serialized by PostgreSQL to preserve numeric precision. Sequence values
# are strings in the envelope and are NOT a global commit-order watermark.
PENDING_SQL = """
SELECT event_id, source_schema, source_table,
       jsonb_build_object(
           'schema_version', 1,
           'capture_method', 'transactional_outbox',
           'source_system', %s,
           'event_id', event_id::text,
           'event_sequence', event_sequence::text,
           'source_schema', source_schema,
           'source_table', source_table,
           'operation', operation,
           'record_key', record_key,
           'record_data', record_data,
           'captured_at', to_char(captured_at AT TIME ZONE 'UTC',
                                 'YYYY-MM-DD"T"HH24:MI:SS.US"Z"')
       )::text AS event_json
FROM cdc.outbox_events
WHERE exported_at IS NULL
ORDER BY event_sequence
LIMIT %s
FOR UPDATE SKIP LOCKED
"""
ACK_SQL = """UPDATE cdc.outbox_events SET exported_at = clock_timestamp()
             WHERE event_id = %s AND exported_at IS NULL"""


@dataclass(frozen=True)
class Config:
    pg_host: str
    sslrootcert: str
    bucket: str = "cdc-lakehouse-raw-ul16"
    region: str = "eu-north-1"
    profile: str = "cdc-dev"
    source_system: str = "lending-postgres-dev"
    prefix: str = "postgresql/outbox"
    pg_port: int = 5432
    pg_dbname: str = "transactions"
    pg_user: str = "cdc_exporter"
    batch_size: int = 100
    max_batches: int = 1

    def __post_init__(self):
        if not self.pg_host or not self.sslrootcert:
            raise ValueError("pg_host and sslrootcert are required")
        if not re.fullmatch(r"[a-z0-9][a-z0-9.-]{1,61}[a-z0-9]", self.bucket):
            raise ValueError("Invalid S3 bucket name")
        for segment in [self.source_system, *self.prefix.split("/")]:
            if not re.fullmatch(r"[A-Za-z0-9_-]+", segment):
                raise ValueError("Use nonempty, safe source/prefix path segments")
        for name, value, maximum in [
            ("batch_size", self.batch_size, 500),
            ("max_batches", self.max_batches, 100),
            ("pg_port", self.pg_port, 65535),
        ]:
            if (
                isinstance(value, bool)
                or not isinstance(value, int)
                or not 1 <= value <= maximum
            ):
                raise ValueError(f"Invalid {name}")


def object_key(config, event):
    for name in ["source_schema", "source_table"]:
        if not re.fullmatch(r"[A-Za-z_][A-Za-z0-9_]*", event[name]):
            raise ValueError("Unsafe source identifier")
    event_id = str(UUID(str(event["event_id"])))
    return (
        f"{config.prefix}/source={config.source_system}/"
        f"schema={event['source_schema']}/table={event['source_table']}/"
        f"event_id={event_id}.json"
    )


def put_event(s3, config, event):
    """Create once. A retry must prove existing bytes match before acknowledging."""
    body = event["event_json"].encode("utf-8")
    key = object_key(config, event)
    try:
        s3.put_object(
            Bucket=config.bucket,
            Key=key,
            Body=body,
            ContentType="application/json",
            ServerSideEncryption="AES256",
            IfNoneMatch="*",
            Metadata={"sha256": hashlib.sha256(body).hexdigest()},
        )
        return "uploaded"
    except ClientError as error:
        if error.response.get("ResponseMetadata", {}).get("HTTPStatusCode") != 412:
            raise
        # Never trust existence or metadata alone: mismatched data must stop the batch.
        response = s3.get_object(Bucket=config.bucket, Key=key)
        stream = response["Body"]
        try:
            existing = stream.read(len(body) + 1)
        finally:
            stream.close()
        if existing != body:
            raise ValueError("Existing S3 object differs from outbox event") from None
        return "reused"


def export_batch(connection, s3, config, dry_run=False):
    """Lock pending rows; upload then acknowledge within one bounded DB transaction."""
    from psycopg.rows import dict_row

    result = dict(selected=0, uploaded=0, reused=0, acknowledged=0)
    with connection.transaction():
        with connection.cursor(row_factory=dict_row) as cursor:
            cursor.execute(PENDING_SQL, (config.source_system, config.batch_size))
            events = cursor.fetchall()
            result["selected"] = len(events)
            if dry_run:
                return result
            for event in events:
                outcome = put_event(s3, config, event)
                result[outcome] += 1
                cursor.execute(ACK_SQL, (event["event_id"],))
                if cursor.rowcount != 1:
                    raise RuntimeError(
                        "Acknowledgement did not update exactly one event"
                    )
                result["acknowledged"] += 1
    return result


def run(connection, s3, config, dry_run=False):
    if not dry_run:
        location = s3.get_bucket_location(Bucket=config.bucket)["LocationConstraint"]
        location = "us-east-1" if location is None else location
        location = "eu-west-1" if location == "EU" else location
        if location != config.region:
            raise ValueError("Bucket region does not match configured region")
    total = dict(selected=0, uploaded=0, reused=0, acknowledged=0)
    for _ in range(1 if dry_run else config.max_batches):
        batch = export_batch(connection, s3, config, dry_run)
        for name in total:
            total[name] += batch[name]
        if batch["selected"] < config.batch_size:
            break
    return dict(status="dry_run" if dry_run else "success", **total)


def main():
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--config", required=True, type=Path)
    parser.add_argument("--dry-run", action="store_true")
    args = parser.parse_args()
    config = Config(**json.loads(args.config.read_text(encoding="utf-8-sig")))
    certificate = Path(config.sslrootcert).expanduser()
    if not certificate.is_file():
        parser.error(
            "sslrootcert must point to your downloaded AWS RDS certificate bundle"
        )
    password = getpass.getpass(f"Database password for {config.pg_user}: ")
    import psycopg
    import boto3
    from botocore.config import Config as AwsConfig

    s3 = (
        None
        if args.dry_run
        else boto3.Session(
            profile_name=config.profile, region_name=config.region
        ).client(
            "s3",
            config=AwsConfig(
                retries={"mode": "standard", "total_max_attempts": 5},
                connect_timeout=10,
                read_timeout=30,
            ),
        )
    )
    try:
        with psycopg.connect(
            host=config.pg_host,
            port=config.pg_port,
            dbname=config.pg_dbname,
            user=config.pg_user,
            password=password,
            sslmode="verify-full",
            sslrootcert=str(certificate),
            connect_timeout=10,
            autocommit=True,
            application_name="lending-outbox-exporter",
        ) as connection:
            result = run(connection, s3, config, args.dry_run)
        print(json.dumps(result))
    except Exception as error:
        # Do not print database error details/connection strings or source payloads.
        print(
            json.dumps(
                {
                    "status": "failed",
                    "error_type": type(error).__name__,
                    "message": "Export failed; unacknowledged events can be retried.",
                }
            )
        )
        raise SystemExit(1) from None


if __name__ == "__main__":
    main()
