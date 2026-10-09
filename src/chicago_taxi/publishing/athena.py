"""Publish immutable Parquet snapshots to S3 and explicit Glue external tables.

Catalog updates are atomic per table, not across tables. A control manifest is
advanced only after all updates complete. Failed updates attempt catalog rollback.
"""
import json
import re
from contextlib import contextmanager
from pathlib import Path
from botocore.exceptions import ClientError


def identifier(value):
    if not re.fullmatch(r"[a-z][a-z0-9_]*", value):
        raise ValueError("Invalid catalog identifier: " + value)
    return value


def table_input(table, location):
    partition = table["partition_by"]
    parameters = {"classification": "parquet", "EXTERNAL": "TRUE",
                  "contract_version": table.get("contract_version", "1"), "grain": table.get("grain", "")}
    if partition:
        parameters.update({"projection.enabled": "true", "projection.trip_date.type": "date",
            "projection.trip_date.range": "2000-01-01,NOW", "projection.trip_date.format": "yyyy-MM-dd",
            "projection.trip_date.interval": "1", "projection.trip_date.interval.unit": "DAYS",
            "storage.location.template": location + "trip_date=${trip_date}/"})
    return {"Name": identifier(table["name"]), "TableType": "EXTERNAL_TABLE",
        "Parameters": parameters, "Description": table.get("description", table["name"]),
        "Owner": table.get("owner", "chicago_taxi"),
        "PartitionKeys": [{"Name": name, "Type": "date"} for name in partition],
        "StorageDescriptor": {"Columns": [{"Name": identifier(c["name"]), "Type": c["type"], "Comment": c.get("description", c["name"])}
            for c in table["columns"] if c["name"] not in partition],
            "Location": location,
            "InputFormat": "org.apache.hadoop.hive.ql.io.parquet.MapredParquetInputFormat",
            "OutputFormat": "org.apache.hadoop.hive.ql.io.parquet.MapredParquetOutputFormat",
            "SerdeInfo": {"SerializationLibrary": "org.apache.hadoop.hive.ql.io.parquet.serde.ParquetHiveSerDe"}}}


class AthenaPublisher:
    def __init__(self, bucket, prefix="chicago_taxi", database_prefix="chicago_taxi", region="eu-west-2", s3=None, glue=None):
        import boto3
        self.bucket = bucket
        self.prefix = prefix.strip("/")
        if not re.fullmatch(r"[a-zA-Z0-9_/-]+", self.prefix) or ".." in self.prefix.split("/"):
            raise ValueError("Invalid S3 prefix")
        self.database_prefix = identifier(database_prefix)
        self.s3 = s3 or boto3.client("s3", region_name=region)
        self.glue = glue or boto3.client("glue", region_name=region)

    @contextmanager
    def writer_lock(self, run_id):
        key = self.prefix + "/control/writer.lock"
        try:
            self.s3.put_object(Bucket=self.bucket, Key=key, Body=run_id.encode(), IfNoneMatch="*")
        except ClientError as error:
            if error.response["Error"]["Code"] in ["PreconditionFailed", "ConditionalRequestConflict", "412", "409"]:
                raise RuntimeError("Another lake writer owns the lock; never remove an active lock") from error
            raise
        try:
            yield
        finally:
            self.s3.delete_object(Bucket=self.bucket, Key=key)

    def current(self):
        try:
            value = self.s3.get_object(Bucket=self.bucket, Key=self.prefix + "/control/current.json")
        except ClientError as error:
            if error.response["Error"]["Code"] in ["NoSuchKey", "404"]:
                return None
            raise
        return json.loads(value["Body"].read())

    def hydrate_bronze(self, manifest, destination):
        destination = Path(destination)
        for table in manifest["tables"]:
            if table["layer"] != "bronze":
                continue
            prefix = table["s3_key"]
            if not prefix.startswith(self.prefix + "/snapshots/"):
                raise ValueError("Unexpected snapshot location")
            for page in self.s3.get_paginator("list_objects_v2").paginate(Bucket=self.bucket, Prefix=prefix):
                for item in page.get("Contents", []):
                    key = item["Key"]
                    relative = key[len(prefix):]
                    if not relative or ".." in Path(relative).parts:
                        raise ValueError("Unsafe snapshot path")
                    path = destination / "bronze" / table["name"] / relative
                    path.parent.mkdir(parents=True, exist_ok=True)
                    self.s3.download_file(self.bucket, key, str(path))

    def upload_tree(self, folder, key):
        for path in sorted(Path(folder).rglob("*")):
            if path.is_file():
                self.s3.upload_file(str(path), self.bucket, key + path.relative_to(folder).as_posix())

    def audit(self, run_id, report):
        self.s3.put_object(Bucket=self.bucket, Key=f"{self.prefix}/audit/{run_id}.json",
                           Body=json.dumps(report).encode(), ContentType="application/json")

    def publish(self, folder, manifest):
        folder = Path(folder)
        run_id = manifest["run_id"]
        identifier(run_id)
        base = f"{self.prefix}/snapshots/{run_id}/"
        self.upload_tree(folder, base)
        previous = []
        try:
            for table in manifest["tables"]:
                database = self.database_prefix + "_" + table["layer"]
                try:
                    self.glue.create_database(DatabaseInput={"Name": database})
                except self.glue.exceptions.AlreadyExistsException:
                    pass
                key = base + table["layer"] + "/" + table["name"] + "/"
                table["s3_key"] = key
                new = table_input(table, f"s3://{self.bucket}/{key}")
                try:
                    old = self.glue.get_table(DatabaseName=database, Name=table["name"])["Table"]
                except self.glue.exceptions.EntityNotFoundException:
                    old = None
                previous.append((database, table["name"], old))
                if old:
                    self.glue.update_table(DatabaseName=database, TableInput=new)
                else:
                    self.glue.create_table(DatabaseName=database, TableInput=new)
            # Readers/orchestration may use this as the completion barrier.
            self.s3.put_object(Bucket=self.bucket, Key=self.prefix + "/control/current.json",
                               Body=json.dumps(manifest).encode(), ContentType="application/json")
        except Exception:
            rollback_errors = []
            for database, name, old in reversed(previous):
                try:
                    if old:
                        allowed = {k: old[k] for k in ["Name", "Description", "Owner", "Retention", "StorageDescriptor", "PartitionKeys", "TableType", "Parameters"] if k in old}
                        self.glue.update_table(DatabaseName=database, TableInput=allowed)
                    else:
                        self.glue.delete_table(DatabaseName=database, Name=name)
                except Exception as error:
                    rollback_errors.append(str(error))
            if rollback_errors:
                raise RuntimeError("Catalog rollback incomplete: " + "; ".join(rollback_errors))
            raise
