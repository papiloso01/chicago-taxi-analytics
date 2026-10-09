"""Versioned metadata is the contract for published lake tables."""
import re
from pathlib import Path
import yaml


def load_contracts(root="metadata/tables"):
    contracts = {}
    for path in sorted(Path(root).rglob("*.yml")):
        table = yaml.safe_load(path.read_text())
        name = table["table"]
        if name in contracts or not re.fullmatch(r"(bronze|silver|gold)\.[a-z][a-z0-9_]*", name):
            raise ValueError("Invalid or duplicate table contract: " + name)
        for key in ["description", "grain", "owner", "columns", "primary_key", "upstream", "partition_by", "refresh", "version"]:
            if key not in table:
                raise ValueError(f"Missing {key}: {name}")
        names = [c["name"] for c in table["columns"]]
        if len(names) != len(set(names)) or not table["columns"]:
            raise ValueError("Duplicate or empty columns: " + name)
        for c in table["columns"]:
            if not c.get("description") or not c.get("type") or not isinstance(c.get("nullable"), bool):
                raise ValueError("Invalid column contract: " + name)
        if not set(table["primary_key"] + table["partition_by"]) <= set(names):
            raise ValueError("Invalid contract keys: " + name)
        contracts[name] = table
    if not contracts:
        raise ValueError("No table contracts")
    for name, table in contracts.items():
        for upstream in table["upstream"]:
            if upstream not in contracts and not upstream.startswith("source."):
                raise ValueError("Unknown lineage: " + upstream)
    visiting, done = set(), set()
    def visit(name):
        if name in visiting:
            raise ValueError("Cyclic lineage")
        if name in done or name not in contracts:
            return
        visiting.add(name)
        for upstream in contracts[name]["upstream"]:
            visit(upstream)
        visiting.remove(name)
        done.add(name)
    for name in contracts:
        visit(name)
    return contracts


def validate_frame(name, frame, contract):
    from pyspark.sql import functions as F
    from chicago_taxi.quality.lake import assert_unique
    actual = {field.name: field.dataType.simpleString() for field in frame.schema}
    expected = {field["name"]: field["type"] for field in contract["columns"]}
    if actual != expected:
        raise ValueError(f"Schema drift: {name}: {actual} != {expected}")
    keys = contract["primary_key"]
    if keys:
        assert_unique(frame, keys, name)
    required = [field["name"] for field in contract["columns"] if not field["nullable"]]
    if required:
        counts = frame.agg(*[F.max(F.col(column).isNull().cast("int")).alias(column) for column in required]).first()
        for column in required:
            if counts[column]:
                raise ValueError(f"Null required field: {name}.{column}")


def data_dictionary(contracts, output):
    lines = ["# Data dictionary", "", "Generated from versioned metadata; edit contracts, then regenerate.", ""]
    for name, table in sorted(contracts.items()):
        lines += ["## " + name, "", table["description"], "", "Grain: " + table["grain"], "",
                  "| Column | Type | Nullable | Description |", "|---|---|---|---|"]
        lines += [f"| {c['name']} | {c['type']} | {c['nullable']} | {c['description']} |" for c in table["columns"]]
        lines += [""]
    Path(output).write_text("\n".join(lines))
