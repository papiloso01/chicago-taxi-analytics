from pathlib import Path
import yaml
import pytest
from chicago_taxi.quality.contracts import load_contracts

def test_repository_contract_lineage():
    tables = load_contracts()
    assert "silver.fact_taxi_trip" in tables
    assert "gold.mart_taxi_trip_enriched" in tables
    assert len([t for t in tables if t.startswith("bronze.")]) == 3

def test_unknown_lineage_is_rejected(tmp_path):
    source = next(Path("metadata/tables").rglob("*.yml"))
    table = yaml.safe_load(source.read_text())
    table["upstream"] = ["silver.missing"]
    (tmp_path / "bad.yml").write_text(yaml.safe_dump(table))
    with pytest.raises(ValueError, match="Unknown lineage"):
        load_contracts(tmp_path)
