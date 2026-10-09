import json
from unittest.mock import patch
import pytest
from chicago_taxi.ingestion.lake_batches import extract, write_batch

def test_batch_preserves_payload_and_sequence(tmp_path):
    path = tmp_path / "batch.json"
    assert write_batch(path, [("a", {"fare": "bad"}), ("b", {"fare": None})], "run_one", "2024-01-01T00:00:00+00:00", 10) == 2
    rows = [json.loads(line) for line in path.read_text().splitlines()]
    assert [r["sequence"] for r in rows] == [10, 11]
    assert json.loads(rows[0]["payload"])["fare"] == "bad"

def test_missing_key_fails(tmp_path):
    with pytest.raises(ValueError, match="key"):
        write_batch(tmp_path / "batch.json", [(None, {})], "run_one", "now")

def test_partial_source_failure_does_not_hide_completed_batch(tmp_path):
    def broken(*args, **kwargs):
        yield [{"trip_id": "a"}]
        raise RuntimeError("source unavailable")
    with patch("chicago_taxi.ingestion.lake_batches.pages", broken):
        with pytest.raises(RuntimeError, match="unavailable"):
            extract(tmp_path, "2024-01-01", "2024-01-02", "2024-01-01", "2024-01-02", "run_one", "now")
    assert (tmp_path / "taxi_trip_raw/000000.json").exists()
