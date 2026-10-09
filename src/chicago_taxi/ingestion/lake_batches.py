"""Stream API batches into immutable local JSON Lines staging files."""
import json
from pathlib import Path
from datetime import date
from chicago_taxi.ingestion.taxi_api import pages
from chicago_taxi.ingestion.community_areas import normalize_areas
from chicago_taxi.ingestion.weather import normalize_weather, weather_batches, get_json

def write_batch(path, rows, run_id, ingested_at, sequence=0):
    count = 0
    with Path(path).open("w", encoding="utf-8") as output:
        for key, payload in rows:
            if not key:
                raise ValueError("Missing source record key")
            output.write(json.dumps(dict(record_id=str(key), payload=json.dumps(payload),
                ingested_at_utc=ingested_at, run_id=run_id, sequence=sequence + count)) + "\n")
            count += 1
    return count

def extract(folder, start, end, weather_start, weather_end, run_id, ingested_at,
            sample_trips=None, sample_enrichment=None, token="", dataset="ajtu-isnz"):
    folder = Path(folder)
    counts = {}
    batches = [json.loads(Path(sample_trips).read_text())] if sample_trips else pages(start, end, dataset, token)
    target = folder / "taxi_trip_raw"
    target.mkdir(parents=True)
    counts["taxi_trip_raw"] = 0
    for index, batch in enumerate(batches):
        count = write_batch(target / f"{index:06d}.json", ((r["trip_id"], r) for r in batch),
                            run_id, ingested_at, counts["taxi_trip_raw"])
        counts["taxi_trip_raw"] += count
    sample = json.loads(Path(sample_enrichment).read_text()) if sample_enrichment else None
    areas = normalize_areas(sample["areas"] if sample else get_json(
        "https://data.cityofchicago.org/resource/igwz-8jzy.json?$limit=1000"))
    if not sample and len(areas) != 77:
        raise ValueError("Expected 77 community areas")
    target = folder / "community_area_raw"
    target.mkdir()
    counts["community_area_raw"] = write_batch(target / "000000.json", areas, run_id, ingested_at)
    start_date, end_date = date.fromisoformat(weather_start), date.fromisoformat(weather_end)
    batches = [normalize_weather(sample["weather"], start_date, end_date)] if sample else weather_batches(start_date, end_date)
    target = folder / "weather_hour_raw"
    target.mkdir()
    counts["weather_hour_raw"] = 0
    for index, batch in enumerate(batches):
        count = write_batch(target / f"{index:06d}.json", ((ts.isoformat(), r) for ts, r in batch),
                            run_id, ingested_at, counts["weather_hour_raw"])
        counts["weather_hour_raw"] += count
    return counts
