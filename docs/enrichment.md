# Chicago Moves: weather and community-area enrichment

## Sources

- Community-area names and polygons: City of Chicago dataset **igwz-8jzy**, https://data.cityofchicago.org/resource/igwz-8jzy.json . Live ingestion validates the complete 77-area snapshot; area IDs are canonical strings.
- Historical weather: Open-Meteo archive API, https://archive-api.open-meteo.com/v1/archive . Fixed ERA5 model, one representative Chicago location (41.8781,-87.6298). Hourly temperature °C, precipitation mm, snowfall cm. This is modelled city-level weather, not observations at every pickup. Provider docs: https://open-meteo.com/en/docs/historical-weather-api . Retain source attribution and review provider licensing/usage limits for deployment.

## Bronze → silver → gold

Bronze adds community_areas, weather_hours (unique UTC hour), and enrichment_runs. Source fields are retained as JSONB; refreshes upsert changed payloads. Weather downloads use 30-day chunks and UTC margins around local date boundaries. All end dates are exclusive. Enrichment audit stores completed batch counts and errors.

Silver adds dim_community_area (name and GeoJSON) and dim_weather_hour (unique Chicago-local hour, source-hour count, ambiguity flag). It does not replace the existing warehouse dimensions.

Gold adds mart_enriched_trips (one row per valid trip with both community names and weather coverage), mart_weather_demand (one row per date/hour/weather condition), and mart_area_demand (date/area/activity). The latter has separate Pickup and Dropoff rows; summing both activities counts each trip twice by design, so always select an activity.

## Join correctness

Community IDs join to pickup/dropoff community-area IDs; missing/out-of-range/suppressed values stay Unknown. Raw IDs are retained alongside normalized join IDs. Geographic files contain current polygons, not historical boundary versions. They support area maps and broad flows, not exact travelled road routes.

Weather is keyed by Chicago local date/hour. Incoming UTC timestamps convert with America/Chicago. Two UTC hours can correspond to the same local hour when clocks go back; the dimension collapses them into one flagged hour, clears measurements and labels Ambiguous DST hour. Taxi timestamps cannot resolve the DST fold, so neither weather instance is chosen arbitrarily. No trip is dropped or multiplied. Missing measurements remain NULL and Unavailable, never zero or Dry. Snow means hourly snowfall>0, Wet means precipitation>0 without snowfall, otherwise Dry when all required metrics are present.

Precipitation/snowfall are provider hourly interval quantities; verify provider interval conventions when comparing to trip-start buckets. Use weather measures once per hour in mart_weather_demand. Never sum repeated weather values across individual trips in mart_enriched_trips. Correlations do not establish that weather causes demand changes.

## Run on existing warehouse

```bash
# First run make sample or make run to produce silver.fct_trips.
make enrich-sample  # synthetic fixture, including fictional overlapping polygons
make enrich        # current community snapshot plus published historical weather window
```

For the first year's enrichment, use the same dates as your existing trip backfill (no year-long network job is executed by this PR):

```bash
docker compose run --rm pipeline python -m taxi_pipeline.enrichment --start 2025-01-01 --end 2026-01-01
docker compose run --rm pipeline python scripts/run_enrichment.py
```

ERA5 publishes with approximately five days' delay. The default daily weather window is [Chicago today−19 days, Chicago today−5 days), intentionally leaving a latency margin; it refreshes 14 local days. Taxi ingestion stays T−1. Enable **ENABLE_ENRICHMENT=true** alongside **ENABLE_DAILY_PIPELINE=true** to run it after the base warehouse job. Recent taxi trips remain visible with Unavailable weather until a later catch-up fills them. Weather and taxi repair windows are independent. Failed enrichment stops those workflow steps while base marts already refreshed; base and enrichment publications are separate snapshots. Enrichment uses the same database/JDBC credentials and writer lock as the warehouse, with atomic publication of its five output tables. Full bronze/silver history is re-read; this is not an incremental Spark optimisation.

## Tableau additions

This PR targets main, which currently contains warehouse PR #2. Tableau PR #3 was merged into the warehouse feature branch after #2 merged to main; its files are not copied here. Once that dashboard reaches main, add these sheets using the enriched gold source:

1. **Neighbourhood demand map:** load community_areas.geojson and relate its area_id to mart_area_demand.area_id, selecting Pickup or Dropoff. Colour by SUM(trips); filter by date. Keep Unknown demand in a separate KPI.
2. **Rain/snow comparison:** show average trips per complete hour by weather condition, not merely total trips (the number of observed hours differs). Compare matching hours of day/day types; display the number of available weather hours.
3. **Weather vs travel:** plot temperature_c versus trips or average_trip_seconds at hour grain; include coverage and missing-weather counts.
4. **Community flows:** group mart_enriched_trips by pickup_area_name/dropoff_area_name; show top pairs and date selection, not route geometry.

Export after running the enrichment job:

```bash
python scripts/export_enrichment.py
```

It streams three gold CSVs and writes the small GeoJSON dimension to data/exports, which is gitignored. CircleCI stores only the clearly synthetic CI exports as build artifacts. No Tableau workbook or hosted service is changed in this PR; dashboard rendering remains a Desktop task.

## Testing and limits

Unit checks cover duplicate community IDs, malformed units/array lengths, duplicate UTC hours, end-exclusive boundaries, null measurements, DST conversion and date chunking. Spark checks preserve rows/totals with Unknown areas and ambiguous/unavailable weather. GitHub/CircleCI integration loads enrichments twice, publishes twice, verifies trip counts/totals and dimension uniqueness, and exports sample maps/CSV. CI uses fixtures, not live external API calls. The fixture has three fictional polygons and an intentionally missing temperature; it is not a real neighbourhood/weather sample. Live provider connectivity, complete-year API throughput, map rendering and provider outages require separate operational validation.
