# Chicago Taxi Tableau dashboard

This PR adds an editable `chicago-taxi.twb` workbook template and synthetic CSV so the dashboard can be reviewed before configuring a live database. Tableau Desktop opening, layout and filter QA are still required; XML checks do not prove the workbook renders correctly in Tableau. No Tableau site has been connected and nothing is published.

## Open on Windows

1. Download/clone the **feature/tableau-dashboard** branch, keeping the tableau folder structure intact.
2. Open `tableau/chicago-taxi.twb` in Tableau Desktop. If prompted to locate data, select `tableau/sample/trips.csv`.
3. Open the **Chicago Taxi Overview — SYNTHETIC SAMPLE** dashboard tab.
4. If your Desktop version upgrades the older TWB document version, save a new copy in Tableau Desktop. The XML layout follows Tableau workbook structures, but is not Desktop-render-tested here.
5. Verify sample KPIs: **120 trips**, **$2,452** reported totals, average **$20.433333…** before formatting. Sample rows are synthetic, not live Chicago results.
6. Format monetary fields as USD with two decimals and distances with two decimals. Add the date range filter on trip_date and apply it to all worksheets using this source. Add date filtering to context on the longest/shortest trip-ID sheets so their FIXED extrema recompute within the selected period.
7. Taxi rankings are sorted by reported total. If you want Top 10, add a taxi_id Top filter by SUM(trip_total) and make the date filter a context filter first. Keep tied taxis visible where the business definition requires it.

If the generated TWB does not open in your version, use the CSV connection and the worksheet definitions below to recreate/save the workbook in Desktop. Report the exact error so the template can be corrected; do not present structural validation as successful visual QA.

## Included worksheets

| Worksheet | Definition |
|---|---|
| Trip count | COUNT(trip_id) |
| Reported totals USD | SUM(trip_total) |
| Average trip total USD | SUM(trip_total)/COUNT(trip_id) |
| Daily reported totals | trip_date versus SUM(trip_total) |
| Taxi rankings by reported total | taxi_id versus SUM(trip_total), descending |
| Trips by payment method | payment_type versus COUNT(trip_id) |
| Demand by hour | trip_hour versus COUNT(trip_id) |
| Longest trip km | MAX(distance_km) |
| Shortest positive trip km | MIN(IF distance_km > 0 THEN distance_km END) |
| Longest trip IDs | filter Is longest trip=true; show all tied trip IDs |
| Shortest positive trip IDs | filter Is shortest positive trip=true; show all ties |

The shared source is **one row per trip**, not a join between multiple aggregate marts. FIXED calculations use the entire source until a date filter is placed in context. No passenger-gender chart is included because the source lacks that field. Taxi IDs are medallion IDs, not driver identities; totals are gross reported trip amounts rather than net income. Distance is reported miles × 1.609344, not reconstructed route geometry.

## Replace the synthetic data with gold data

Run the warehouse pipeline from PR #2 first; this PR adds `gold.mart_tableau_trips` with all dashboard fields.

**CSV option:** with DATABASE_URL configured, run:

```powershell
python tableau/export_data.py --output tableau/exports/trips.csv
```

The export streams PostgreSQL COPY output rather than collecting the year into Python memory. In Tableau edit the text-file connection to this CSV. Remove the SYNTHETIC SAMPLE labels only after replacing the source and reconciling its totals. Real exports are gitignored; credentials are read from the environment and never embedded in the workbook.

**Live PostgreSQL / extract option:** in Tableau Desktop select Connect → PostgreSQL; supply your actual server, port, database, read-only username and password. Install the PostgreSQL Tableau driver if prompted. Use Required SSL for a hosted SSL database. Select `gold.mart_tableau_trips`, then replace the workbook's CSV data source, mapping fields by identical names. Initially use a Tableau extract and refresh it only after a successful warehouse run.

For local Docker PostgreSQL, explicitly bind its port to your Windows localhost:

```powershell
docker compose -f compose.yml -f tableau/compose.tableau.yml up -d postgres
```

Server is `localhost`, port `5432`, database is the configured POSTGRES_DB. In the container the host is still `postgres`. The port override does not change the default Compose networking. Use `tableau/grants.sql` as a read-only access pattern for a separately provisioned login.

## Refresh and review

Database ingestion and Tableau refresh are separate operations. This PR does not schedule/publish a Tableau extract refresh or claim a production dashboard is live. Review in Desktop, connect your database, choose Tableau Cloud/Server/Public as appropriate, and then configure refresh using that platform's supported connection method. Public sharing exposes the exported data; publish only the intended dataset.

## Build and tests

```powershell
python tableau/build_workbook.py
python tableau/export_data.py --sample data/sample/trips.json
python -m unittest discover -s tests -p test_tableau.py -v
```

CI additionally validates the new gold table's grain, fields, amount reconciliation and sample export. JUnit and warehouse checks continue in CircleCI. Desktop rendering, date context-filter behaviour and database connection UI are manual acceptance checks.

Reference: https://help.tableau.com/current/pro/desktop/en-us/examples_postgresql.htm
