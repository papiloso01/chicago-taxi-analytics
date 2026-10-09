# Tests and advisory review

Install `.[spark,lake,test]` with Java 17. Each suite is an independently named CircleCI job and
GitHub Actions matrix entry. JUnit results are uploaded even when tests fail.

```bash
python -m ruff check src pipelines tools tests
python tools/validate_metadata.py
python -m pytest tests/functional
python -m pytest tests/integration
python -m pytest tests/etl_validation
python -m pytest tests/e2e
```

- Functional: retries, pagination, windows, weather normalization, bronze extraction, contracts and review-input boundaries.
- Integration: Moto S3/Glue registration, locks and failed-publication rollback.
- ETL validation: real Spark parsing/quarantine, distance units, star joins, missing areas/weather,
  DST ambiguity and corrected-record replay. Runtime checks also enforce contract keys/types/nullability
  and bronze→silver→gold trip/revenue reconciliation.
- E2E: fixture extraction, bronze/silver/gold Parquet, a second replay, CSV/GeoJSON exports and failed-run recovery.
- PostgreSQL compatibility: separate database-backed regression of old commands, warehouse publication and export.

No mandatory suite calls live source APIs or AWS. Real Athena smoke queries and a full-year
benchmark remain deployment acceptance checks, not mocked-CI claims.

## AI review on every PR

The repository includes two routes; choose one to avoid duplicate feedback:

1. **Workflow reviewer:** after this PR is merged, add repository secret `OPENAI_API_KEY`, set
   `AI_REVIEW_MODEL` to an available Responses API model, and set `ENABLE_AI_REVIEW=true`.
   It triggers for opened/reopened PRs, new pushes and ready-for-review transitions.
   It runs trusted base-branch code, reads patches through GitHub API and includes architecture,
   table/metric/source contracts, base implementation and test inventory. It never checks out or
   executes the PR head. Findings are advisory comments, not pass/fail assertions.
   The first PR cannot execute a base-branch script that does not exist yet; review this mega PR manually.
2. **GitHub Copilot:** if licensed, enable automatic code review and review of new pushes in
   Copilot settings. `.github/copilot-instructions.md` supplies repo-wide context.

AI review requires external configuration and may incur model costs; no key or service is enabled
by the code alone. Oversized input fails rather than silently truncating; missing API patches are
labelled unavailable. Model output is untrusted advice. Keep deterministic suite checks required
and inspect high-impact findings yourself. Configure required job names in branch rules after the
new jobs first appear; no branch rules are changed by this PR.
