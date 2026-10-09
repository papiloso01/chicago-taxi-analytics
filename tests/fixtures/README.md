# Fixture conventions

Shared committed source fixtures remain under `data/sample/` to preserve CLI/Docker compatibility.
Tests use those fixtures directly. All fixture taxi records and community polygons are synthetic.
Tests must not make network calls to production source APIs.
