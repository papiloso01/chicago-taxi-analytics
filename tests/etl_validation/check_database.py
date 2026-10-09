"""CI asserts replay does not duplicate trips and invalid data is quarantined."""
import os
import psycopg
with psycopg.connect(os.environ["DATABASE_URL"]) as conn:
    assert conn.execute("select count(*) from bronze.trips").fetchone()[0] == 121
    assert conn.execute("select count(*) from analytics.fct_trips").fetchone()[0] == 120
    assert conn.execute("select count(*) from analytics.quarantine_trips").fetchone()[0] == 1
    assert conn.execute("select count(*) from bronze.pipeline_runs where status='success'").fetchone()[0] == 2
print("Replay and quarantine assertions passed")
