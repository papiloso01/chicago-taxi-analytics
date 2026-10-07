"""Real PostgreSQL ingestion success, replay, failure-audit tests."""
import os
import subprocess
import sys
import tempfile
from pathlib import Path
import psycopg
cmd=[sys.executable,'-m','taxi_pipeline.cli','--start','2024-01-01','--end','2024-01-04']
for _ in range(2):
    subprocess.run(cmd+['--sample','data/sample/trips.json'],check=True)
with tempfile.TemporaryDirectory() as tmp:
    p=Path(tmp)/'invalid.json';p.write_text('[{"fare":"2"}]')
    assert subprocess.run(cmd+['--sample',str(p)]).returncode!=0
with psycopg.connect(os.environ['DATABASE_URL']) as conn:
    assert conn.execute("select count(*) from bronze.trips").fetchone()[0]==121
    assert conn.execute("select count(*) from bronze.pipeline_runs where status='failed' and error is not null").fetchone()[0]==1
print('Success, replay and failure auditing passed')
