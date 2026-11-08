"""Dedicated suite must execute real Iceberg, never silently skip it."""
import os
from pathlib import Path
import pytest
from lending_cdc.iceberg import session


@pytest.fixture(scope='session')
def spark(tmp_path_factory):
    jar = os.environ.get('ICEBERG_RUNTIME_JAR')
    if not jar or not Path(jar).is_file():
        pytest.fail('Set ICEBERG_RUNTIME_JAR; use docker/lending-cdc/Dockerfile or scripts/fetch_iceberg_runtime.py')
    spark = session(str(tmp_path_factory.mktemp('warehouse')), jars=jar)
    spark.conf.set('spark.sql.shuffle.partitions', '2')
    spark.sparkContext.setLogLevel('ERROR')
    yield spark
    spark.stop()
