"""Real local Iceberg format-v2 tables. S3/Glue access is not implied."""
from decimal import Decimal
import json
from pathlib import Path
import shutil
from uuid import uuid4
from unittest.mock import patch
import pytest
from pyspark.sql import functions as F
from lending_cdc.iceberg import current, run

FIXTURES = Path('tests/fixtures/lending_cdc')


def copy_input(tmp_path):
    target = tmp_path / 'raw'
    shutil.copytree(FIXTURES, target)
    return target


def add(raw, name, event):
    (raw / name).write_text(json.dumps(event, indent=2))


def test_initial_load_updates_deletes_replay_and_join(spark, tmp_path):
    raw = copy_input(tmp_path)
    db = 'test_' + uuid4().hex
    report = run(spark, str(raw), database=db)
    assert report == {'input': 8, 'quarantined': 0, 'valid_unique': 8,
                      'ledger_events': 8, 'current_loans': 2,
                      'current_transactions': 3, 'orphan_transactions': 0}
    namespace = 'local.' + db
    assert current(spark, namespace, 'loans').count() == 2
    tx = current(spark, namespace, 'loan_transactions')
    assert tx.count() == 3
    assert tx.filter('transaction_id = 5999').count() == 0
    tombstone = spark.table(namespace + '.loan_transactions_state').filter('transaction_id = 5999').first()
    assert tombstone.is_deleted and tombstone.event_sequence == 8
    mart = spark.table(namespace + '.loan_portfolio').filter('loan_id = 1001').first()
    assert mart.transaction_count == 2 and mart.repaid_amount == Decimal('100.00')
    assert mart.disbursed_amount == Decimal('1000.00')
    # Duplicate file, an earlier event arriving late, and full replay cannot resurrect a delete.
    shutil.copy(raw / '06_loan_transactions_I.json', raw / 'duplicate.json')
    run(spark, str(raw), database=db)
    assert spark.table(namespace + '.event_ledger').count() == 8
    assert current(spark, namespace, 'loan_transactions').count() == 3
    stale = tmp_path / 'stale'
    stale.mkdir()
    shutil.copy(raw / '06_loan_transactions_I.json', stale / 'stale.json')
    run(spark, str(stale), database=db)
    assert current(spark, namespace, 'loan_transactions').filter('transaction_id = 5999').count() == 0
    # A genuinely newer update is applied, then replayed with no double counting.
    event = json.loads((raw / '07_loan_transactions_U.json').read_text())
    event.update(event_id=str(uuid4()), event_sequence='9')
    event['record_data']['amount'] = '1234567890.12'
    add(raw, 'new_update.json', event)
    run(spark, str(raw), database=db)
    row = current(spark, namespace, 'loan_transactions').filter('transaction_id = 5999').first()
    assert row.amount == Decimal('1234567890.12') and row.event_sequence == 9
    run(spark, str(raw), database=db)
    assert spark.table(namespace + '.event_ledger').count() == 9


def test_quarantine_is_replay_safe_and_conflicting_history_fails(spark, tmp_path):
    raw = copy_input(tmp_path)
    (raw / 'broken.json').write_text('{broken')
    db = 'test_' + uuid4().hex
    assert run(spark, str(raw), database=db)['quarantined'] == 1
    run(spark, str(raw), database=db)
    assert spark.table(f'local.{db}.quarantine').count() == 1
    # Even a historical event ID cannot be rewritten in a later incremental batch.
    conflict = tmp_path / 'conflict'
    conflict.mkdir()
    event = json.loads((raw / '06_loan_transactions_I.json').read_text())
    event['record_data']['amount'] = '999.00'
    add(conflict, 'conflict.json', event)
    with pytest.raises(ValueError, match='Conflicting'):
        run(spark, str(conflict), database=db)
    assert spark.table(f'local.{db}.event_ledger').count() == 8
    # A different event ID at the same per-key sequence is also ambiguous.
    event['event_id'] = str(uuid4())
    add(conflict, 'conflict.json', event)
    with pytest.raises(ValueError, match='Conflicting'):
        run(spark, str(conflict), database=db)


def test_partial_failure_recovers_and_late_unrelated_key_is_not_skipped(spark, tmp_path):
    raw = copy_input(tmp_path)
    db = 'test_' + uuid4().hex
    for path in raw.glob('*.json'):
        event = json.loads(path.read_text())
        event['event_sequence'] = str(int(event['event_sequence']) * 10)
        add(raw, path.name, event)
    from lending_cdc.iceberg import merge_frame
    def fail_state(session, frame, target, keys, newer=None):
        if target.endswith('loans_state'):
            raise RuntimeError('injected failure after ledger commit')
        return merge_frame(session, frame, target, keys, newer)
    with patch('lending_cdc.iceberg.merge_frame', side_effect=fail_state):
        with pytest.raises(RuntimeError, match='injected'):
            run(spark, str(raw), database=db)
    assert spark.table(f'local.{db}.event_ledger').count() == 8
    run(spark, str(raw), database=db)
    late = tmp_path / 'late'
    late.mkdir()
    event = json.loads((raw / '01_loans_I.json').read_text())
    event.update(event_id=str(uuid4()), event_sequence='5')
    event['record_key']['loan_id'] = event['record_data']['loan_id'] = 1003
    add(late, 'late.json', event)
    run(spark, str(late), database=db)
    assert current(spark, f'local.{db}', 'loans').count() == 3
    assert spark.table(f'local.{db}.loan_portfolio').filter(F.col('loan_id') == 1003).first().transaction_count == 0
