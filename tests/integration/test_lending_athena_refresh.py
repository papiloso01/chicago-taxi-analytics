"""Run the merge SQL locally in DuckDB; Athena API/catalog behavior is separate."""
import json
from pathlib import Path
from uuid import UUID

import duckdb
import pytest

from pipelines.lending.refresh_athena import DB, refresh, statement


class LocalAthena:
    def __init__(self):
        self.db = duckdb.connect()
        self.writes = []
        self.fail_at = None
        self.db.execute(f'CREATE SCHEMA {DB}')
        self.db.execute(f'CREATE TABLE {DB}.raw_outbox (event_json VARCHAR)')
        self.db.execute("CREATE MACRO json_extract_scalar(doc, path) AS json_extract_string(doc, path)")
        for name in ('03_event_ledger.sql', '04_loans_state.sql', '05_loan_transactions_state.sql'):
            self.db.execute(statement(name).split('LOCATION', 1)[0])

    def execute(self, sql):
        self.writes.append(sql)
        if self.fail_at and self.fail_at in sql:
            raise RuntimeError('injected failure')
        self.db.execute(sql)

    def read(self, sql):
        rows = self.db.execute(sql)
        names = [col[0] for col in rows.description]
        return [dict(zip(names, (None if value is None else str(value) for value in row)))
                for row in rows.fetchall()]

    def add(self, value):
        raw = json.dumps(value) if isinstance(value, dict) else value
        self.db.execute(f'INSERT INTO {DB}.raw_outbox VALUES (?)', [raw])


@pytest.fixture
def athena():
    engine = LocalAthena()
    for path in sorted(Path('tests/fixtures/lending_cdc').glob('*.json')):
        engine.add(json.loads(path.read_text(encoding='utf-8')))
    yield engine
    engine.db.close()


def event(athena, sequence):
    return json.loads(athena.read(f'SELECT event_json FROM {DB}.raw_outbox')[sequence - 1]['event_json'])


def test_initial_batch_replay_and_fresh_batch(athena):
    athena.add(event(athena, 3))  # Duplicate delivery within the same raw scan.
    assert refresh(athena)['new_events'] == 8
    assert not athena.writes
    first = refresh(athena, apply=True)
    assert first['actual'] == {
        'ledger_events': 8, 'current_loans': 2, 'current_transactions': 3,
        'transaction_tombstones': 1, 'orphan_transactions': 0,
        'portfolio_loans': 2, 'portfolio_transactions': 3,
    }
    assert refresh(athena, apply=True)['new_events'] == 0
    update = event(athena, 5)
    update.update(event_id=str(UUID(int=9)), event_sequence='9', operation='U')
    update['record_data']['amount'] = '123.45'
    # Unknown envelope metadata is preserved verbatim, including apostrophes.
    update['note'] = "borrower's update"
    athena.add(update)
    next_batch = refresh(athena, apply=True)
    assert next_batch['new_events'] == 1
    assert next_batch['actual']['ledger_events'] == 9
    assert next_batch['actual']['current_transactions'] == 3
    assert next_batch['actual']['transaction_tombstones'] == 1
    assert athena.read(f'SELECT repaid_amount FROM {DB}.loan_portfolio WHERE loan_id = 1001')[0]['repaid_amount'].rstrip('0') == '123.45'
    stored = athena.read(f'SELECT event_json FROM {DB}.event_ledger WHERE event_sequence = 9')
    assert json.loads(stored[0]['event_json'])['note'] == update['note']
    assert refresh(athena, apply=True)['new_events'] == 0


@pytest.mark.parametrize('failure,pattern', [
    ('malformed', 'Invalid raw event'),
    ('identity', 'Conflicting event ID'),
    ('metadata', 'Conflicting event ID'),
    ('sequence', 'Conflicting source sequence'),
    ('orphan', 'orphan transactions'),
])
def test_reject_batch_before_merging(athena, failure, pattern):
    refresh(athena, apply=True)
    athena.writes.clear()
    change = event(athena, 5)
    if failure == 'metadata':
        change['note'] = 'changed envelope metadata'
    else:
        change['record_data']['amount'] = '123.45'
    if failure in ('sequence', 'orphan'):
        change['event_id'] = str(UUID(int=9))
    if failure == 'orphan':
        change['event_sequence'] = '9'
        change['record_data']['loan_id'] = 1234567
    athena.add('{broken' if failure == 'malformed' else change)
    with pytest.raises(ValueError, match=pattern):
        refresh(athena, apply=True)
    assert not athena.writes


def test_retry_repairs_state_after_ledger_commit(athena):
    athena.fail_at = 'MERGE INTO lending_cdc_athena.loan_transactions_state'
    with pytest.raises(RuntimeError, match='injected failure'):
        refresh(athena, apply=True)
    athena.fail_at = None
    recovered = refresh(athena, apply=True)
    assert recovered['new_events'] == 0
    assert recovered['actual']['ledger_events'] == 8
    assert recovered['actual']['current_transactions'] == 3
