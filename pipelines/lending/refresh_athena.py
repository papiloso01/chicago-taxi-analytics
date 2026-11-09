"""Refresh the existing lending Iceberg tables through Athena; no Glue ETL job."""
import argparse
from dataclasses import dataclass
import json
from pathlib import Path
import time

from lending_cdc.validation import parse_event

SQL = Path(__file__).resolve().parents[2] / 'sql/lending/athena'
DB = 'lending_cdc_athena'
SOURCE = 'lending-postgres-dev'
MAX_ROWS = 1000
MAX_QUERY_BYTES = 240 * 1024
LEDGER_COLUMNS = ('source_system', 'event_id', 'event_sequence', 'source_table', 'operation')


class Athena:
    def __init__(self, client, workgroup, results):
        self.client = client
        self.workgroup = workgroup
        self.results = results

    def execute(self, sql):
        query_id = self.client.start_query_execution(
            QueryString=sql,
            WorkGroup=self.workgroup,
            QueryExecutionContext={'Catalog': 'AwsDataCatalog'},
            ResultConfiguration={'OutputLocation': self.results},
            ResultReuseConfiguration={'ResultReuseByAgeConfiguration': {'Enabled': False}},
        )['QueryExecutionId']
        deadline = time.monotonic() + 180
        while time.monotonic() < deadline:
            status = self.client.get_query_execution(QueryExecutionId=query_id)['QueryExecution']['Status']
            state = status['State']
            if state == 'SUCCEEDED':
                return query_id
            if state in ('FAILED', 'CANCELLED'):
                raise RuntimeError(f"Athena query {query_id} {state}: {status.get('StateChangeReason', '')}")
            time.sleep(1)
        raise RuntimeError(f'Athena query {query_id} timed out; confirm it has stopped before retrying')

    def read(self, sql):
        query_id = self.execute(sql)
        pages = self.client.get_paginator('get_query_results').paginate(QueryExecutionId=query_id)
        output = []
        for index, page in enumerate(pages):
            result = page['ResultSet']
            names = [column['Name'] for column in result['ResultSetMetadata']['ColumnInfo']]
            rows = result['Rows'][1:] if index == 0 else result['Rows']
            for row in rows:
                values = [value.get('VarCharValue') for value in row['Data']]
                output.append(dict(zip(names, values)))
        return output


@dataclass(frozen=True)
class Event:
    event_id: str
    sequence: int
    source: str
    table: str
    key: int
    operation: str
    payload: dict
    raw: str

    @property
    def columns(self):
        return (self.source, self.event_id, str(self.sequence), self.table, self.operation)


def plan(raw_rows, ledger_rows):
    """Validate the exact rows we read, then return unique new events and expected state."""
    history = {}
    sequences = {}
    additions = []
    for label, rows in [('ledger', ledger_rows), ('raw', raw_rows)]:
        if len(rows) > MAX_ROWS:
            raise ValueError(f'{label} exceeds the {MAX_ROWS}-row lab limit')
        for index, row in enumerate(rows, 1):
            raw = row.get('event_json')
            error, event_id, sequence, source, table, key, op, _, payload, _ = parse_event(raw, SOURCE)
            if error:
                raise ValueError(f'Invalid {label} event at row {index}: {error}')
            event = Event(event_id, sequence, source, table, key, op, json.loads(payload), raw)
            if label == 'ledger' and tuple(row.get(col) for col in LEDGER_COLUMNS) != event.columns:
                raise ValueError(f'Ledger metadata does not match the payload at row {index}')
            identity = (source, event_id)
            previous = history.get(identity)
            if previous:
                if previous.raw != raw:
                    raise ValueError(f'Conflicting event ID: {event_id}')
                if label == 'ledger':
                    raise ValueError(f'Duplicate event ID in ledger: {event_id}')
                continue
            sequence_key = (source, sequence)
            if sequence_key in sequences:
                raise ValueError(f'Conflicting source sequence: {sequence}')
            sequences[sequence_key] = event_id
            history[identity] = event
            if label == 'raw':
                additions.append(event)
    if not history:
        raise ValueError('No outbox events found in raw S3 data or the ledger')
    if len(history) > MAX_ROWS:
        raise ValueError(f'Combined history exceeds the {MAX_ROWS}-event lab limit')
    return additions, history.values()


def expected_counts(history):
    latest = {}
    for event in history:
        key = (event.source, event.table, event.key)
        if key not in latest or event.sequence > latest[key].sequence:
            latest[key] = event
    loans = {(e.source, e.key) for e in latest.values() if e.table == 'loans' and e.operation != 'D'}
    transactions = [e for e in latest.values() if e.table == 'loan_transactions' and e.operation != 'D']
    orphans = sum((e.source, e.payload['loan_id']) not in loans for e in transactions)
    return dict(
        ledger_events=len(history), current_loans=len(loans), current_transactions=len(transactions),
        transaction_tombstones=sum(e.operation == 'D' for e in latest.values() if e.table == 'loan_transactions'),
        orphan_transactions=orphans, portfolio_loans=len(loans), portfolio_transactions=len(transactions) - orphans,
    )


def sql_string(value):
    return "'" + value.replace("'", "''") + "'"


def ledger_merge(events):
    rows = []
    for event in events:
        source, event_id, sequence, table, operation = event.columns
        rows.append('(' + ', '.join([
            sql_string(source), sql_string(event_id), f"BIGINT '{sequence}'",
            sql_string(table), sql_string(operation), sql_string(event.raw),
        ]) + ')')
    sql = statement('06_merge_event_ledger.sql').replace('{validated_rows}', ',\n    '.join(rows))
    if len(sql.encode('utf-8')) > MAX_QUERY_BYTES:
        raise ValueError('Validated events exceed the lab query size limit; no merge was started')
    return sql


def statement(name):
    return (SQL / name).read_text(encoding='utf-8')


def refresh(athena, *, apply=False, bootstrap=False):
    if bootstrap:
        if not apply:
            raise ValueError('--bootstrap requires --apply')
        for name in ('01_database.sql', '02_raw_outbox.sql', '03_event_ledger.sql',
                     '04_loans_state.sql', '05_loan_transactions_state.sql'):
            athena.execute(statement(name))
    raw = athena.read(f'SELECT event_json FROM {DB}.raw_outbox LIMIT {MAX_ROWS + 1}')
    columns = ', '.join(LEDGER_COLUMNS)
    ledger = athena.read(f'SELECT {columns}, event_json FROM {DB}.event_ledger LIMIT {MAX_ROWS + 1}')
    additions, history = plan(raw, ledger)
    expected = expected_counts(history)
    if expected['orphan_transactions']:
        raise ValueError('Latest events contain orphan transactions; wait for or repair the related loan events')
    merge_sql = ledger_merge(additions) if additions else None
    report = {'status': 'validated', 'raw_rows': len(raw), 'new_events': len(additions), 'expected': expected}
    if not apply:
        return report
    if merge_sql:
        athena.execute(merge_sql)
    for name in ('07_merge_loans.sql', '08_merge_transactions.sql', '09_loan_portfolio.sql'):
        athena.execute(statement(name))
    actual = {key: int(value) for key, value in athena.read(statement('10_reconcile.sql'))[0].items()}
    if actual != expected:
        raise RuntimeError(f'Reconciliation failed: expected {expected}; observed {actual}')
    return {**report, 'status': 'refreshed', 'actual': actual}


def main():
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument('--profile', default='cdc-dev')
    parser.add_argument('--region', default='eu-north-1')
    parser.add_argument('--workgroup', default='primary')
    parser.add_argument('--results', default='s3://cdc-lakehouse-curated-ul16/athena-results/')
    parser.add_argument('--apply', action='store_true', help='Merge validated events and rebuild reporting state')
    parser.add_argument('--bootstrap', action='store_true', help='Create missing tables before validation')
    args = parser.parse_args()
    if args.bootstrap and not args.apply:
        parser.error('--bootstrap requires --apply')
    import boto3
    client = boto3.Session(profile_name=args.profile, region_name=args.region).client('athena')
    print(json.dumps(refresh(Athena(client, args.workgroup, args.results),
                             apply=args.apply, bootstrap=args.bootstrap), indent=2))


if __name__ == '__main__':
    main()
