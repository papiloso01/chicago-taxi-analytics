"""Contract checks need no JVM, cloud account or live API."""
import json
from pathlib import Path
import pytest
from lending_cdc.validation import parse_event

SAMPLE = Path('tests/fixtures/lending_cdc/01_loans_I.json')
SOURCE = 'lending-postgres-dev'


def test_valid_decimal_is_preserved():
    event = json.loads(SAMPLE.read_text())
    event['record_data']['principal_amount'] = '9999999999.99'
    result = parse_event(json.dumps(event), SOURCE)
    assert result[0] is None
    assert json.loads(result[8])['principal_amount'] == '9999999999.99'


@pytest.mark.parametrize('field,value', [
    ('schema_version', 2), ('schema_version', True), ('event_id', 'bad'),
    ('event_id', 2), ('event_sequence', '-1'), ('event_sequence', '9223372036854775808'),
    ('operation', 'X'), ('source_table', 'unknown'), ('source_system', 'other'),
    ('captured_at', '2026-01-01'), ('record_key', {'loan_id': 42}),
])
def test_invalid_envelope_has_reason(field, value):
    event = json.loads(SAMPLE.read_text())
    event[field] = value
    assert parse_event(json.dumps(event), SOURCE)[0]


@pytest.mark.parametrize('amount', ['0.00', '-1.00', '1.001', '10000000000.00', 'NaN'])
def test_invalid_money_is_rejected(amount):
    event = json.loads(SAMPLE.read_text())
    event['record_data']['principal_amount'] = amount
    assert parse_event(json.dumps(event), SOURCE)[0]


@pytest.mark.parametrize('raw', ['{broken', '[]', '{"x":1,"x":2}'])
def test_invalid_json_is_rejected(raw):
    assert parse_event(raw, SOURCE)[0]
