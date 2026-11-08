"""Strict versioned JSON contract. Decimal amounts never pass through float."""
from datetime import datetime, timezone
from decimal import Decimal, InvalidOperation
from functools import lru_cache
from importlib.resources import files
import hashlib
import json
from uuid import UUID


@lru_cache
def contract():
    return json.loads(files('lending_cdc').joinpath('contracts/postgres_v1.json').read_text())


def timestamp(value):
    if not isinstance(value, str):
        raise ValueError('timestamp must be a timezone-aware string')
    result = datetime.fromisoformat(value.replace('Z', '+00:00'))
    if result.tzinfo is None:
        raise ValueError('timestamp must include timezone')
    return result.astimezone(timezone.utc).isoformat()


def bigint(value):
    if isinstance(value, bool) or not isinstance(value, (str, int)):
        raise ValueError('bigint must be an integer')
    number = int(value)
    if str(number) != str(value) or not -(2**63) <= number < 2**63:
        raise ValueError('invalid bigint')
    return number


def no_duplicate_keys(pairs):
    result = {}
    for key, value in pairs:
        if key in result:
            raise ValueError('duplicate JSON field')
        result[key] = value
    return result


def parse_event(raw, expected_source):
    """Return a normalized envelope or one quarantine reason; never discard raw text."""
    try:
        event = json.loads(raw, parse_float=Decimal, object_pairs_hook=no_duplicate_keys)
        if not isinstance(event, dict):
            raise ValueError('envelope must be an object')
        spec = contract()
        if type(event.get('schema_version')) is not int or event['schema_version'] != spec['schema_version']:
            raise ValueError('unsupported schema_version')
        if event.get('capture_method') != spec['capture_method']:
            raise ValueError('unsupported capture_method')
        if event.get('source_system') != expected_source:
            raise ValueError('unexpected source_system')
        if event.get('source_schema') != spec['source_schema']:
            raise ValueError('unsupported source_schema')
        table = event.get('source_table')
        if table not in spec['tables']:
            raise ValueError('unsupported source_table')
        table_spec = spec['tables'][table]
        if not isinstance(event.get('event_id'), str):
            raise ValueError('event_id must be a UUID string')
        event_id = str(UUID(event['event_id']))
        sequence = bigint(event['event_sequence'])
        if sequence <= 0:
            raise ValueError('event_sequence must be positive')
        if event.get('operation') not in ('I', 'U', 'D'):
            raise ValueError('unsupported operation')
        captured_at = timestamp(event['captured_at'])
        data = event['record_data']
        if not isinstance(data, dict) or set(data) != set(table_spec['fields']):
            raise ValueError('record_data fields do not match contract')
        normalized = {}
        for field, kind in table_spec['fields'].items():
            value = data[field]
            if value is None:
                raise ValueError(f'{field} cannot be null')
            if kind == 'bigint':
                value = bigint(value)
            elif kind.startswith('decimal'):
                if isinstance(value, bool) or not isinstance(value, (int, Decimal, str)):
                    raise ValueError(f'{field} must be decimal')
                value = Decimal(value)
                if not value.is_finite() or abs(value) >= Decimal('10000000000') or value != value.quantize(Decimal('.01')):
                    raise ValueError(f'{field} exceeds decimal(12,2)')
                value = format(value, '.2f')
            elif kind == 'timestamp':
                value = timestamp(value)
            elif not isinstance(value, str):
                raise ValueError(f'{field} must be string')
            if field in table_spec.get('positive', []) and Decimal(value) <= 0:
                raise ValueError(f'{field} must be positive')
            if field in table_spec.get('enums', {}) and value not in table_spec['enums'][field]:
                raise ValueError(f'unsupported {field}')
            if field == 'currency' and (len(value) != 3 or not value.isascii() or not value.isalpha() or not value.isupper()):
                raise ValueError('currency must be three uppercase letters')
            normalized[field] = value
        key = table_spec['key']
        record_key = event['record_key']
        if not isinstance(record_key, dict) or set(record_key) != {key} or bigint(record_key[key]) != normalized[key]:
            raise ValueError('record_key does not match record_data')
        payload = json.dumps(normalized, sort_keys=True, separators=(',', ':'))
        canonical = json.dumps([expected_source, event_id, sequence, table, event['operation'], captured_at, payload])
        return (None, event_id, sequence, expected_source, table, normalized[key],
                event['operation'], captured_at, payload, hashlib.sha256(canonical.encode()).hexdigest())
    except (ValueError, TypeError, KeyError, InvalidOperation, OverflowError) as error:
        return (str(error),) + (None,) * 9
