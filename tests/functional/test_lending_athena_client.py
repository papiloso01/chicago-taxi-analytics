from pipelines.lending.refresh_athena import Athena
import pytest


class Client:
    def __init__(self, state='SUCCEEDED'):
        self.state = state

    def start_query_execution(self, **params):
        assert params['WorkGroup'] == 'primary'
        assert not params['ResultReuseConfiguration']['ResultReuseByAgeConfiguration']['Enabled']
        return {'QueryExecutionId': 'query-1'}

    def get_query_execution(self, **params):
        assert params['QueryExecutionId'] == 'query-1'
        return {'QueryExecution': {'Status': {'State': self.state, 'StateChangeReason': 'denied'}}}

    def get_paginator(self, operation):
        assert operation == 'get_query_results'
        return self

    def paginate(self, **params):
        for values in [['event_json', 'first'], ['second', None]]:
            yield {'ResultSet': {
                'ResultSetMetadata': {'ColumnInfo': [{'Name': 'event_json'}]},
                'Rows': [{'Data': [{'VarCharValue': value} if value is not None else {}]}
                         for value in values],
            }}


def test_result_pagination_drops_only_first_header():
    api = Athena(Client(), 'primary', 's3://results/')
    assert api.read('SELECT event_json FROM raw') == [
        {'event_json': 'first'}, {'event_json': 'second'}, {'event_json': None},
    ]


def test_failed_query_propagates_query_id():
    api = Athena(Client('FAILED'), 'primary', 's3://results/')
    with pytest.raises(RuntimeError, match='query-1 FAILED: denied'):
        api.execute('SELECT 1')
