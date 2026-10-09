"""Explicit gold schemas are versioned alongside table metadata."""
from chicago_taxi.quality.contracts import load_contracts

def schemas():
    from pyspark.sql import types as T
    return {name: T.StructType([T.StructField(column['name'], T._parse_datatype_string(column['type']), column['nullable'])
            for column in contract['columns']]) for name, contract in load_contracts().items() if name.startswith('gold.')}
