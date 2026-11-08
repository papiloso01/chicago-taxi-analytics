"""Bounded batch raw JSON → quarantine, event ledger and Iceberg CDC state.

Single writer only. Commits are atomic per Iceberg table, not across all tables.
A full raw replay repairs an interrupted multi-table run without a watermark.
"""
import re
import os
import sys
from uuid import uuid4

from pyspark.sql import SparkSession, Window, functions as F, types as T

from lending_cdc.validation import contract, parse_event

PARSED = T.StructType([
    T.StructField('error', T.StringType()),
    T.StructField('event_id', T.StringType()),
    T.StructField('event_sequence', T.LongType()),
    T.StructField('source_system', T.StringType()),
    T.StructField('source_table', T.StringType()),
    T.StructField('record_id', T.LongType()),
    T.StructField('operation', T.StringType()),
    T.StructField('captured_at', T.StringType()),
    T.StructField('payload_json', T.StringType()),
    T.StructField('event_hash', T.StringType()),
])


def session(warehouse, catalog='local', mode='local', jars=None):
    identifier(catalog)
    if mode not in ('local', 'glue'):
        raise ValueError('mode must be local or glue')
    if mode == 'glue' and not warehouse.startswith('s3://'):
        raise ValueError('Glue requires an s3:// warehouse')
    if mode == 'local' and '://' in warehouse and not warehouse.startswith('file://'):
        raise ValueError('Local Hadoop catalog requires a local filesystem warehouse')
    builder = (SparkSession.builder.appName('lending-cdc-iceberg')
        .config('spark.sql.extensions', 'org.apache.iceberg.spark.extensions.IcebergSparkSessionExtensions')
        .config(f'spark.sql.catalog.{catalog}', 'org.apache.iceberg.spark.SparkCatalog')
        .config(f'spark.sql.catalog.{catalog}.warehouse', warehouse)
        .config('spark.sql.session.timeZone', 'UTC')
        .config('spark.sql.ansi.enabled', 'true'))
    if mode == 'local':
        os.environ.setdefault('PYSPARK_PYTHON', sys.executable)
        builder = (builder.master('local[2]').config(f'spark.sql.catalog.{catalog}.type', 'hadoop')
                   .config('spark.pyspark.python', sys.executable))
    else:
        builder = (builder.config(f'spark.sql.catalog.{catalog}.catalog-impl', 'org.apache.iceberg.aws.glue.GlueCatalog')
                   .config(f'spark.sql.catalog.{catalog}.io-impl', 'org.apache.iceberg.aws.s3.S3FileIO'))
    if jars:
        builder = builder.config('spark.jars', jars)
    return builder.getOrCreate()


def identifier(value):
    if not re.fullmatch(r'[a-z][a-z0-9_]*', value):
        raise ValueError('Invalid catalog/database identifier')
    return value


def decode(raw, source):
    parser = F.udf(lambda value: parse_event(value, source), PARSED)
    return raw.withColumn('parsed', parser('value')).select('value', 'source_file', 'parsed.*')


def merge_frame(spark, frame, target, keys, newer=None):
    view = 'incoming_' + uuid4().hex
    frame.createOrReplaceTempView(view)
    on = ' AND '.join(f't.{key} = s.{key}' for key in keys)
    update = f'WHEN MATCHED AND {newer} THEN UPDATE SET *' if newer else ''
    try:
        spark.sql(f'MERGE INTO {target} t USING {view} s ON {on} {update} WHEN NOT MATCHED THEN INSERT *')
    finally:
        spark.catalog.dropTempView(view)


def ensure_tables(spark, namespace):
    spark.sql(f'CREATE NAMESPACE IF NOT EXISTS {namespace}')
    spark.sql(f'''CREATE TABLE IF NOT EXISTS {namespace}.event_ledger (
        event_id STRING, event_sequence BIGINT, source_system STRING, source_table STRING,
        record_id BIGINT, operation STRING, captured_at TIMESTAMP, payload_json STRING,
        event_hash STRING
    ) USING iceberg TBLPROPERTIES ('format-version'='2')''')
    spark.sql(f'''CREATE TABLE IF NOT EXISTS {namespace}.quarantine (
        rejection_id STRING, source_file STRING, raw_json STRING, reason STRING
    ) USING iceberg TBLPROPERTIES ('format-version'='2')''')
    for name, spec in contract()['tables'].items():
        fields = ', '.join(f'{field} {kind}' for field, kind in spec['fields'].items())
        spark.sql(f'''CREATE TABLE IF NOT EXISTS {namespace}.{name}_state (
            source_system STRING, {fields}, event_sequence BIGINT, event_id STRING,
            event_hash STRING, captured_at TIMESTAMP, is_deleted BOOLEAN
        ) USING iceberg TBLPROPERTIES ('format-version'='2')''')


def assert_consistent(events):
    for keys in [('source_system', 'event_id'), ('source_system', 'source_table', 'record_id', 'event_sequence')]:
        if events.groupBy(*keys).agg(F.countDistinct('event_hash').alias('variants')).filter('variants > 1').limit(1).count():
            raise ValueError('Conflicting event identity or per-key sequence; no state tables were merged')


def run(spark, input_path, source='lending-postgres-dev', catalog='local', database='lending_cdc'):
    namespace = f'{identifier(catalog)}.{identifier(database)}'
    # One immutable JSON object per file. No partition inference from raw paths.
    raw = spark.read.option('recursiveFileLookup', True).option('pathGlobFilter', '*.json').text(input_path, wholetext=True)
    raw = raw.withColumn('source_file', F.input_file_name())
    parsed = decode(raw, source).cache()
    try:
        counts = parsed.agg(F.count('*').alias('input'), F.sum(F.when(F.col('error').isNotNull(), 1).otherwise(0)).alias('invalid')).first()
        report = {'input': counts['input'], 'quarantined': counts['invalid'] or 0}
        ensure_tables(spark, namespace)
        rejected = parsed.filter('error IS NOT NULL').select(
            F.sha2(F.concat_ws('\n', 'source_file', 'value'), 256).alias('rejection_id'),
            'source_file', F.col('value').alias('raw_json'), F.col('error').alias('reason'))
        if report['quarantined']:
            merge_frame(spark, rejected.dropDuplicates(['rejection_id']), f'{namespace}.quarantine', ['rejection_id'])
        valid = parsed.filter('error IS NULL').select(
            'event_id', 'event_sequence', 'source_system', 'source_table', 'record_id',
            'operation', F.to_timestamp('captured_at').alias('captured_at'), 'payload_json', 'event_hash')
        ledger = spark.table(f'{namespace}.event_ledger')
        # Check the entire retained history, including older events, before any state mutation.
        assert_consistent(valid.unionByName(ledger))
        unique = valid.dropDuplicates(['source_system', 'event_id']).cache()
        try:
            report['valid_unique'] = unique.count()
            if report['valid_unique']:
                merge_frame(spark, unique, f'{namespace}.event_ledger', ['source_system', 'event_id'])
                # Use all ledger history: repairs state after a prior partial multi-table failure.
                history = spark.table(f'{namespace}.event_ledger')
                for name, spec in contract()['tables'].items():
                    window = Window.partitionBy('source_system', 'record_id').orderBy(F.desc('event_sequence'))
                    latest = (history.filter(F.col('source_table') == name)
                              .withColumn('_rank', F.row_number().over(window)).filter('_rank = 1'))
                    schema = ', '.join(f'{k} {v}' for k, v in spec['fields'].items())
                    state = latest.withColumn('data', F.from_json('payload_json', schema)).select(
                        'source_system', 'data.*', 'event_sequence', 'event_id', 'event_hash',
                        'captured_at', (F.col('operation') == 'D').alias('is_deleted'))
                    merge_frame(spark, state, f'{namespace}.{name}_state', ['source_system', spec['key']],
                                's.event_sequence > t.event_sequence')
        finally:
            unique.unpersist()
        mart = portfolio(spark, namespace)
        target = f'{namespace}.loan_portfolio'
        if spark.catalog.tableExists(target):
            mart.writeTo(target).overwrite(F.lit(True))
        else:
            mart.writeTo(target).using('iceberg').tableProperty('format-version', '2').create()
        report['ledger_events'] = spark.table(f'{namespace}.event_ledger').count()
        report['current_loans'] = current(spark, namespace, 'loans').count()
        transactions = current(spark, namespace, 'loan_transactions')
        report['current_transactions'] = transactions.count()
        report['orphan_transactions'] = transactions.join(
            current(spark, namespace, 'loans').select('source_system', 'loan_id'),
            ['source_system', 'loan_id'], 'left_anti').count()
        return report
    finally:
        parsed.unpersist()


def current(spark, namespace, table):
    if table not in contract()['tables']:
        raise ValueError('Unsupported table')
    return spark.table(f'{namespace}.{table}_state').filter('NOT is_deleted')


def portfolio(spark, namespace):
    """One row per source/loan, including loans without transactions. No FX conversion."""
    loans = current(spark, namespace, 'loans').alias('l')
    transactions = current(spark, namespace, 'loan_transactions')
    totals = transactions.groupBy('source_system', 'loan_id').agg(
        F.count('*').alias('transaction_count'),
        F.sum(F.when(F.col('transaction_type') == 'repayment', F.col('amount')).otherwise(F.lit(0))).alias('repaid_amount'),
        F.sum(F.when(F.col('transaction_type') == 'disbursement', F.col('amount')).otherwise(F.lit(0))).alias('disbursed_amount'))
    return loans.join(totals, ['source_system', 'loan_id'], 'left').fillna(
        {'transaction_count': 0, 'repaid_amount': 0, 'disbursed_amount': 0})
