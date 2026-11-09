CREATE EXTERNAL TABLE IF NOT EXISTS lending_cdc_athena.raw_outbox (event_json STRING)
ROW FORMAT SERDE 'org.apache.hadoop.hive.serde2.RegexSerDe'
WITH SERDEPROPERTIES ('input.regex' = '(.*)')
STORED AS TEXTFILE
LOCATION 's3://cdc-lakehouse-raw-ul16/postgresql/outbox/';
