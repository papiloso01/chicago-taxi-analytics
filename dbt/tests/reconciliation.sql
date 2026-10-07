-- Every source ID must appear exactly once as either valid or quarantined.
select (select count(*) from {{ ref('stg_trips') }}) as source_rows
where (select count(*) from {{ ref('stg_trips') }}) !=
 (select count(*) from {{ ref('fct_trips') }}) +
 (select count(*) from {{ ref('quarantine_trips') }})
