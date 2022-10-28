# bqmake

BigQuery Powered Data Build Tool like `make`.  
`bqmake` provides some utilities for maintaining up-to-date tables.


All utilties are provided by **BigQuery Routines (UDF or PROCEDER)**.
You can use these routines out-of-the-box!

This tool supports following features.
- Supported **Partition Table** Data Update
    * Routine to update partial partition with staleness check.
        * Supports complicated partition alignment i.e. week to day.
        * Dynamic staleness check saves BigQuery query processed bytes and slots!
    * You can keep tables fresh even though they have various and complexed source tables.
- Supported **Table Snapshot**
    * Table snapshot enable you to query with historical changes and saves your storage capacity.
- Supported metadata/table profiling utilities for data management
    * BigQuery Labeling tools for partition tables
    * SQL Generator for descriptive statistics

Currently this is public beta and all routines are subject to change wihtout notice.
Please send us your comments and suggestion via issue!

## Get Started

### Partition Table

```sql
declare query string;

-- Prepare dataset and tables
create schema if not exists `zsandbox`;
create or replace table `zsandbox.ga4_count`(event_date date, event_name string, records int64)
partition by event_date;

set query = """
  select date(timestamp_micros(event_timestamp)) as event_date, event_name, count(1)
  from `bigquery-public-data.ga4_obfuscated_sample_ecommerce.events_*`
  where parse_date('%Y%m%d', _TABLE_SUFFIX) between @begin and @end
  group by event_date, event_name
""";

call `bqmake.v0.partition_table__update`(
  (null, 'zsandbox', 'ga4_count')
  , [('bigquery-public-data', 'ga4_obfuscated_sample_ecommerce', 'events_*')]
  , `bqmake.v0.alignment_day2day`('2021-01-01', '2021-01-01')
  , query
  , null
);
--> Affect 16 rows

-- If you re-call this routine, this avoid to update already updated partitions.
call `bqmake.v0.partition_table__update`(
  (null, 'zsandbox', 'ga4_count')
  , [('bigquery-public-data', 'ga4_obfuscated_sample_ecommerce', 'events_*')]
  , `bqmake.v0.alignment_day2day`('2021-01-01', '2021-01-01')
  , query
  , null
);
--> No affect
```

### Snapshot Table

```sql
declare query string;
set query = "select * from `bigquery-public-data.austin_bikeshare.bikeshare_stations` limit 0"

-- Initialize Snapshot table
call `bqmake.v0.snapshot_table__init`(
  (null, 'zsandbox', 'ga4_count')
  , (
    'station_id'
    , query
    , current_timestamp()
  )
  , null
);

-- Snapshot after some modification
call `bqmake.v0.snapshot_table__update`(
  destination
  , null
  , (
    'station_id'
    -- This example changes some records on purpose
    , 'select * replace(if(station_id in (2499), "closed", status) as status) from `bigquery-public-data.austin_bikeshare.bikeshare_stations`'
    , current_timestamp()
    )
  )
 , to_json(struct(
    -- For demo, example disable table staleness check.
    current_timestamp() as force_expire_at
 ))
)
```

### Metadata for partition tables

### Labeling partition tables on Dataset

`v0.dataset__update_table_labels` set useful labels for partitions to tables and views in dataset.

- `partition-min`: Oldest partition_id
- `partition-max`: Latest partition_id
- `partition-skip`: Skipped partition count

```sql
call `v0.dataset__update_table_labels`(('your_project', 'your_dataset'))
```
