bqmake
===

BigQuery Powered Data Build Tool.
The goal is to build derivative table wihtout workflow management system.

This tool supports following features.

- **Dynamic Data Refresh Utilities**:\
    * Like materialized view, `bqmake.v0.partition_table__update` automatically checks and update target table data, taking into account reference tables of query generating target's data.
    * Dynamic staleness check saves BigQuery query processed bytes and slots!
    * Supports partial partition update including complicated alignment i.e. week to day.
- **Data Snapshot Utilities**:\
  Table snapshot enables you to query with historical changes and save your storage capacity.
- **Update Metadata Utilities**:\
  Metadata utilties make you free to manage complex/irritated table information.
    * Intra-dataset data lineage embedding into dataset
    * Partition table labeling

Currently this is public beta and all routines are subject to change wihtout notice.
Please send us your comments and suggestion via issue!

## Get Started

All utilities are **BigQuery Routines (UDF or PROCEDER)** and published at `bqmake.v0` dataset.

### Refreshing Partition Table Data

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

### Metadata Updates

#### Labeling partition tables on Dataset

`v0.dataset__update_table_labels` set useful labels for partitions to tables and views in dataset.

- `partition-min`: Oldest partition_id
- `partition-max`: Latest partition_id
- `partition-skip`: Skipped partition count

```sql
call `v0.dataset__update_table_labels`(('your_project', 'your_dataset'))
```

#### Generating Intra-Dataset Lineage on Dataset

`v0.dataset__update_description` generate dataset description with intra-dataset lineage in [marmaid.js](https://mermaid-js.github.io/mermaid/#/) representation.

```sql
call `v0.dataset__update_description`(('your_project', 'your_dataset'))
```
