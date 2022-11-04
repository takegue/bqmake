bqmake: BigQuery Powered Data Build Tool.
===

`bqmake` provides BigQuery routines that help you to make typical data-modeling.\
All routines are designed to be idempotent and have smart data update mechanism.\
This let free you from awkward DAG workflow management.

This tool gives following utilities.

- **Dynamic whole/partial Data Refresh for BigQuery Table**:\
  Like materialized view, `bqmake.v0.partition_table__update` automatically checks freshness and updates data if needed.\
  This is useful to build pre-computed tables which conists of frequent or expensive query.\
  See [Refreshing Partition Table Data](#refreshing-partition-table-data) section for more details.
- **Data Snapshot Utilities**:\
  Table snapshot captures data changes and stores in Slowly Changing Dimension II format.
- **Update Metadata Utilities**:\
  Metadata utilties make you free to manage complex/irritated table information.
    * Intra-dataset data lineage embedding into dataset
    * Partition table labeling

Currently this is public beta and all routines are subject to change wihtout notice.
Please send us your comments and suggestion via issue!

## Get Started

All utilities are **BigQuery Routines (UDF or PROCEDER)** and published at `bqmake.v0` dataset.\
You can use them without any installation.

### Refreshing Partition Table Data

`bqmake.v0.partition_table__update` makes derived table fresh in specified partition range.
It dynamically analyze partition whose derived table and its referenced tables and update data if needed.

By using [Scheduling Query](https://cloud.google.com/bigquery/docs/scheduling-queries?hl=ja), the procedure is almost behaves like materialized view. 
But comparing materialized view, you can get extra advanteges:
* No restricted query syntax.
* You can get vanilla BigQuery Table that has useful features in BigQuery console such as Preview, BI Engine supports and so on.

```sql
declare query string;

-- Prepare dataset and table
create schema if not exists `zsandbox`;
create or replace table `zsandbox.ga4_count`(event_date date, event_name string, records int64)
partition by event_date;

-- Prepare data generation query parameterized by @begin and @end (DATE type)
set query = """
  select date(timestamp_micros(event_timestamp)) as event_date, event_name, count(1)
  from `bigquery-public-data.ga4_obfuscated_sample_ecommerce.events_*`
  where parse_date('%Y%m%d', _TABLE_SUFFIX) between @begin and @end
  group by event_date, event_name
""";

-- First call procedure to update data
call `bqmake.v0.partition_table__update`(
  (null, 'zsandbox', 'ga4_count')
  , [('bigquery-public-data', 'ga4_obfuscated_sample_ecommerce', 'events_*')]
  , `bqmake.v0.alignment_day2day`('2021-01-01', '2021-01-01')
  , query
  , null
);
--> Affect 16 rows

-- Second call won't update partition data because 2022-01-01 partition is still freshed.
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

`v0.dataset__update_table_labels` set useful labels for partitions tables.

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
