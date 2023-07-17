Procedure to check partition stalesns and update partitions if needed.

Arguments
====

- destination: The destination table to check and update partitions.
- sources: The source tables of destination table. The procedure will check if the source tables have new partitions.
- partition_alignments: Partition alignment rules. The procedure will check destination staleness correspoinding to each alignment rule.
- query: The query to update destination table partitions. Its table schema must be same as destination table.
- options: JSON value
    * dry_run: Whether to run the update job as a dry run. [Default: false].
    * tolerate_delay: The delay to tolerate before updating partitions. If newer source partitions are found but its timestamp is within this delay, the procedure will not update partitions. [Default: 0 minutes].
    * max_update_partition_range: The interval to limit the range of partitions to update. This option is useful to avoid updating too many partitions at once. [Default: 1 month].
    * via_temp_table: Whether to update partitions via a temporary table. [Default: false].
    * force_expired_at: The timestamp to force expire partitions. If the destination's partition timestamp is older than this timestamp, the procedure stale the partitions. [Default: null].
    * bq_location: BigQuery Location of job. This is used for query analysis to get dependencies. [Default: "region-us"]
    * backfill_direction: The direction to backfill partitions. [Default: "backward"]
    * auto_recreate: if target table schema change is detected, procedure recreate whole table [Default: "error_if_target_not_exists"]

Examples
===

- Check and update partitions of `my_project.my_dataset.my_table` table.

```
begin
  declare query string;
  declare _sources array<struct<project_id string, dataset_id string, table_id string>> default sources;

  set query = """
    select date(timestamp_micros(event_timestamp)) as event_date, event_name, count(1)
    from `bigquery-public-data.ga4_obfuscated_sample_ecommerce.events_*`
    where parse_date('%Y%m%d', _TABLE_SUFFIX) between @begin and @end
    group by event_date, event_name
  """;

  create schema if not exists `zsandbox`;
  create or replace table `zsandbox.ga4_count`(event_date date, event_name string, records int64)
  partition by event_date;
  call `bqmake.v0.partition_table__check_and_update`(
    (null, 'zsandbox', 'ga4_count')
    , [('bigquery-public-data', 'ga4_obfuscated_sample_ecommerce', 'events_*')]
    , `bqmake.v0.alignment_day2day`('2021-01-01', '2021-01-01')
    , query
    , null
  );
end
```
