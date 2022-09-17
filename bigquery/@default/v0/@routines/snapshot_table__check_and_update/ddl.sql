create or replace procedure `v0.snaphost_table__check_and_update`(
  destination STRUCT<project_id STRING, dataset_id STRING, table_id STRING>,
  update_job_query STRING,
  unique_key STRING,
  options JSON
)
options(description="""Procedure to check partition stalesns and update partitions if needed.

Arguments
====

- destination: The destination table to check and update partitions.
- sources: The source tables of destination table. The procedure will check if the source tables have new partitions.
- update_job_query: The query to update destination table partitions. Its table schema must be same as destination table.
- options: JSON value
    * dry_run: Whether to run the update job as a dry run. [Default: false].
    * tolerate_delay: The delay to tolerate before updating partitions. If newer source partitions are found but its timestamp is within this delay, the procedure will not update partitions. [Default: 30 minutes].
    * via_temp_table: Whether to update partitions via a temporary table. [Default: false].
    * location: BigQuery Location of job. This is used for query analysis to get dependencies. [Default: "region-us"]

Examples
===

```

"""
)
begin
  declare stale_partitions array<string>;
  declare sources array<struct<project_id string, dataset_id string, table_id string>>

  -- Options
  declare _options struct<dry_run BOOL, tolerate_delay INTERVAL, max_update_partition_range INTERVAL, via_temp_table BOOL> default (
    ifnull(bool(options.dry_run), false)
    , ifnull(bool(options.via_temp_table), false)
  );

  -- Assert invalid options
  select logical_and(if(
    key in ('dry_run', 'via_temp_table')
    , true
    , error(format("Invalid Option: name=%t in %t'", key, `options`))
  ))
  from unnest(if(`options` is not null, `bqutil.fn.json_extract_keys`(to_json_string(`options`)), [])) as key
  ;

  call `v0.scan_query_referenced_tables`(sources, update_job_query, to_json(struct(true as enable_query_rewrite)));

  call `v0.detect_staleness`(
    stale_partitions
    , destination
    , sources
    , ['__NULL__', ['__NULL__']]
    , to_json(struct(interval 0 hours as tolerate_delay))
  );

  if ifnull(array_length(stale_partitions), 0) = 0 then
    return;
  end if;

  -- Run Update Job
  if _options.dry_run then
    select
      format('%P', to_json(struct(
        destination
        , sources
        , _options
        , partition_range
    )))
    ;
    return;
  end if;

  execute immediate `v0.zgensql__snapshot_scd_type2`(destination, update_job_query, unique_key);

  if @@row_count = 0 then
    raise using message = format('Update but No data update: %t', (update_job_query, partition_range));
  end if;
end;
