create or replace procedure `v0.snapshot_table__check_and_update`(
  in destination struct<project_id string, dataset_id string, table_id string>,
  in sources array<struct<project_id string, dataset_id string, table_id string>>,
  in update_job struct<
    unique_key string
    , query string
    , snapshot_timestamp timestamp
  >,
  in options json
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
    * force_expire_at: The timestamp to force expire partitions. If the destination's partition timestamp is older than this timestamp, the procedure stale the partitions. [Default: null].
    * location: BigQuery Location of job. This is used for query analysis to get dependencies. [Default: "region-us"]

Examples
===

```

"""
)
begin
  declare _stale_partitions array<string>;
  declare _sources array<struct<project_id string, dataset_id string, table_id string>> default sources;

  -- Options
  declare _options struct<dry_run BOOL, tolerate_delay INTERVAL, force_expire_at timestamp> default (
    ifnull(safe.bool(options.dry_run), false)
    , ifnull(safe_cast(safe.string(options.tolerate_delay) as interval), interval 0 minute)
    , timestamp(safe.string(options.force_expire_at))
  );

  -- Assert invalid options
  select logical_and(if(
    key in ('dry_run', 'tolerate_delay', 'location', 'force_expire_at')
    , true
    , error(format("Invalid Option: name=%t in %t'", key, `options`))
  ))
  from unnest(if(`options` is not null, `bqutil.fn.json_extract_keys`(to_json_string(`options`)), [])) as key
  ;

  if _sources is null then
    -- Auto-detect sources
    call `v0.scan_query_referenced_tables`(
      _sources, update_job_query, to_json(struct(true as enable_query_rewrite))
    );
  end if;


  call `v0.detect_staleness`(
    _stale_partitions
    , destination
    , sources
    , ['__NULL__', ['__ANY__']]
    , to_json(struct(interval 0 hours as tolerate_delay))
  );

  if ifnull(array_length(_stale_partitions), 0) = 0 then
    return;
  end if;

  -- Run Update Job
  if _options.dry_run then
    select
      format('%P', to_json(struct(
        destination
        , _sources
        , _options
        , partition_range
    )))
    ;
    return;
  end if;

  execute immediate `v0.zgensql__snapshot_scd_type2`(
    destination, update_job_query, unique_key
  ).updatge_dml
    using update_job.snapshot_timestamp as timestamp
  ;

  if @@row_count = 0 then
    raise using message = format('Update but No data update: %t', (update_job_query, partition_range));
  end if;
end;
