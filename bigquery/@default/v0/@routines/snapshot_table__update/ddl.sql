create or replace procedure `v0.snapshot_table__update`(
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
-     sources: The source tables of destination table referecend by update_job.query. The procedure will check if the source tables have new partitions.
               If null is given, the procedure will automatically detect the source tables from update_job.query.
-  update_job:
  * unique_key: The unique key of the update job. The procedure will check if the update job is already running.
  * query: The query to update the destination table.
  * snapshot_timestamp: The timestamp to use for the snapshot. If null is given, the procedure will use the current timestamp.
- options: JSON value
  * dry_run: Whether to run the update job as a dry run. [Default: false].
  * tolerate_delay: The delay to tolerate before updating partitions. If newer source partitions are found but its timestamp is within this delay, the procedure will not update partitions. [Default: 30 minutes].
  * force_expired_at: The timestamp to force expire partitions. If the destination's partition timestamp is older than this timestamp, the procedure stale the partitions. [Default: null].
  * bq_location: BigQuery Location of job. This is used for query reference analysis. [Default: "region-us"]

Examples
===

```
call `bqmake.v0.snapshot_table__check_and_update`(
  destination
  , null
  (
    "staion_id"
    , "select * from `bigquery-public-data.austin_bikeshare.bikeshare_stations` limit 0"
    , current_timestamp()
  )
  , to_json(struct(
    current_timestamp() as force_expired_at
  ))
)
```

"""
)
begin
  declare _stale_partitions array<string>;
  declare _sources array<struct<project_id string, dataset_id string, table_id string>> default sources;

  -- Options
  declare _options struct<dry_run bool, tolerate_delay interval, force_expired_at timestamp, bq_location string> default (
    ifnull(safe.bool(options.dry_run), false)
    , ifnull(safe_cast(safe.string(options.tolerate_delay) as interval), interval 0 minute)
    , timestamp(safe.string(options.force_expired_at))
    , ifnull(safe.string(options.bq_location), 'region-us')
  );

  -- Assert invalid options
  select logical_and(if(
    key in ('dry_run', 'tolerate_delay', 'bq_location', 'force_expired_at')
    , true
    , error(format("Invalid Option: name=%t in %t'", key, `options`))
  ))
  from unnest(if(`options` is not null, `bqutil.fn.json_extract_keys`(to_json_string(`options`)), [])) as key
  ;

  -- Automatic source tables detection
  if _sources is null then
    call `v0.analyze_query_referenced_tables`(
      _sources, update_job.query, to_json(struct(options.bq_location as default_region))
    );
  end if;

  -- Check partition staleness
  call `v0.detect_staleness`(
    _stale_partitions
    , destination
    , _sources
    , [('__NULL__', ['__ANY__'])]
    , to_json(struct(
      _options.tolerate_delay as tolerate_delay
      , _options.force_expired_at as force_expired_at
    ))
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
        , update_job
    )))
    ;
    return;
  end if;

  execute immediate `v0.zgensql__snapshot_scd_type2`(
    destination, update_job.query, update_job.unique_key
  ).update_dml
    using ifnull(update_job.snapshot_timestamp, current_timestamp()) as timestamp
  ;

  if @@row_count = 0 then
    raise using message = format('Updated but No data: %t', (update_job.query));
  end if;
end;
