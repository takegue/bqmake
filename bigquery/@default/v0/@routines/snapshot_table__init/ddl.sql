create or replace procedure `v0.snapshot_table__init`(
  in destination struct<project_id string, dataset_id string, table_id string>,
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
  * dry_run: Whether to run the update job as a dry run or not. [Default: false].
  * enable_timetravel_tvf: Wheter to create TVF for timetrval or not [Default: true].
  * enable_timeline_tvf: Wheter to create TVF for timetrval or not [Default: true].
  * enable_entity_monitor: Whether to create monitor view for entity history [Default: true].
  * enable_snapshot_monitor: Whether to create monitor view for snapshot job history or not [Default: true].

"""
)
begin
  declare _stale_partitions array<string>;
  declare _table_ddl, _latest_view_ddl, _tvf_ddl, _timeline_tvf_ddl, _snapshot_history, _entity_stats string;

  -- Options
  declare _options struct<
    dry_run bool
    , enable_timetravel_tvf bool
    , enable_timeline_tvf bool
    , enable_snapshot_monitor bool
    , enable_entity_monitor bool
    > default (
    ifnull(safe.bool(options.dry_run), false)
    , ifnull(safe.bool(options.enable_timetravel_tvf), true)
    , ifnull(safe.bool(options.enable_timeline_tvf), true)
    , ifnull(safe.bool(options.enable_snapshot_monitor), true)
    , ifnull(safe.bool(options.enable_entity_monitor), true)
  );

  -- Assert invalid options
  select logical_and(if(
    key in ('dry_run', "enable_timetravel_tvf", "enable_timeline_tvf", "enable_snapshot_monitor", "enable_entity_monitor")
    , true
    , error(format("Invalid Option: name=%t in %t'", key, `options`))
  ))
  from unnest(if(`options` is not null, `bqutil.fn.json_extract_keys`(to_json_string(`options`)), [])) as key
  ;

  set (_table_ddl, _latest_view_ddl, _tvf_ddl, _timeline_tvf_ddl, _snapshot_history, _entity_stats) = (
    select as struct
      t.create_ddl, t.latest_view_ddl, t.access_tvf_ddl, t.timeline_tvf_ddl, t.profiler__snapshot_job, t.profiler__entity
    from unnest([`v0.zgensql__snapshot_scd_type2`(
      (ifnull(destination.project_id, @@project_id), destination.dataset_id, destination.table_id)
      , update_job.query, update_job.unique_key
    )]) as t
  )
  ;

  -- Run Update Job
  if _options.dry_run then
    select
      format('%P', to_json(struct(
        destination
        , _sources
        , _options
        , [
          _table_ddl
          , _tvf_ddl
          , _latest_view_ddl
        ] as will_executed
    )))
    ;
    return;
  end if;

  execute immediate _table_ddl
    using ifnull(update_job.snapshot_timestamp, current_timestamp()) as timestamp
  ;

  execute immediate _latest_view_ddl
    using ifnull(update_job.snapshot_timestamp, current_timestamp()) as timestamp
  ;

  if _options.enable_timetravel_tvf then
    execute immediate _tvf_ddl
      using ifnull(update_job.snapshot_timestamp, current_timestamp()) as timestamp
    ;
  end if;

  if _options.enable_timeline_tvf then
    execute immediate _timeline_tvf_ddl
      using ifnull(update_job.snapshot_timestamp, current_timestamp()) as timestamp
    ;
  end if;

  if _options.enable_snapshot_monitor then
    execute immediate format(
      """
      create or replace view `%s.%s.%s`
      as %s
      """
        , ifnull(destination.project_id, @@project_id)
        , destination.dataset_id
        , format('monitor__%s__snapshot_job', destination.table_id)
        , _snapshot_history
    );
  end if;

  if _options.enable_entity_monitor then
    execute immediate format(
      """
      create or replace view `%s.%s.%s`
      as %s
      """
        , ifnull(destination.project_id, @@project_id)
        , destination.dataset_id
        , format('monitor__%s__entity', destination.table_id)
        , _entity_stats
    );
  end if;

end;
