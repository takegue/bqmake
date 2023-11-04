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
  * auto_recreate: if target table schema change is detected, procedure recreate whole table [Default: "error_if_target_not_exists"]

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
  declare _repo_identifier, _table_ddl, _update_dml, _snapshot_query, _repository_query string;
  declare _snapshot_timestamp timestamp;


  -- Options
  declare _options struct<
    dry_run bool
    , tolerate_delay interval
    , force_expired_at timestamp
    , bq_location string 
    , auto_recreate string
  > default (
    ifnull(safe.bool(options.dry_run), false)
    , ifnull(safe_cast(safe.string(options.tolerate_delay) as interval), interval 0 minute)
    , timestamp(safe.string(options.force_expired_at))
    , ifnull(safe.string(options.bq_location), 'region-us')
    , ifnull(string(options.auto_recreate), "error_if_not_exists")
  );

  -- Assert invalid options
  select logical_and(if(
    key in ('dry_run', 'tolerate_delay', 'bq_location', 'force_expired_at', 'auto_recreate')
    , true
    , error(format("Invalid Option: name=%t in %t'", key, `options`))
  ))
  from unnest(if(`options` is not null, `bqutil.fn.json_extract_keys`(to_json_string(`options`)), [])) as key
  ;

  -- Initialize parameters
  set (_repo_identifier, _table_ddl, _update_dml, _snapshot_query, _repository_query, _snapshot_timestamp) = (
    select as struct
      _repo_identifier
      , t.create_ddl
      , t.update_dml
      , t.snapshot_query
      , t.repository_query
      , ifnull(update_job.snapshot_timestamp, current_timestamp())
    from unnest([`v0.zgensql__snapshot_scd_type2`(
      (ifnull(destination.project_id, @@project_id), destination.dataset_id, destination.table_id)
      , update_job.query, update_job.unique_key
    )]) as t
    left join unnest([struct(
      array_reverse(regexp_extract_all(t.repository_query, r'`(.+)`'))[safe_offset(0)] as _repo_identifier
    )])
  )
  ;

  case _options.auto_recreate
    when 'error_if_not_exists' THEN
      execute immediate format("select * from `%s` limit 0", _repo_identifier);
    when 'replace_if_changed' THEN
      begin
        -- Zero scan-amount check of table schema compatibility
        execute immediate format(
          "(\n%s\n) union all (\n%s\n) limit 0", _repository_query, _snapshot_query
        ) using
          _snapshot_timestamp as timestamp
        ;
      exception when error then
        if _options.dry_run then
          select error(format(
            "Table schema change detected: %s\n%s\n%s"
            , _repo_identifier, _repository_query, _snapshot_query
          ));
          return;
        end if;

        begin 
          begin
            execute immediate format(
              "alter table `%s` rename to %s"
              , _repo_identifier
              , format("`%s__%s`"
                , array_reverse(split(_repo_identifier, '.'))[safe_offset(0)]
                , format_timestamp('%Y%m%d%H%M%S', _snapshot_timestamp)
              )
            );
          exception when error then
          end;
          execute immediate _table_ddl 
            using _snapshot_timestamp as timestamp;
          return;
        end;
      end
    ;
    else 
      select error('Invalid option');
  end case;


  -- Automatic source tables detection
  if _sources is null then
    call `v0.analyze_query_referenced_tables`(
      _sources, update_job.query, to_json(struct(options.bq_location as default_region))
    );
  end if;

  -- Check partition staleness
  begin 
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
  exception when error then
    select @@errors.messages;
  end;

  -- Run Update Job
  if _options.dry_run then
    select
      format('Update job will be executed: %P', to_json(struct(
        destination
        , _sources
        , _options
        , _update_dml
    )))
    ;
    return;
  end if;

  execute immediate _update_dml
    using _snapshot_timestamp as timestamp
  ;

  if @@row_count = 0 then
    raise using message = format('Updated but No data: %t', (update_job.query));
  end if;
end;
