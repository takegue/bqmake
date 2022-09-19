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
  * dry_run: Whether to run the update job as a dry run. [Default: false].

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
    current_timestamp() as force_expire_at
  ))
)
```

"""
)
begin
  declare _stale_partitions array<string>;
  declare _table_ddl string;
  declare _tvf_ddl string;

  -- Options
  declare _options struct<dry_run bool> default struct(
    ifnull(safe.bool(options.dry_run), false)
  );

  -- Assert invalid options
  select logical_and(if(
    key in ('dry_run')
    , true
    , error(format("Invalid Option: name=%t in %t'", key, `options`))
  ))
  from unnest(if(`options` is not null, `bqutil.fn.json_extract_keys`(to_json_string(`options`)), [])) as key
  ;

  set (_table_ddl, _tvf_ddl) = (
    select as struct
      t.create_ddl, t.access_tvf_ddl
    from unnest([`v0.zgensql__snapshot_scd_type2`(
      destination, update_job.query, update_job.unique_key
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
        ] as will_executed
    )))
    ;
    return;
  end if;

  execute immediate _table_ddl
    using ifnull(update_job.snapshot_timestamp, current_timestamp()) as timestamp
  ;

  execute immediate _tvf_ddl
    using ifnull(update_job.snapshot_timestamp, current_timestamp()) as timestamp
  ;

  if @@row_count = 0 then
    raise using message = format('Updated but No data: %t', (update_job.query));
  end if;
end;
