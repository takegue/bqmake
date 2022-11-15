declare update_job string;
declare is_update bool default false;

set update_job = `bqmake.bqtest.zgensql__table_profiler`("demo_sample_table", null, to_json(struct(true as materialized_view_mode)));
call `bqmake.v0.snapshot_table__init`(
  (null, "bqtest", "zsnapshot_profile__demo_sample_table")
  , (
    "format('%t', (partition_key, group_keys))"
    , update_job
    , null
  )
  , null
);

if not ifnull(is_update, false) then
  -- Show Changes
  execute immediate `bqmake.v0.zgensql__snapshot_scd_type2`(
    ('bqmake', 'bqtest', 'zsnapshot_profile__demo_sample_table')
    , update_job, "format('%t', (partition_key, group_keys))"
    ).diff_query
    using current_timestamp() as timestamp;
else
  -- Save Changes
  call `bqmake.v0.snapshot_table__update`(
    (null, "bqtest", "zsnapshot_profile__demo_sample_table")
    , null
    , (
      "format('%t', (partition_key, group_keys))"
      , update_job
      , null
    )
    , null
  );
end if;
