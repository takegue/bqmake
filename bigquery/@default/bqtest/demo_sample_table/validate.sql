declare update_job string;
set update_job = `bqmake.bqtest.zgensql__table_profiler`("demo_sample_table", null, null);
call `bqmake.v0.snapshot_table__init`(
  (null, "bqtest", "zsnapshot_profile__demo_sample_table")
  , (
    "format('%t', (partition_key, group_keys))"
    , update_job
    , null
  )
  , null
);

// Show Changes
execute immediate `bqmake.v0.zgensql__snapshot_scd_type2`(
  ('bqmake', 'bqtest', 'zsnapshot_profile__demo_sample_table')
  , update_job, "format('%t', (partition_key, group_keys))"
  ).diff_query
  using current_timestamp() as timestamp;

// Save Changes
call `bqmake.v0.snapshot_table__update`(
  (null, "bqtest", "zsnapshot_profile__demo_sample_table")
  , (
    "format('%t', (partition_key, group_keys))"
    , update_job
    , null
  )
  , null
);
