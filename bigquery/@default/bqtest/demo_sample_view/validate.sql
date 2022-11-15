declare update_job string;
set update_job = `bqmake.bqtest.zgensql__table_profiler`("demo_sample_view", ["week"], null);
select update_job;

call `bqmake.v0.snapshot_table__init`(
  (null, "bqtest", "zsnapshot_profile__demo_sample_view")
  , (
    "format('%t', (partition_key, group_keys))"
    , update_job
    , null
  )
  , null
);

call `bqmake.v0.snapshot_table__update`(
  (null, "bqtest", "zsnapshot_profile__demo_sample_view")
  , null
  , (
    "format('%t', (partition_key, group_keys))"
    , update_job
    , null
  )
  , null
);
