-- table existence check;
with Q as (
  select * from bqtest.demo_sample_table
)
select 1;

call `bqmake.v0.assert_golden`(
  (null, "bqtest", "zsnapshot_profile__demo_sample_table")
  , -- Profiling query
  `bqmake.bqtest.zbqt_gensql__table_profiler`(
    "demo_sample_table"
    , null
    , to_json(struct(true as materialized_view_mode))
  )
  , 'format("%t", (partition_key, group_keys))'
  , @update_golden
);
