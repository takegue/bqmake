
with Q as (
  select * from bqtest.zsnapshot_profile__demo_sample_table
)
select 1;

call `bqtest.assert_golden`(
  (null, "bqtest", "zsnapshot_profile__demo_sample_table")
  , -- Profiling query
  `bqmake.bqtest.zgensql__table_profiler`(
    "demo_sample_table"
    , null
    , to_json(struct(true as materialized_view_mode))
  )
  , false
);
