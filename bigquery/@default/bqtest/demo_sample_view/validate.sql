-- table existence check
with Q as (
  select * from bqtest.demo_sample_view
)
select 1;

call `bqtest.assert_golden`(
  (null, "bqtest", "zsnapshot_profile__demo_sample_view")
  , -- Profiling query
  `bqmake.bqtest.zbqt_gensql__table_profiler`(
    "demo_sample_view"
    , null
    , to_json(struct(true as materialized_view_mode))
  )
  , false
);
