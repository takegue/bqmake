with datasource as (
  select * from `bqtest.demo_sample_table`
)
, datasource_sampled as (
  select * from `bqtest.demo_sample_table`
  TABLESAMPLE SYSTEM (5 percent)
)
, __test_count as (
  select count(1) from datasource
)

select * from datasource
