with datasource as (
  select * from `bigquery-public-data.google_trends.top_terms`
)
, datasource_sampled as (
  select * from `bigquery-public-data.google_trends.top_terms`
  TABLESAMPLE SYSTEM (5 percent)
)
, __test_count as (
  select count(1) from datasource
)

select * from datasource
