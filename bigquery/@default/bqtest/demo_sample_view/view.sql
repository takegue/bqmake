with datasource as (
  select * from `bigquery-public-data.google_trends.top_terms`
)
, __test_count as (
  select count(1) from datasource
)

select * from datasource
