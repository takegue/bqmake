with datasource as (
  select * from `bigquery-public-data.google_trends.top_terms`
)

select * from datasource
