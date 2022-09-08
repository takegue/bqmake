create schema if not exists `zsandbox`;
create or replace table `zsandbox.ga4_count`(event_date date, event_name string, records int64)
partition by event_date;


call `bqmake.v0.partition_table__check_and_update`(
  (null, 'zsandbox', 'ga4_count'),
  [('bigquery-public-data', 'ga4_obfuscated_sample_ecommerce', 'events_*')],
  `bqmake.v0.alignment__day2day`('2021-01-01', '2021-01-01'),
  """
   select date(timestamp_micros(event_timestamp)) as event_date, event_name, count(1)
   from `bigquery-public-data.ga4_obfuscated_sample_ecommerce.events_*`
   where parse_date('%Y%m%d', _TABLE_SUFFIX) between @begin and @end
   group by event_date, event_name
  """
  , null
);

drop schema if exists `zsandbox` cascade;
