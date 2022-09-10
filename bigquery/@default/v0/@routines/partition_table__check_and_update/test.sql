declare query string;
set query = """
  select date(timestamp_micros(event_timestamp)) as event_date, event_name, count(1)
  from `bigquery-public-data.ga4_obfuscated_sample_ecommerce.events_*`
  where parse_date('%Y%m%d', _TABLE_SUFFIX) between @begin and @end
  group by event_date, event_name
""";

create schema if not exists `zpreview__partition_check_and_update`;
create or replace table `zpreview__partition_check_and_update.ga4_count`(event_date date, event_name string, records int64)
partition by event_date;

call `bqmake.v0.partition_table__check_and_update`(
  (null, 'zpreview__partition_check_and_update', 'ga4_count')
  , [('bigquery-public-data', 'ga4_obfuscated_sample_ecommerce', 'events_*')]
  , `bqmake.v0.alignment__day2day`('2021-01-01', '2021-01-01')
  , query
  , null
);
assert exists(select @@row_count > 0)
  as "20110101 partition update call should be added"
;

call `bqmake.v0.partition_table__check_and_update`(
  (null, 'zpreview__partition_check_and_update', 'ga4_count')
  , [('bigquery-public-data', 'ga4_obfuscated_sample_ecommerce', 'events_*')]
  , `bqmake.v0.alignment__day2day`('2021-01-01', '2021-01-01')
  , query
  , null
);
assert exists(select @@row_count > 0)
  as "20110101 partition update call should't be updated"
;

drop schema `zpreview__partition_check_and_update` cascade;
