declare start date default '2019-01-01';

set start = (
  least(
    ifnull((
      select as value
        date_add(max(parse_date('%Y%m%d', regexp_extract(table_name, r'\d+$'))), interval 1 day)
      from `bqtest.INFORMATION_SCHEMA.PARTITIONS`
      where
        starts_with(table_name, 'demo_sample_shards_')
        and ifnull(
          parse_datetime('%Y%m%d', regexp_extract(table_name, r'\d+$')) > current_datetime() - interval 30 day
          , true
      ))
      , start
    )
    , date '2023-01-01'
  )
);

select start;

FOR record in (
  select * from unnest(generate_date_array(start, '2022-12-31')) as d
) DO
  execute immediate format("""
    create table if not exists bqtest.demo_sample_shards_%s
    as
    select @date as date_jst
  """
  , format_date('%Y%m%d', record.d)
  ) using record.d as date
  ;
END FOR;

with t as (
  select * from `bqtest.demo_sample_shards_*`
)
select 1;
