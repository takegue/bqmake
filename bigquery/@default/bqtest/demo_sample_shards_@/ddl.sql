

FOR record in (
  select * from unnest(generate_date_array('2019-01-01', '2022-12-31')) as d
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
