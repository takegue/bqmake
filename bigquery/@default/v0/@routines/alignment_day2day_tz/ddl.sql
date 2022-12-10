create or replace function `bqmake.v0.alignment_day2day_tz`(`from` DATE, `to` DATE)
as
(
 array(
  select as struct
    fd as destination, [format_date('%Y%m%d', d - interval 1 day), fd, format_date('%Y%m%d', d + interval 1 day)] as sources
  from unnest(generate_date_array(`from`, `to`)) as d
  left join unnest([format_date('%Y%m%d', d)]) as fd
 )
)
;

begin
  assert array_length(`bqmake.v0.alignment_day2day_tz`('2022-01-01', '2022-01-31')) = 31
    as "v0.alignment_day2day_tz('2022-01-01', '2022-01-01') should return 31 rows"
  ;
end;
