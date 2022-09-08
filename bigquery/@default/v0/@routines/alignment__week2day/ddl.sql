create or replace function `v0.alignment__week2day`(`from` DATE, `to` DATE)
as
(
 array(
  select as struct
    format_date('%Y%m%d', date_trunc(d, WEEK)) as destiantion
    , array_agg(distinct format_date('%Y%m%d', d)) as sources
  from unnest(generate_date_array(`from`, `to`)) as d
  group by destiantion
 )
)
;

begin
  assert array_length(`v0.alignment__week2day`('2022-01-01', '2022-01-31')) = 6
    as "v0.alignment__week2day('2022-01-01', '2022-01-01') should return 6 rows"
  ;
end;
