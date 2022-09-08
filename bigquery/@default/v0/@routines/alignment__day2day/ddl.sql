create or replace function `v0.alignment__day2day`(`from` DATE, `to` DATE)
as
(
 array(
  select as struct
    fd as destination, [fd] as sources
  from unnest(generate_date_array(`from`, `to`)) as d
  left join unnest([format_date('%Y%m%d', d)]) as fd
 )
)
;

begin
  assert array_length(`v0.alignment__day2day`('2022-01-01', '2022-01-31')) = 31
    as "v0.alignment__day2day('2022-01-01', '2022-01-01') should return 31 rows"
  ;
end;
