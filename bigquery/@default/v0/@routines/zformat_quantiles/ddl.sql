create or replace function `v0.zformat_quantiles`(
  approx_array any type, picks array<int64>, round_scale int64
)
as (
  array(
    select as struct
      100 * ix / (array_length(approx_array) - 1) as key
      , ifnull(round(a, round_scale), a) as value
    from unnest(approx_array) as a with offset ix
    join unnest(if(
      array_length(picks) > 0
      , picks
      , generate_array(0, array_length(approx_array)))
    ) as ix using(ix)
  )
);

begin
  select
    `v0.zformat_quantiles`(approx_quantiles(latitude, 4), [])
  from `bqmake.bqtest.demo_sample_table`;
end;
