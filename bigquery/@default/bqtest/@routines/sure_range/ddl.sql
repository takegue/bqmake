create or replace function `bqtest.sure_range`(
  value ANY TYPE
  , lower_bound ANY TYPE
  , upper_bound ANY TYPE
  , allow_NULL BOOLEAN
)
as (
  if(
    ifnull(
      value
        between
          case
            when lower_bound is not null then lower_bound
            else
              case `bqutil.fn.typeof`(value)
                when 'INT64' then -0x8000000000000000   -- 4byte = 2^63
                when 'NUMERIC' then -9.9999999999999999999999999999999999999E+28
                when 'BIGNUMERIC' then -5.7896044618658097711785492504343953926634992332820282019728792003956564819968E+38
                when 'FLOAT64' then cast("-inf" as float64)
                else error(format("Unsupported default value: type=%t", `bqutil.fn.typeof`(value)))
              end
          end
        and
          case
            when upper_bound is not null then lower_bound
            else
              case `bqutil.fn.typeof`(value)
                when 'INT64' then 0xFFFFFFFFFFFFFFF   -- 4byte = 2^63 - 1
                when 'NUMERIC' then 9.9999999999999999999999999999999999999E+28
                when 'BIGNUMERIC' then +5.7896044618658097711785492504343953926634992332820282019728792003956564819968E+38
                when 'FLOAT64' then cast("+inf" as float64)
                else error(format("Unsupported default value: type=%t", `bqutil.fn.typeof`(value)))
              end
            end
      , ifnull(allow_NULL, true)
    )

    , value
    , error(format("bqmake.bqtest.sure_range: Value %T must be included in [%T, %T]", value, lower_bound, upper_bound))
  )
);

begin
  select `bqtest.sure_range`(1, 1, 10, true);
end
