create or replace function `bqtest.sure_values`(
  value ANY TYPE
  , acceptable_value_array ANY TYPE
  , allow_null BOOLEAN
)
as (
  if(
    value in unnest(acceptable_value_array)
    or ifnull(allow_null, true) and value is null
    , value
    , error(format("bqmake.bqtest.sure_values: Value %T is not allowed. %T", value, acceptable_value_array))
  )
);

begin
  select `bqtest.sure_values`("hoge", ["hoge", "fuga"], false);
end
