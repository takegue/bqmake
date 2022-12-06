create or replace function `bqtest.sure_values`(
  value ANY TYPE
  , acceptable_value_array ANY TYPE
)
as (
  if(
    value not in unnest(acceptable_value_array)
    , error(format("bqmake.bqtest.sure_values: Value %T is not allowed. %T", value, acceptable_value_array))
    , value
  )
);

begin
  select `bqtest.sure_values`("hoge", ["hoge", "fuga"]) = "hoge";
  assert `bqtest.sure_values`( NULL, ["hoge", "fuga"]) is null;
end
