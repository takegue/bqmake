create or replace function `bqtest.sure_values`(value ANY TYPE, acceptable_value_array ANY TYPE)
as (
  if(
    value in unnest(acceptable_value_array)
    , value
    , error(format("Assertion failed: %T not in %T", value, acceptable_value_array))
  )
);

begin
  select `bqtest.sure_values`("hoge", ["hoge", "fuga"]);
end
