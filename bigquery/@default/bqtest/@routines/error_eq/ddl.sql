create or replace function `bqtest.error_eq`(expected ANY TYPE, actual ANY TYPE, msg STRING)
as (
  if(
    ifnull(expected = actual, expected is null and actual is null)
    , null
    , error(format("Assertion failed: %s (left: %T, right: %T)", msg, expected, actual))
  )
);

begin
  select `bqtest.error_eq`('hoge', 'hoge', "string");
  select `bqtest.error_eq`(('a', 'b'), ('a', 'b'), "struct");
  select `bqtest.error_eq`(format('%t', ['a', 'b']), format('%t', ['a', 'b']), "array");
  select `bqtest.error_eq`(null, null, "null");
end
