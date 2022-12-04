create or replace function `bqtest.sure_eq`(actual ANY TYPE, expected ANY TYPE, msg STRING)
as (
  if(
    ifnull(expected = actual, expected is null and actual is null)
    , actual
    , error(format("Assertion failed: %s (actual: %T, expected: %T)", msg, expected, actual))
  )
);

begin
  select `bqtest.sure_eq`('hoge', 'hoge', "string");
  select `bqtest.sure_eq`(('a', 'b'), ('a', 'b'), "struct");
  select `bqtest.sure_eq`(format('%t', ['a', 'b']), format('%t', ['a', 'b']), "array");
  select `bqtest.sure_eq`(null, null, "null");
end
