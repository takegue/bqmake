create or replace function `bqtest.assert_eq`(expected ANY TYPE, actual ANY TYPE, msg STRING)
as (
  if(
    ifnull(expected = actual, expected is null and actual is null)
    , null
    , error(format("ASSERT: %s (%T != %T)", msg, expected, actual))
  )
);

begin
  select `bqtest.assert_eq`('hoge', 'hoge', "string");
  select `bqtest.assert_eq`(('a', 'b'), ('a', 'b'), "struct");
  select `bqtest.assert_eq`(format('%t', ['a', 'b']), format('%t', ['a', 'b']), "array");
  select `bqtest.assert_eq`(null, null, "null");
end
