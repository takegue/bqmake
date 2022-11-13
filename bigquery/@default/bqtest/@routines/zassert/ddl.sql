create or replace function `bqtest.zassert`(expected ANY TYPE, actual ANY TYPE)
as (
  if(expected = actual, "PASSED", error(format("FAILED: Expected %T but actual is %T", expected, actual)))
);

begin
  select `bqtest.zassert`('hoge', 'hoge');
end
