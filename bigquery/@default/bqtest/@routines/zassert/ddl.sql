create or replace function `bqtest.zassert`(expected ANY TYPE, actual ANY TYPE)
as (
  if(expected = actual, null, error(format("Expected %T: %T", expected, actual)))
);

begin
  select `bqtest.zassert`('hoge', 'hoge');
end
