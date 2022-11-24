create or replace function `bqtest.sure`(value ANY TYPE, condition bool, errmsg STRING)
as (
  if(
    ifnull(condition)
    , value
    , error(format("Assertion failed: %s", errmsg))
  )
);

begin
  select `bqtest.sure`('hoge', true, "sure string");
end
