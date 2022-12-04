create or replace function `bqtest.sure_nonull`(value ANY TYPE)
as (
  if(
    value is not null
    , value
    , error(format("Assertion failed: %s", errmsg))
  )
);

begin
  select `bqtest.sure_nonull`('hoge', true, "sure string");
  select `bqtest.sure_nonull`(1, true, "sure int64");
end
