create or replace function `bqtest.sure_like`(value string, like_pattern string)
as (
  if(
    value not like like_pattern
    , error(format("bqmake.bqtest.sure_like: Value must be matech (%T LIKE %T)", value, like_pattern))
    -- NULL or value is passed
    , value
  )
);

begin
  select `bqtest.sure_like`('[Testcase]', "[%]");
end
