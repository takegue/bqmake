create or replace function `bqtest.sure_like`(value string, like_pattern string)
as (
  if(
    value like like_pattern
    , value
    , error(format("bqmake.bqtest.sure_like: Value must be matech (%T LIKE %T)", value, like_pattern))
  )
);

begin
  select `bqtest.sure_like`('[Testcase]', "[%]");
end
