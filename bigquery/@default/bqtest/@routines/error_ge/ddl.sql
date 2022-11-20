create or replace function `bqtest.error_ge`(lhs ANY TYPE, rhs ANY TYPE, msg string)
as (
  if(
    lhs >= rhs
    , null
    , error(rtrim(format("Assertion failed: left >= right (left: %T, right: %T). %s", lhs, rhs, ifnull(msg, ''))))
  )
);

begin
  select `bqtest.error_ge`('hoge', 'hoge', "same");
  select `bqtest.error_ge`(0, 1, "int");
end
