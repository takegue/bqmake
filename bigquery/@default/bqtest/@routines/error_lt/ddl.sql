create or replace function `bqtest.error_lt`(lhs ANY TYPE, rhs ANY TYPE, msg string)
as (
  if(
    lhs < rhs
    , null
    , error(rtrim(format("Assertion failed: left < right (left: %T, right: %T). %s", lhs, rhs, ifnull(msg, ''))))
  )
);

begin
  select `bqtest.error_lt`(2, 3, "int");
end
