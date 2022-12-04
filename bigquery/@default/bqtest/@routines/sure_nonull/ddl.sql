create or replace function `bqtest.sure_nonull`(value ANY TYPE)
as (
  if(
    value is not null
    , value
    , error("bqmake.bqtest.sure_nonull: Value must be nonull")
  )
);

begin
  select `bqtest.sure_nonull`('hoge');
  select `bqtest.sure_nonull`(1);
end
