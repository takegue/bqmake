create or replace procedure `bqtest.log`(obj any type)
options(
  strict_mode = false
)
begin
  execute immediate
    format("select\n/*%T*/\n@p as obj", obj)
    using obj as p
  ;
end;

begin
  call `bqtest.log`(struct("hoge" as a, "fuga" as b));
end
