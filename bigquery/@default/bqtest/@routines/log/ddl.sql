create or replace procedure `bqtest.log`(obj any type)
options(
  strict_mode = false
  , description="""BigQuery Script Logger
  Argument `obj` is available for any type including struct, array.
  And it should be noted that should STRUCT type will cause runtime error
  """
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
