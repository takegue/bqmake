create or replace procedure `bqtest.bqtest__init`(
  target_name struct<project_id string, dataset_id string>
)
begin
  declare identifier string default format('%s.%s', coalesce(target_name.project_id, @@project_id), target_name.dataset_id);
  execute immediate format("""
    create or replace view `%s.zbqtest_view`
    as
    select * from `%s.INFORMATION_SCHEMA.VIEWS`
    """
    , identifier
    , identifier
  );
end
;

begin
  declare name, init_sql, defer_sql string;
  set (name, init_sql, defer_sql) = `bqtest.zgensql__temporary_dataset`();
  execute immediate init_sql;
  begin
    call `bqtest.bqtest__init`((null, name));
  exception when error then
    select
      -- Except Errors
     @@error.message as message, @@error.formatted_stack_trace as formatted_stack_trace
    ;
  end;
  execute immediate defer_sql;
end
