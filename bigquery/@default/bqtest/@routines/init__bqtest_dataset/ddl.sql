create or replace procedure `bqtest.init__bqtest_dataset`(
  target_name struct<project_id string, dataset_id string>
)
begin
  declare identifier string default format('%s.%s', coalesce(target_name.project_id, @@project_id), target_name.dataset_id);
  for routine in (
    select
      ddl as _origin
      , replace(
        `bqtest.zreplace_table_identifier`(
          ddl
          , (
            "bqtest.INFORMATION_SCHEMA"
            , format("%s.INFORMATION_SCHEMA", identifier)
          )
        )
        , "CREATE FUNCTION bqmake.bqtest"
        , format("create or replace function %s", identifier)
      ) as ddl
    from `bqtest.INFORMATION_SCHEMA.ROUTINES`
    where
      starts_with(routine_name, 'zbqt')
  )
  do
    begin
      execute immediate routine.ddl;
      exception when error then
        call `v0.log`(@@error.message);
    end;
  end for;
end
;

begin
  declare name, init_sql, defer_sql string;
  set (name, init_sql, defer_sql) = `bqtest.zgensql__temporary_dataset`();
  execute immediate init_sql;
  begin
    -- Provisioning for test
    call `bqtest.init__bqtest_dataset`((null, name));
    execute immediate format("""
      create or replace view `%s.%s`
      as
      with datasource as (
        select * from `bigquery-public-data.austin_311.311_service_requests`
      )
      select * from datasource
      """
      , name, "derivative_view"
    );

    -- Run test case
    execute immediate format("""
        `bqmake.%s.zgensql__view_test`(
        "derivative_view"
        , [
          ("datasource", ["unique_key"], if(false, [''], []), if(false, [('', [''])], []))
        ]
      )
      """
      , name
    );
  exception when error then
    call `v0.log`(struct(@@error.message as message, @@error.formatted_stack_trace as formatted_stack_trace));
  end;
  -- Tear up for testing
  execute immediate defer_sql;
end
