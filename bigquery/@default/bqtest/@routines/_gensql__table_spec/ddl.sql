create or replace function `bqtest._gensql__table_spec`(
  _table_name string
  , test_configs array<struct<
    cte string
    , unique_columns array<string>
    , nonnull_columns array<string>
    , accepted_values_columns array<struct<column string, accepcted_values array<string>>>
  >>
) as ((
with views as (
  select view_definition
  from `bqtest.INFORMATION_SCHEMA.VIEWS`
  where table_name = _table_name
  union all
  select format(ltrim(`bqtest.zdeindent`("""
    # Auto-generated SQL by bqmake.bqtest
    with datasource as (
      select * from `%s.%s.%s`
    )
    select * from datasource
    """))
  , table_catalog, table_schema, table_name)
  from `bqtest.INFORMATION_SCHEMA.TABLES`
  where table_name = _table_name
)
, switched as (
  select
    view_definition
    , rtrim(left(
        view_definition
        , `bqtest.zfind_final_select`(view_definition)
    ))
    -- test case CTEs
    || array_to_string(array(
        select
          format(
            ', __test_%s as (\n%s\n)'
            , cte
            , `bqtest.zgensql__table_tester`(
              cte
              , config.unique_columns
              , config.nonnull_columns
              , config.accepted_values_columns
            )
          )
        from unnest(`bqtest.zfind_ctes`(view_definition)) as cte
        left join unnest(test_configs) as config using(cte)
        where config.cte is not null
      )
      , '\n'
    )
    -- final select
    || '\n'
    || array_to_string(array(
        select
          format('select * from __test_%s', cte)
        from unnest(`bqtest.zfind_ctes`(view_definition)) as cte
        left join unnest(test_configs) as config using(cte)
        where config.cte is not null
      )
      , '\nunion all\n'
    ) as sql
  from views
)

select as value sql from switched
))
;

begin
  declare name, init_sql, defer_sql string;
  set (name, init_sql, defer_sql) = `bqtest.zgensql__temporary_dataset`();
  execute immediate init_sql;
  begin
    call `bqtest.init_bqtest`((null, name));
    execute immediate format("""
      create or replace views `%s.%s`
      with datasource as (
        select * from `bigquery-public-data.austin_311.311_service_requests`
      )
      select * from datasource
      """
      , name, "derivative_view"
    );
  exception when error then
    call `v0.log`(struct(@@error.message as message, @@error.formatted_stack_trace as formatted_stack_trace));
  end;

  execute immediate """
    select `bqmake.bqtest._gensql__table_spec`(
      "derivative_view"
      , [
          ("datasource", ["unique_key"], if(false, [''], []), if(false, [('', [''])], []))
        ]
    );
  """;

  exception when error then
    execute immediate defer_sql;
end;