create or replace procedure `bqtest.switching_final_view`(
  out generated_sql string
  , target struct<table_catalog string, table_schema string, table_name string>
  , new_final_target string
)
options (description="""Generate New SQL based on `target`'s view_definition.
Arguments
===
- generated_sql: output variable
- target: target view
- placeholders:

Examples
===
"""
)
begin
  execute immediate format("""
  with views as (
    select
      view_definition
    from
      `%s.INFORMATION_SCHEMA.VIEWS`
    where
      table_name = "%s"
  )
  , switched as (
    select
      view_definition
      , rtrim(left(
          view_definition
          , `bqmake.zsbx__prototyping.zfind_final_select`(view_definition)
      ))
      || '\\nselect * from `%s`'
      as new_view_definition
    from views
  )

  select new_view_definition from switched
  """
    , coalesce(format('%s.%s', target.table_catalog, target.table_schema), target.table_schema)
    , target.table_name
    , new_final_target
  ) into generated_sql
  ;
end;

begin
  declare generated_sql string;

  create schema if not exists `zpreview_proto`;

  create or replace view `zpreview_proto.derivative_view`
  as
  with datasource as (
    select * from `bigquery-public-data.austin_311.311_service_requests`
  )
  , __test_count as (
    select count(1) from datasource
  )
  select * from datasource
  ;

  call `bqtest.switching_final_view`(generated_sql, (null, 'zpreview_proto', 'derivative_view'), '__test_count');
  execute immediate generated_sql;

  drop schema `zpreview_proto` cascade;
end
