create or replace procedure `zsbx__prototyping.templating_tvf` (
  out generated_sql string
  , target struct<table_catalog string, table_schema string, table_name string>
  , placeholders array<struct<identifier string, new_identifier string>>
)
options (description="""Generate New SQL based on `target`'s view_definition.
Arguments
===
- generated_sql: output variable
- target: target view
- placeholders:
Examples
===
  declare target default struct(string(null) as table_catalog, "sandbox" as table_schema, "zmock_sample" as table_name);
  declare placeholders default [
    struct(
          "__mock__data" as identifier
          , "(select 2)" as new_identifier
      )];
  declare generated_sql string;
  call `sandbox.template_engine`(target, placeholders, generated_sql);
"""
)
begin
  execute immediate format("""
    with recursive template_apply as (
      select 0 as stage, view_definition as sql, placeholders from get_mockable_sql
      union all
      select
        stage + 1 as stage
        , regexp_replace(
          sql
          , format(r'`%%s`', json_value(p, '$.identifier'))
          , json_value(p, '$.new_identifier')
        )
        , placeholders
      from template_apply
      left join unnest([struct(placeholders[safe_offset(stage)] as p)])
      where stage < array_length(placeholders)
    )
    ,  input_view_information_schema as (
      select
        view_definition
        , @placeholders as placeholders
      from
        `%s.INFORMATION_SCHEMA.VIEWS`
      where
          table_name = "%s"
    )
    , get_mockable_sql as (
      select * replace(
          # NOTE: Struct type is not supported in the WITH RECURSIVE clause
          array(select to_json_string(struct(v.identifier, v.new_identifier)) from data.placeholders as v) as placeholders
      ) from `input_view_information_schema` data
    )
    select as struct
      trim(sql) as sql
    from template_apply
    where stage = array_length(placeholders)
  """
    , target.table_schema
    , target.table_name
  )
    into generated_sql
    using placeholders as placeholders
  ;
end
