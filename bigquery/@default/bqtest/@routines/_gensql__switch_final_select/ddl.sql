create or replace function `bqtest._gensql__switch_final_select`(
  _table_name string
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
as ((
  with views as (
    select view_definition
    from `bqtest.INFORMATION_SCHEMA.VIEWS`
    where table_name = _table_name
  )
  , switched as (
    select
      view_definition
      , rtrim(left(
          view_definition
          , `bqmake.bqtest.zfind_final_select`(view_definition)
      ))
      || format('\nselect * from `%s`', new_final_target)
    as new_view_definition
    from views
  )

  select new_view_definition from switched
))
;

begin
  execute immediate `bqtest._gensql__switch_final_select`(derivative_view, '__test_count');
end
