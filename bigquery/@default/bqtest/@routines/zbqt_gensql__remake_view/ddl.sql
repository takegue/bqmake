create or replace function `bqtest.zbqt_gensql__remake_view`(
  _table_name string
  , _final_cte string
  , _mocking_ctes array<struct<from_value string, to_value string>>
)
options (description="""Generate New SQL based on `target`'s view_definition.

Arguments
===
- _table_name
- _final_cte
- _mocking_ctes

Examples
===
"""
)
as ((
  with views as (
    select
      view_definition
    from `bqtest.INFORMATION_SCHEMA.VIEWS`
    where table_name = _table_name
  )
  , switched as (
    select
      view_definition
      , rtrim(left(
        remaked
        , `bqmake.bqtest.zfind_final_select`(remaked)
      ))
      || format('\nselect * from `%s`', _final_cte)
    as new_view_definition
    from views
    left join unnest([struct(
       `bqmake.bqtest.zreplace_table_identifiers`(view_definition, _mocking_ctes) as remaked
    )])
  )

  select new_view_definition from switched
))
;

begin
  with deps as (
    select * from bqtest.demo_sample_view
  )
  select 1;

  execute immediate `bqtest.zbqt_gensql__remake_view`(
    'demo_sample_view', '__test_count', [('datasource', 'datasource_sampled')]
  );
end
