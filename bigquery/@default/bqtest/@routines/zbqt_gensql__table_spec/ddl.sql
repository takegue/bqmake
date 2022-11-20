create or replace function `bqtest.zbqt_gensql__table_spec`(
  _table_name string
  , test_configs array<struct<
    cte string
    , unique_columns array<string>
    , nonnull_columns array<string>
    , accepted_values_columns array<struct<column string, accepcted_values array<string>>>
  >>
)
returns string
as ((
with views as (
  select view_definition
  from `bqtest.INFORMATION_SCHEMA.VIEWS`
  where table_name = _table_name
  union all
  select format(
    ltrim(`v0.zdeindent`("""
      # Auto-generated SQL by bqmake.bqtest
      with __final__ as (
        select * from `%s.%s.%s`
      )
      select * from __final__
    """))
    , table_catalog, table_schema, table_name
  )
  from `bqtest.INFORMATION_SCHEMA.TABLES`
  where
    table_name = _table_name
    and table_type = 'BASE TABLE'
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
            , `bqtest.zgensql__property_testing`(
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
    || array_to_string(if(
        array_length(
          final_selects) > 0
          , final_selects
          , error(format('Not Found CTE: %T', test_configs))
        )
        , '\nunion all\n'
      ) as sql
  from views
  left join unnest([struct(
    array(
      select
        format('select * from __test_%s', cte)
      from unnest(`bqtest.zfind_ctes`(view_definition)) as cte
      left join unnest(test_configs) as config using(cte)
      where config.cte is not null
    ) as final_selects
  )])
)

select as value sql from switched limit 1
))
;

begin
  execute immediate `bqtest.zbqt_gensql__table_spec`(
    "demo_sample_table"
    , [
        ("__final__", ["unique_key"], if(false, [''], []), if(false, [('', [''])], []))
      ]
  );
end;
