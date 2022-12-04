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
      select * from `%s.%s.%s`
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
    , coalesce(
      if(
        contains_substr(cte_parts, 'with')
        , cte_parts || '\n, '
        , null
      )
      , 'with '
    )
    || `bqmake.v0.zdeindent`(format("""
      __default_final__ as (
        %s
      )
      """
      , trim(substr(view_definition, `bqtest.zfind_final_select`(view_definition) + 1))
    ))
    -- test case CTEs
    || array_to_string(array(
        select
          format(
            ', __test_%s as (\n%s\n)'
            , ifnull(cte, "__default_final__")
            , `bqtest.zgensql__property_testing`(
              ifnull(cte, "__default_final__")
              , config.unique_columns
              , config.nonnull_columns
              , config.accepted_values_columns
            )
          )
        from unnest(test_configs) as config
        left join unnest(`bqtest.zfind_ctes`(view_definition) || [string(null)]) as cte
           on config.cte is not distinct from cte

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
    trim(left(
      view_definition, `bqtest.zfind_final_select`(view_definition)
    )) as cte_parts
    , array(
      select
        format('select * from __test_%s', ifnull(cte, '__default_final__'))
      from unnest(test_configs) as config
      left join unnest(`bqtest.zfind_ctes`(view_definition) || [string(null)]) as cte
          on config.cte is not distinct from cte
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
        (string(null), ["unique_key"], if(false, [''], []), if(false, [('', [''])], []))
      ]
  );
  execute immediate `bqtest.zbqt_gensql__table_spec`(
    "demo_sample_view"
    , [
        (string(null), ["unique_key"], if(false, [''], []), if(false, [('', [''])], []))
      ]
  );
end;
