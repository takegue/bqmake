create or replace function `bqtest.zbqt_gensql__table_spec`(
  _table_name string
  , unique_columns array<struct<cte_name string, column string>>
  , accepted_values_columns array<struct<cte_name string, column string, accepcted_values array<string>>>
  , nonnull_columns array<struct<cte_name string, column string>>
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
            , `bqmake.v0.zreindent`(`bqtest.zgensql__property_testing`(
                ifnull(cte, "__default_final__")
                , config.unique_columns
                , config.nonnull_columns
                , config.accepted_values_columns
              )
              , 2
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
      array(
        select as struct
          cte_name as cte
          , array_agg(distinct U.column) as unique_columns
          , array_agg(distinct N.column) as nonnull_columns
          , array_agg(struct(A.column, A.accepcted_values)) as accepted_values_columns
        from unnest(
          array(select cte_name from unnest(unique_columns))
          || array(select cte_name from unnest(accepted_values_columns))
          || array(select cte_name from unnest(nonnull_columns))
        ) as cte_name
        left join unnest(unique_columns) as U
          on cte_name is not distinct from U.cte_name
        left join unnest(accepted_values_columns) as A
          on cte_name is not distinct from U.cte_name
        left join unnest(nonnull_columns) as N
          on cte_name is not distinct from U.cte_name
        group by cte_name
      ) as test_configs
  )])
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
    , unique_columns => [
      (string(null), "unique_key")
    ]
    , nonnull_columns => []
    , accepted_values_columns => []
  );
  execute immediate `bqtest.zbqt_gensql__table_spec`(
    "demo_sample_view"
    , unique_columns => [
      (string(null), "unique_key")
    ]
    , nonnull_columns => []
    , accepted_values_columns => []
  );
end;
