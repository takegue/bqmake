create or replace function `bqtest.zgensql__view_test`(
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
  from `zpreview_proto.INFORMATION_SCHEMA.VIEWS`
  where
    table_name = _table_name
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
            , `bqtest.zgensql__table_test`(
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

select bqmake.bqtest.zgensql__view_test(
  'derivative_view'
  , [
    ("datasource", ["unique_key"], if(false, [''], []), if(false, [('', [''])], []))
  ]
)
