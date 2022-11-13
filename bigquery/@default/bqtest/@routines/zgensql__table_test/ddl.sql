create or replace table function `zpreview_proto.zgensql_table_test`(
  _table_name string
  , unique_columns array<string>
  , nonnull_columns array<string>
  , accepted_values_columns array<struct<column string, accepcted_values array<string>>>
)
as (
  with _templates as (
    select
      *
    from unnest([struct(
      struct(
        "column_uniqueness_check" as cte_name
        , """
        with unique_count as (
        select any_value(_uniqueness_target) as tgt, count(1) as actual
        from datasource
        left join unnest([
          struct(string(null) as _key, string(null) as _value)
          , %s
        ]) as _uniqueness_target
        group by format('%%t', _uniqueness_target)
        having tgt._key is not null
      )
      select
        format("Uniqueness check: %%s=%%s", tgt._key, tgt._value) as name
        , actual
        , 1 as expected
      from unique_count
      """
        as body_template
      , "('%s', format('%%t', %s))" as column_template
      ) as sql_uniqueness_check
      , struct(
        "column_nonnull_check" as cte_name
        , """
        with nonnull_count as (
          select
            any_value(_uniqueness_target) as tgt
            , countif(nullif(_uniqueness_target._value, 'NULL') is null) as actual
          from datasource
          left join unnest([
            struct(string(null) as _key, string(null) as _value)
            , %s
          ]) as _uniqueness_target
          group by _uniqueness_target._key
          having tgt._key is not null
        )
        select
          format("Non-null check: %%s", tgt._key) as name
          , actual
          , 0 as expected
        from nonnull_count
      """
        as body_template
        , "('%s', format('%%t', %s))" as column_template
      ) as sql_nonnull_check
      , struct(
        "column_accepted_values_check" as cte_name
        , """
        with stats as (
          select
            any_value(_uniqueness_target.spec) as spec
            , approx_top_count(nullif(_uniqueness_target._value, 'NULL'), 100) as actual
          from datasource
          left join unnest([
            struct(struct(string(null) as _key, cast(null as array<string>) as _expected) as spec, string(null) as _value)
            , %s
          ]) as _uniqueness_target
          group by format('%%t', _uniqueness_target.spec)
          having spec._key is not null
        )
        select
          format("Accepted column pattern check: %%s", spec._key) as name
          , array(select value from unnest(stats.actual) order by value) as actual
          , array(select format('%%T', value) from unnest(spec._expected) as value order by value) as expected
        from stats
        """
          as body_template
          , "((('%s', %T), format('%%T', %s)))"
        as column_template
      ) as sql_accepted_values_check
    )])
  )

  -- select original, new_view_definition from switched
  select
    array_to_string(
      [
        format('with\ndatasource as (select * from %s)', _table_name)
        , format('%s as (\n%s\n)', sql_uniqueness_check.cte_name, cte.sql_uniquness)
        , format('%s as (\n%s\n)', sql_nonnull_check.cte_name, cte.sql_nonnull)
        , format('%s as (\n%s\n)', sql_accepted_values_check.cte_name, cte.sql_accepted_values)
        , cte.report
      ]
      , '\n, '
    ) as sql
  from _templates
  left join unnest([struct(
    ltrim(format(
      sql_uniqueness_check.body_template
      , ifnull(nullif(array_to_string(array(
        select format(sql_uniqueness_check.column_template, c, c) from unnest(unique_columns) as c)
        , '\n, '), ''), 'NULL')
    )) as sql_uniquness
    , ltrim(format(
      sql_nonnull_check.body_template
      , ifnull(nullif(array_to_string(array(
        select format(sql_nonnull_check.column_template, c, c) from unnest(nonnull_columns) as c)
        , '\n, '), ''), 'NULL')
    )) as sql_nonnull
    , ltrim(format(
      sql_accepted_values_check.body_template
      , ifnull(nullif(array_to_string(array(
        select format(sql_accepted_values_check.column_template, c.column, c.accepcted_values, c.column) from unnest(accepted_values_columns) as c)
        , '\n, '), ''), 'NULL')
    )) as sql_accepted_values
    , """report as (
      with all_testcases as (
        select 'column_uniqueness_check' as group_name, name, format('%T', actual) as actual, format('%T', expected) as expected from column_uniqueness_check
        union all
        select 'column_nonnull_check' as group_name, name, format('%T', actual) as actual, format('%T', expected) as expected from column_nonnull_check
        union all
        select 'column_accepted_values_check' as group_name, name, format('%T', actual) as actual, format('%T', expected) as expected from column_accepted_values_check
      )
      select
        group_name
        , count(1) as n_cases
        , countif(actual = expected) as n_cases_passed
        , countif(actual != expected) as n_cases_failed
        , approx_top_sum(
            if(
              actual = expected
              , null
              , format('%s; Expected %t but actual is %t', name, expected, actual)
            )
            , if(actual = expected, null, 1)
            , 20
        ) as errors
      from all_testcases
      group by group_name
    )

    select * from report
    """
    as report
  )]) as cte
);

begin
  execute immediate
    (
      select as value sql
      from zpreview_proto.zgensql_table_test(
        "`bigquery-public-data.austin_311.311_service_requests`"
        , ["unique_key"]
        , ["status", "source"]
        , [("status", ["Closed"])]
      )
    )
  ;
end
