create or replace function `bqtest.zgensql__property_testing`(
  _table_identifier string
  , unique_columns array<string>
  , nonnull_columns array<string>
  , accepted_values_columns array<struct<column string, accepcted_values array<string>>>
)
as ((
  with _templates as (
    select
      *
    from unnest([struct(
      struct(
        "column_uniqueness_check" as cte_name
        , `v0.zdeindent`("""
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
        """) as body_template
        , "('%s', format('%%t', %s))" as column_template_arg1
        , string(null) as column_template_arg2
      ) as sql_uniqueness_check
      , struct(
        "column_nonnull_check" as cte_name
        , `v0.zdeindent`("""
          with nonnull_count as (
            select
              count(1) as actual__records
              , %s
            from datasource
          )
          select
            format("Non-null check: %%s [%%4.1f%%%%]", tgt._key, tgt.actual / actual__records) as name
            , actual
            , 0 as expected
          from nonnull_count
          left join unnest([
            struct(string(null) as _key, null as actual)
            , %s
          ]) as tgt
          where tgt._key is not null
      """)
        as body_template
        , "count(1) - countif(%s is not null) as %s" as column_template_arg1
        , "(%T, %s)" as column_template_arg2
      ) as sql_nonnull_check
      , struct(
        "column_accepted_values_check" as cte_name
        , `v0.zdeindent`("""
        with stats as (
          select
            any_value(_target.spec) as spec
            , approx_top_count(nullif(_target._value, 'NULL'), 100) as actual
          from datasource
          left join unnest([
            struct(struct(string(null) as _key, cast(null as array<string>) as _expected) as spec, string(null) as _value)
            , %s
          ]) as _target
          group by format('%%t', _target.spec)
          having spec._key is not null
        )
        select
          format("Accepted column pattern check: %%s", spec._key) as name
          , diff as actual
          , [("SURPLUS", []), ("MISSING", [])] as expected
        from stats
        left join unnest([struct(
          array(select value from unnest(stats.actual) order by value) as actual_values
          , array(select value from unnest(spec._expected) as value order by value) as expected_values
        )])
        left join unnest([struct(
          (
            select as value
              [
                struct("SURPLUS" as type, ifnull(array_agg(distinct if(expected is null, actual, null) ignore nulls), []) as values)
                ,("MISSING", ifnull(array_agg(if(actual is null, expected, null) ignore nulls), []))
              ] as diffs
            from unnest(actual_values || expected_values) as both
            left join unnest(actual_values) as actual on both = actual
            left join unnest(expected_values) as expected on both = expected
            limit 1
          ) as diff
        )])
        """)
        as body_template
        , "((('%s', %T), format('%%t', %s)))" as column_template_arg1
        , string(null) as column_template_arg2
      ) as sql_accepted_values_check
    )])
  )

  select
    array_to_string(
      [
        format('with\ndatasource as (select * from %s)', _table_identifier)
        , format('%s as (\n%s\n)', sql_uniqueness_check.cte_name, cte.sql_uniquness)
        , format('%s as (\n%s\n)', sql_nonnull_check.cte_name, cte.sql_nonnull)
        , format('%s as (\n%s\n)', sql_accepted_values_check.cte_name, cte.sql_accepted_values)
        , cte.report
      ]
      , '\n, '
    ) as sql
  from _templates
  left join unnest([struct(
    rtrim(format(
      sql_uniqueness_check.body_template
      , ifnull(nullif(array_to_string(array(
        select format(sql_uniqueness_check.column_template_arg1, c, c) from unnest(unique_columns) as c)
        , '\n, '), ''), 'NULL')
    )) as sql_uniquness
    , rtrim(format(
      sql_nonnull_check.body_template
      , ifnull(nullif(array_to_string(array(
        select format(sql_nonnull_check.column_template_arg1, c, c) from unnest(nonnull_columns) as c)
        , '\n, '), ''), 'NULL')
      , ifnull(nullif(array_to_string(array(
        select format(sql_nonnull_check.column_template_arg2, c, c) from unnest(nonnull_columns) as c)
        , '\n, '), ''), 'NULL')
    )) as sql_nonnull
    , rtrim(format(
      sql_accepted_values_check.body_template
      , ifnull(nullif(array_to_string(array(
        select format(sql_accepted_values_check.column_template_arg1, c.column, c.accepcted_values, c.column) from unnest(accepted_values_columns) as c)
        , '\n, '), ''), 'NULL')
    )) as sql_accepted_values
    , ltrim(`v0.zdeindent`(format(
      """
      report as (
        with all_testcases as (
          select 'column_uniqueness_check' as group_name, name, format('%%T', actual) as actual, format('%%T', expected) as expected from column_uniqueness_check
          union all
          select 'column_nonnull_check' as group_name, name, format('%%T', actual) as actual, format('%%T', expected) as expected from column_nonnull_check
          union all
          select 'column_accepted_values_check' as group_name, name, format('%%T', actual) as actual, format('%%T', expected) as expected from column_accepted_values_check
        )
        select
          %T as target_table
          , group_name
          , count(1) as n_cases
          , countif(actual = expected) as n_cases_passed
          , countif(actual != expected) as n_cases_failed
          , approx_top_sum(
              if(
                actual = expected
                , null
                , format('%%s; Expected %%t but actual is %%t', name, expected, actual)
              )
              , if(actual = expected, null, 1)
              , 20
          ) as errors
        from all_testcases
        group by group_name
    )
    select * from report
    """
    , _table_identifier
  )))
    as report
  )]) as cte
));

begin
  execute immediate
    (
      `bqtest.zgensql__property_testing`(
        "`bigquery-public-data.austin_311.311_service_requests`"
        , ["unique_key"]
        , ["status", "source"]
        , [("status", ["CancelledTesting", "Closed -Incomplete", "Closed -Incomplete Information", "Closed", "Duplicate (closed)", "Duplicate (open)", "Incomplete", "New", "Open", "Resolved", "TO BE DELETED", "Transferred", "Work In Progress"])]
      )
    )
  ;
end
