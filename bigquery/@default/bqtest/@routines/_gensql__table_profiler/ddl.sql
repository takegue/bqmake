create or replace function `bqtest._gensql__table_profiler`(
  target_table_name string
  , group_keys array<string>
  , options_json json
)
as ((
with
  options as (
    select
      ifnull(bool(options_json.materialized_view_mode), false) as option_materialized_view_mode
      , ifnull(int64(options_json.numeric_precision), 6) as option_numeric_precision
  )
  , table_columns as (
    select
      table_catalog, table_schema, target_table as table_name, column_name
      , field_path
      , ordinal_position as position
      , depth
      , max(partition_column) over (partition by table_name) as partition_column
      , row_number() over (partition by table_name, column_name, ordinal_position, depth) as subposition
      , path.data_type
      , starts_with(c.data_type, 'ARRAY') as is_under_array
    from `bqtest.INFORMATION_SCHEMA.COLUMNS` c
    left join `bqtest.INFORMATION_SCHEMA.COLUMN_FIELD_PATHS` as path
      using(table_catalog, table_schema, table_name, column_name)
    left join unnest([struct(
       target_table_name as target_table
    )])
    left join unnest([struct(
      array_length(REGEXP_EXTRACT_ALL(field_path, r'\.')) as depth
      , contains_substr(target_table, '*') as has_wildcard
    )])
    left join unnest([struct(
      coalesce(
        if(is_partitioning_column = 'YES', column_name, null)
        , if(has_wildcard, '_TABLE_SUFFIX', null)
        , null
      ) as partition_column
    )])
    where
      if(
        has_wildcard
        , starts_with(table_name, regexp_replace(target_table, r'\*$', ''))
        , table_name = target_table
      )
    qualify
        1 = row_number() over (partition by table_schema, target_table, field_path order by table_name desc)
    order by position, depth, subposition
 )
, ddl as (
select
  table_catalog, table_schema, table_name
  ,
  "  select\n"
  || format('%s as partition_key\n', ifnull(max(partition_column), 'null'))
  || ifnull(
    format(
      ", nullif(format('%%t', (%s)), '') as group_keys\n"
      , nullif(array_to_string(group_keys, ', '), '')
    )
    , ', null as group_keys\n')
  || '   , count(1) as count\n'
  || string_agg(
    trim(
      replace(
        replace(
          replace(template_selected, '!column!', replace(field_path, '.', '___'))
          , '!fieldnum!', if(depth = 0, format('f%d',position), format('f%d_%d_%d', position, depth, subposition))
        )
        ,  '!fieldname!', field_path
      )
    )
    , '\n' order by position, depth, subposition
  )
  || format("""
    from `%s.%s.%s`
    group by partition_key, group_keys
    """
    , table_catalog, table_schema, table_name
  )
  as query
  from table_columns, options
  left join unnest([struct(
      format(r"""
      -- !column! (!fieldnum!)
      , count(!fieldname! is not null) as !column!__nonnull
      , approx_count_distinct(!fieldname!) as !column!__unique
      , hll_count.init(!fieldname!) as !column!__hll
      , sum(cast(!fieldname! as bignumeric)) as !column!__sum
      , round(avg(!fieldname!), %d) as !column!__avg
      , min(!fieldname!) as !column!__min
      , max(!fieldname!) as !column!__max
      """
      , option_numeric_precision
    ) || if(
        not option_materialized_view_mode
        , """
        , approx_top_count(!fieldname!, 5) as !column!__top_count
        , approx_quantiles(!fieldname!, 20) as !column!__20quantile
        , '!fieldname!' as !column!__name
        """
        , ''
      ) as number
      , format(r"""
        -- !column! (!fieldnum!)
        , count(!fieldname! is not null) as !column!__nonnull
        , sum(cast(!fieldname! as bignumeric)) as !column!__sum
        , round(avg(!fieldname!), %d) as !column!__avg
        , min(!fieldname!) as !column!__min
        , max(!fieldname!) as !column!__max
      """
      , option_numeric_precision
    ) || if(
          not option_materialized_view_mode
          , """
          , approx_top_count(!fieldname!, 5) as !column!__top_count
          , approx_quantiles(!fieldname!, 20) as !column!__20quantile
          , '!fieldname!' as !column!__name
        """
        , ''
      ) as float
    , format(r"""
      -- !column! (!fieldnum!)
      , count(!fieldname! is not null) as !column!__nonnull
      , approx_count_distinct(!fieldname!) as !column!__unique
      , hll_count.init(!fieldname!) as !column!__hll
      , round(avg(CHARACTER_LENGTH(!fieldname!)), %d) as !column!__avg_len
      , min(CHARACTER_LENGTH(!fieldname!)) as !column!__min_len
      , max(CHARACTER_LENGTH(!fieldname!)) as !column!__max_len
      """
      , option_numeric_precision
    ) || if(
        not option_materialized_view_mode
        , """
        , approx_top_count(!fieldname!, 20) as !column!__top_count
        , approx_quantiles(!fieldname!, 20) as !column!__20quantile
        , '!fieldname!' as !column!__name
        """
        , ''
      ) as string
      , r"""
        -- !column! (!fieldnum!)
        , count(!fieldname! is not null) as !column!__nonnull
        , hll_count.init(string(date(!fieldname!))) as !column!__day_hll
        , min(!fieldname!) as !column!__min
        , max(!fieldname!) as !column!__max
      """ as timestamp
      , r"""
        -- !column! (!fieldnum!)
        , count(!fieldname! is not null) as !column!__nonnull
        , '!fieldname!' as !column!__name
      """ as anything
      )]) as template
      left join unnest([struct(
          case
          when data_type in ('INT64', 'NUMERIC', 'BIGNUMERIC')
            then template.number
          when data_type in ('FLOAT64')
            then template.float
          when data_type in ('TIMESTAMP', 'DATE', 'DATETIME')
            then template.timestamp
          when data_type in ('STRING')
            then template.string
          else
            template.anything
          end as template_selected
      )])
  where
    -- Filter unsuported types
    not starts_with(data_type, 'STRUCT')
    and not ifnull(is_under_array, false)
    and field_path not in unnest(group_keys)

    group by table_catalog, table_schema, table_name
  )

select any_value(ddl.query) from ddl
))
;

execute immediate `bqtest._gensql__table_profiler`("demo_sample_table", null, null);
execute immediate `bqtest._gensql__table_profiler`("demo_sample_view", null, null);
