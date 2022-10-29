create or replace procedure `v0.zgensql__table_profiler`(
  out ret string
  , destination struct<project string, dataset string, table string>
  , group_keys array<string>
  , options_json json
)
options(description="""Generate SQL for profiling table data

This is private routine and not designed to be called directly.
"""
)
begin
  declare dataset_ref string default format(
    '%s.%s'
    , coalesce(destination.project, @@project_id)
    , ifnull(destination.dataset, error('Not found dataset'))
  );
  declare option_materialized_view_mode bool default
    ifnull(bool(options_json.materialized_view_mode), false);

  execute immediate format("""
    create or replace temp table _tmp_table_columns
    as
      select
        table_catalog, table_schema, table_name, column_name
        , field_path
        , ordinal_position as position
        , depth
        , max(partition_column) over (partition by table_name) as partition_column
        , row_number() over (partition by table_name, column_name, ordinal_position, depth) as subposition
        , path.data_type
        , starts_with(c.data_type, 'ARRAY') as is_under_array
      from `%s.INFORMATION_SCHEMA.COLUMNS` c
      left join `%s.INFORMATION_SCHEMA.COLUMN_FIELD_PATHS` as path
        using(table_catalog, table_schema, table_name, column_name)
      left join unnest([struct(
        array_length(REGEXP_EXTRACT_ALL(field_path, r'\\.')) as depth
        , contains_substr(table_name, '*') as has_wildcard
        , "%s" as target_table
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
          , starts_with(table_name, regexp_replace(target_table, r'\\*$', ''))
          , table_name = target_table
        )
  """
    , dataset_ref
    , dataset_ref
    , destination.table
  )
  ;

  set ret = (
    with
      ddl as (
      select
        table_catalog, table_schema, table_name
        ,
        "  select\n"
        || "   partition_key\n"
        || ifnull(
          format(
            ", nullif(format('%%t', (%s)), '') as group_keys\n"
            , nullif(array_to_string(group_keys, ', '), '')
          )
          , ', null as group_keys\n')
        || '   , count(1) as count\n'
        || string_agg(
            trim(replace(
              replace(
                replace(
                  replace(template_selected, '!column!', field_path)
                  , '!column!', format('%s',field_path)
                )
                ,  '!fieldnum!', if(depth = 0, format('f%d',position), format('f%d_%d_%d', position, depth, subposition))
              )
              ,  '!fieldname!', field_path
            ))
          , '\n' order by position, depth, subposition
        )
        || format("""
          from `%s.%s.%s`
          left join unnest([%s]) partition_key
          group by partition_key, group_keys
        """
          , table_catalog, table_schema, table_name
          , ifnull(max(partition_column), 'null')
        )
          as query
      from _tmp_table_columns
      left join unnest([struct(
        r"""
          -- !column! (!fieldnum!)
          , count(!column! is not null) as !column!__nonnull
          , approx_count_distinct(!column!) as !column!__unique
          , hll_count.init(!column!) as !column!__hll
          , sum(cast(!column! as bignumeric)) as !column!__sum
          , avg(!column!) as !column!__avg
          , min(!column!) as !column!__min
          , max(!column!) as !column!__max
        """
        || if(
          not option_materialized_view_mode
          , """
            , approx_top_count(!column!, 5) as !column!__top_count
            , approx_quantiles(!column!, 20) as !column!__20quantile
            , '!fieldname!' as !column!__name
          """
          , ''
        )
          as number
        , r"""
          -- !column! (!fieldnum!)
          , count(!column! is not null) as !column!__nonnull
          , sum(cast(!column! as bignumeric)) as !column!__sum
          , avg(!column!) as !column!__avg
          , min(!column!) as !column!__min
          , max(!column!) as !column!__max
        """
        || if(
          not option_materialized_view_mode
          , """
            , approx_top_count(!column!, 5) as !column!__top_count
            , approx_quantiles(!column!, 20) as !column!__20quantile
          , '!fieldname!' as !column!__name
          """
          , ''
        )
          as float
        , r"""
          -- !column! (!fieldnum!)
          , count(!column! is not null) as !column!__nonnull
          , approx_count_distinct(!column!) as !column!__unique
          , hll_count.init(!column!) as !column!__hll
          , avg(CHARACTER_LENGTH(!column!)) as !column!__avg_len
          , min(CHARACTER_LENGTH(!column!)) as !column!__min_len
          , max(CHARACTER_LENGTH(!column!)) as !column!__max_len
        """ || if(
          not option_materialized_view_mode
          , """
            , approx_top_count(!column!, 20) as !column!__top_count
            , approx_quantiles(!column!, 20) as !column!__20quantile
            , '!fieldname!' as !column!__name
          """
          , ''
        )
          as string
        , r"""
          -- !column! (!fieldnum!)
          , count(!column! is not null) as !column!__nonnull
          , hll_count.init(string(date(!column!))) as !column!__day_hll
          , min(!column!) as !column!__min
          , max(!column!) as !column!__max
        """
          as timestamp
        , r"""
          -- !column! (!fieldnum!)
          , count(!column! is not null) as !column!__nonnull
          , '!fieldname!' as !column!__name
        """
          as anything
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
        and field_path not in unnest(group_keys)

      group by table_catalog, table_schema, table_name
    )

    select any_value(ddl.query) from ddl
  );

end;
