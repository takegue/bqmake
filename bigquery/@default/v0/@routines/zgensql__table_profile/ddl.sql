create or replace procedure `v0.zgensql__table_profile`(
  ret string
  , destination struct<project string, dataset string, table string>
  , group_keys array<string>
  , options array<struct<key string, value string>>
)
options(description="""Generate SQL for profiling table data
"""
)
begin
  declare dataset_ref string default format(
    '%s.%s'
    , coalesce(destination.project, @@project_id)
    , ifnull(destination.dataset, error('Not found dataset'))
  );
  declare option_materialized_view_mode bool default ifnull((
    select safe_cast(max(opt.value) as bool)
    from unnest(options) opt
    where key = 'materialized_view_mode'
  ), false);

  execute immediate format("""
    create or replace temp table _tmp_table_columns
    as
      select
        table_catalog, table_schema, table_name, column_name
        , field_path
        , ordinal_position as position
        , depth
        , max(if(is_partitioning_column = 'YES', field_path, null)) over (partition by table_name, column_name) as partition_column
        , row_number() over (partition by table_name, column_name, ordinal_position, depth) as subposition
        , path.data_type
        , starts_with(c.data_type, 'ARRAY') as is_under_array
      from `%s.INFORMATION_SCHEMA.COLUMNS` c
      left join `%s.INFORMATION_SCHEMA.COLUMN_FIELD_PATHS` as path
        using(table_catalog, table_schema, table_name, column_name)
      left join unnest([struct(
          array_length(REGEXP_EXTRACT_ALL(field_path, r'\\.')) as depth
      )])
      where
        table_name = '%s'
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
          format(", nullif(format('%%t', (%s)), '') as group_keys\n"
            , array_to_string(group_keys, ', '))
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
          , '!fieldname!' as !fieldnum!__name
          , count(!column! is not null) as !fieldnum!__nonnull
          , approx_count_distinct(!column!) as !fieldnum!__unique
          , hll_count.init(!column!) as !fieldnum!__hll
          , sum(!column!) as !fieldnum!__sum
          , avg(!column!) as !fieldnum!__avg
          , min(!column!) as !fieldnum!__min
          , max(!column!) as !fieldnum!__max
        """
        || if(
          not option_materialized_view_mode
          , """
            , approx_top_count(!column!, 5) as !fieldnum!__top_count
            , approx_quantiles(!column!, 20) as !fieldnum!__20quantile
          """
          , ''
        )
          as number
        , r"""
          -- !column! (!fieldnum!)
          , '!fieldname!' as !fieldnum!__name
          , count(!column! is not null) as !fieldnum!__nonnull
          , sum(!column!) as !fieldnum!__sum
          , avg(!column!) as !fieldnum!__avg
          , min(!column!) as !fieldnum!__min
          , max(!column!) as !fieldnum!__max
        """
        || if(
          not option_materialized_view_mode
          , """
            , approx_top_count(!column!, 5) as !fieldnum!__top_count
            , approx_quantiles(!column!, 20) as !fieldnum!__20quantile
          """
          , ''
        )
          as float
        , r"""
          -- !column! (!fieldnum!)
          , '!fieldname!' as !fieldnum!__name
          , count(!column! is not null) as !fieldnum!__nonnull
          , approx_count_distinct(!column!) as !fieldnum!__unique
          , hll_count.init(!column!) as !fieldnum!__hll
          , avg(CHARACTER_LENGTH(!column!)) as !fieldnum!__avg_len
          , min(CHARACTER_LENGTH(!column!)) as !fieldnum!__min_len
          , max(CHARACTER_LENGTH(!column!)) as !fieldnum!__max_len
        """ || if(
          not option_materialized_view_mode
          , """
            , approx_top_count(!column!, 5) as !fieldnum!__top_count
            , approx_quantiles(!column!, 20) as !fieldnum!__20quantile
          """
          , ''
        )
          as string
        , r"""
          -- !column! (!fieldnum!)
          , '!fieldname!' as !fieldnum!__name
          , count(!column! is not null) as !fieldnum!__nonnull
          , hll_count.init(string(date(!column!))) as !fieldnum!__day_hll
          , min(!column!) as !fieldnum!__min
          , max(!column!) as !fieldnum!__max
        """
          as timestamp
        , r"""
          -- !column! (!fieldnum!)
          , '!fieldname!' as !fieldnum!__name
          , count(!column! is not null) as nonnull
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
        not is_under_array
        and not starts_with(data_type, 'STRUCT')
      group by table_catalog, table_schema, table_name
    )

    select any_value(ddl.query) from ddl
  );

end;
