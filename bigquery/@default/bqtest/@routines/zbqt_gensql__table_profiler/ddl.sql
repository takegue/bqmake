create or replace function `bqtest.zbqt_gensql__table_profiler`(
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
    -- For expanding views
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
  , format("""
    with datasource as (
      select
        %s as partition_key
        , %s as group_keys
        , *
      from %s
    )
    """
    , ifnull(max(partition_column), 'date(null)')
    , ifnull(
      format(
        'nullif(format("%%t", (%s)), "")'
        , nullif(array_to_string(group_keys, ", "), "")
      )
      , 'null'
    )
    , if(
      any_value(option_materialized_view_mode and view_definition is not null)
      -- View expansion
      , format("(%s)", any_value(view_definition))
      , format('`%s.%s.%s.`', table_catalog, table_schema, table_name)
    )
  )
  -- For materialized table
  || format(
    """
    , restricted_view as (
      select
        partition_key
        , group_keys
        , count(1) as count
        %s
      from datasource
      group by partition_key, group_keys
    )
    """
    , string_agg(
      replace(replace(replace(
        template_selected.restricted
        ,    '!column!', replace(field_path, '.', '___'))
        ,  '!fieldnum!', if(depth = 0, format('f%d',position), format('f%d_%d_%d', position, depth, subposition)))
        , '!fieldname!', field_path
      )
      , '\n' order by position, depth, subposition
    )
  )
  || if(
    any_value(option_materialized_view_mode)
    , ""
    , format("""
      , advanced_parts as (
        select
          partition_key
          , group_keys
          %s
        from datasource
        group by partition_key, group_keys
      )
      """
      , string_agg(
        replace(replace(replace(
          template_selected.advanced
          ,    '!column!', replace(field_path, '.', '___'))
          ,  '!fieldnum!', if(depth = 0, format('f%d',position), format('f%d_%d_%d', position, depth, subposition)))
          , '!fieldname!', field_path
        )
        , '\n' order by position, depth, subposition
      )
    )
  )
  || ifnull(string_agg(
    replace(format("""
    , %s__subprofiler as (
      with total as (
        select
          partition_key
          , group_keys
          , count(1) as count
          , max(!fieldname!) as max
          , min(!fieldname!) as min
          , approx_quantiles(!fieldname!, 4) as qtile
        from datasource
        group by partition_key, group_keys
      )
      , agg as (
          select
            datasource.partition_key
            , datasource.group_keys
            , bucket_ix
            , struct(
              struct(
                if(bucket_ix = 0, cast('-inf' as float64), round(min(!fieldname!), %d))  as min
                , if(bucket_ix = array_length(any_value(buckets)), cast('+inf' as float64), round(max(!fieldname!), %d)) as max
              ) as bucket
              , count(1) as count
              , round(safe_divide(count(1), any_value(Q.count)), 4) as ratio
            ) as agg_item
          from
            datasource
            join total as Q
              on Q.partition_key is not distinct from datasource.partition_key
                and Q.group_keys is not distinct from datasource.group_keys
            left join unnest([struct(
              Q.qtile[offset(3)] - Q.qtile[offset(1)] as iqr
            )])
            left join unnest([struct(
              generate_array(Q.qtile[offset(1)] - 1.5 * iqr, Q.qtile[offset(3)] + 1.5 * iqr, iqr * 4 / 20) as buckets
            )])
            left join unnest([struct(
              range_bucket(!fieldname!, buckets) as bucket_ix
            )])
          group by partition_key, group_keys, bucket_ix
          order by bucket_ix
        )
        select partition_key, group_keys, array_agg(agg_item order by bucket_ix) as value
        from agg
        group by partition_key, group_keys
    )
    """
    , if(
        data_type in ('INT64', 'NUMERIC', 'BIGNUMERIC', 'FLOAT64')
        and not option_materialized_view_mode
        , replace(field_path, '.', '___')
        , null
      )
      , option_numeric_precision
      , option_numeric_precision
    )
    , '!fieldname!', field_path
    )
    , '\n'
  ), "")
  || if(
    any_value(option_materialized_view_mode)
    , "select * from restricted_view"
    , rtrim(format("""
      select
        core.partition_key
        , core.group_keys
        %s
      from restricted_view as core
      left join advanced_parts
        on advanced_parts.partition_key is not distinct from core.partition_key
          and advanced_parts.group_keys is not distinct from core.group_keys
      %s
      """
      , string_agg(
        replace(replace(replace(
          regexp_replace(
            template_selected.restricted || template_selected.advanced
            , r",.+\bas\b"
            , ", "
          )
          ,    '!column!', replace(field_path, '.', '___'))
          ,  '!fieldnum!', if(depth = 0, format('f%d',position), format('f%d_%d_%d', position, depth, subposition)))
          , '!fieldname!', field_path
        )
        , '\n' order by position, depth, subposition
      )
      , string_agg(
        replace(replace(replace(
          if(
            data_type in ('INT64', 'NUMERIC', 'BIGNUMERIC', 'FLOAT64')
            , """
              left join !column!__subprofiler
                on !column!__subprofiler.partition_key is not distinct from core.partition_key
                  and !column!__subprofiler.group_keys is not distinct from core.group_keys
            """
            , null
          )
          ,    '!column!', replace(field_path, '.', '___'))
          ,  '!fieldnum!', if(depth = 0, format('f%d',position), format('f%d_%d_%d', position, depth, subposition)))
          , '!fieldname!', field_path
        )
        , '\n' order by position, depth, subposition
      )
    ))
  )
  as query
  from table_columns, options
  left join `bqtest.INFORMATION_SCHEMA.VIEWS` as path
    using(table_catalog, table_schema, table_name)
  left join unnest([struct(
    struct(
      """
        -- !column! (!fieldnum!)
        , countif(!fieldname! is not null) as !column!__nonnull
        , approx_count_distinct(!fieldname!) as !column!__unique
        , hll_count.init(!fieldname!) as !column!__hll
        , sum(cast(!fieldname! as bignumeric)) as !column!__sum
        , sum(cast(!fieldname! as bignumeric) * cast(!fieldname! as bignumeric)) as !column!__sum2
        , avg(!fieldname!) as !column!__avg
        , min(!fieldname!) as !column!__min
        , max(!fieldname!) as !column!__max
      """ as restricted
      , """
        , approx_top_count(!fieldname!, 5) as !column!__top_count
        , approx_quantiles(!fieldname!, 20) as !column!__20quantile
      """ as advanced
    ) as number
    , struct(
      """
        -- !column! (!fieldnum!)
        , countif(!fieldname! is not null) as !column!__nonnull
        , sum(cast(!fieldname! as bignumeric)) as !column!__sum
        , avg(!fieldname!) as !column!__avg
        , min(!fieldname!) as !column!__min
        , max(!fieldname!) as !column!__max
      """ as restricted
      , """
        , approx_top_count(!fieldname!, 5) as !column!__top_count
        , approx_quantiles(!fieldname!, 20) as !column!__20quantile
      """ as advanced
    ) as float
    , struct(
      r"""
        -- !column! (!fieldnum!)
        , countif(!fieldname! is not null) as !column!__nonnull
        , approx_count_distinct(!fieldname!) as !column!__unique
        , hll_count.init(!fieldname!) as !column!__hll
        , avg(CHARACTER_LENGTH(!fieldname!)) as !column!__avg_len
        , min(CHARACTER_LENGTH(!fieldname!)) as !column!__min_len
        , max(CHARACTER_LENGTH(!fieldname!)) as !column!__max_len
      """ as restricted
      , """
        , approx_top_count(!fieldname!, 20) as !column!__top_count
        , approx_quantiles(!fieldname!, 20) as !column!__20quantile
      """ as advanced
    ) as string
    , struct(
      r"""
        , countif(!fieldname! is not null) as !column!__nonnull
        , hll_count.init(string(date(!fieldname!))) as !column!__day_hll
        , min(!fieldname!) as !column!__min
        , max(!fieldname!) as !column!__max
      """ as restricted
      , "" as advanced
    ) as timestamp
    , struct(
      r"""
        -- !column! (!fieldnum!)
        , countif(!fieldname! is not null) as !column!__nonnull
      """ as restricted
      , "" as advanced
    ) as anything
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

select any_value(`bqmake.v0.zdeindent`(ddl.query)) from ddl
where table_name = target_table_name
))
;

execute immediate format("with validateSQL as (%s) select 1", `bqtest.zbqt_gensql__table_profiler`("demo_sample_table", null, null));
execute immediate 'create materialized view `bqtest.mateview1` options(enable_refresh = false) as \n'
  || `bqtest.zbqt_gensql__table_profiler`(
    "demo_sample_table"
    , null
    , to_json(struct(true as materialized_view_mode))
  );
drop materialized view if exists `bqtest.mateview1`;


execute immediate format("with validateSQL as (%s) select 1", `bqtest.zbqt_gensql__table_profiler`("demo_sample_table", ["status"], null));
execute immediate 'create materialized view `bqtest.mateview` options(enable_refresh = false) as \n'
  || `bqtest.zbqt_gensql__table_profiler`(
    "demo_sample_table"
    , ["status"]
    , to_json(struct(true as materialized_view_mode))
  );
drop materialized view if exists `bqtest.mateview`;

execute immediate format("with validateSQL as (%s) select 1", `bqtest.zbqt_gensql__table_profiler`("demo_sample_partition_table", null, null));
execute immediate 'create materialized view `bqtest.mateview1` options(enable_refresh = false) as \n'
  || `bqtest.zbqt_gensql__table_profiler`(
    "demo_sample_partition_table"
    , null
    , to_json(struct(true as materialized_view_mode))
  );
drop materialized view if exists `bqtest.mateview1`;

execute immediate format("with validateSQL as (%s) select 1", `bqtest.zbqt_gensql__table_profiler`("demo_sample_view", [], null));
execute immediate 'create materialized view `bqtest.mateview1` options(enable_refresh = false) as \n'
  || `bqtest.zbqt_gensql__table_profiler`(
    "demo_sample_view"
    , null
    , to_json(struct(true as materialized_view_mode))
  );
drop materialized view if exists `bqtest.mateview1`;
