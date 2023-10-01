create or replace function `v0.zgensql__table_lineage`(
  project_id STRING
  , location STRING
  , information_schema_job_table_name STRING
)
options(description="""Generate SQL for lineage table data

This is private routine and not designed to be called directly.
"""
)
as (
  format(r"""
-- BigQuery Table Depdencies
-- depth = -1 means query destination tables in ordinal usage
with recursive lineage as (
  select
    format('%%s.%%s.%%s', dst_project, dst_dataset, dst_table) as destination
    , 0 as depth
    , relations.* except(unique_key)
    , cast([] as array<string>) as _ancestors
  from relations
  where unique_key not like '%%(User)%%'
    and not starts_with(dst_table, '_')
    and not starts_with(dst_dataset, '_script')
  union all
  select
    destination
    , depth + 1 as depth
    , lineage.src_project as dst_project
    , lineage.src_dataset as dst_dataset
    , lineage.src_table as dst_table
    , relations.* except(dst_project, dst_dataset, dst_table, unique_key)
    , lineage._ancestors || [_parent] as _ancestors
  from lineage
    join relations
      on (relations.dst_project, relations.dst_dataset, relations.dst_table)
       = (lineage.src_project, lineage.src_dataset, lineage.src_table)
    left join unnest([struct(
      format('%%T', (lineage.dst_project, lineage.dst_dataset, lineage.dst_table)) as _parent
      , format('%%T', (lineage.src_project, lineage.src_dataset, lineage.src_table)) as _self
    )])
  where
    depth <= @max_depth
    and ifnull(_self not in unnest(_ancestors), true)
)
, job as (
  select
    job_id
    , user_email
    , creation_time
    , end_time - start_time as processed_time
    , start_time - creation_time as wait_time
    , query
    , statement_type
    , total_bytes_processed
    , total_slot_ms
    , destination_table
    , referenced_tables
  from `%s`
  where
    destination_table.table_id is not null
    and error_result.reason is null
    and state = 'DONE'
    and creation_time between @begin and @end
)
, relations__impl as (
  select
    if(
      is_temporary and is_anonymous_query
      , format('(User) -> %%t', ref)
      , format('%%t <- %%t', destination_table, ref)
    ) as unique_key
    , any_value(destination_table).project_id as dst_project
    , any_value(destination_table).dataset_id as dst_dataset
    , any_value(normalized_dst_table) as dst_table
    , any_value(ref).project_id as src_project
    , any_value(ref).dataset_id as src_dataset
    , any_value(normalized_ref_table) as src_table
    , struct(
      max(creation_time) as job_latest
      , approx_top_sum(query, unix_seconds(creation_time), 10)[safe_offset(0)].value as query
      , approx_count_distinct(user_email) as n_user
      , approx_count_distinct(query) as n_queries
      , approx_count_distinct(job_id) as n_job
      , sum(total_bytes_processed) as total_bytes_procesed

      , approx_quantiles(processed_time_ms, 10) as total_time_processed__quantiles
      , approx_quantiles(wait_time_ms, 10) as wait_time__quantiles

      , sum(total_slot_ms) as total_slots_ms
      , approx_quantiles(total_slot_ms, 10) as total_slots_ms__quantiles
    ) as attrs
  from job, unnest(referenced_tables) as ref
    left join unnest([struct(
      coalesce(
        statement_type
        -- Interpolate statement_type because materialized view udpate by system is lacking statement_type
        , if(contains_substr(query, 'CALL'), 'CALL', null)
        , 'UNKNOWN'
      ) as statement_type
      , extract(millisecond from processed_time)
        + extract(second from processed_time) * 1000
        + extract(minute from processed_time) * 60 * 1000
        + extract(hour from processed_time) * 60 * 60 * 1000
      as processed_time_ms
      , extract(millisecond from wait_time)
        + extract(second from wait_time) * 1000
        + extract(minute from wait_time) * 60 * 1000
        + extract(hour from wait_time) * 60 * 60 * 1000
      as wait_time_ms
      , regexp_extract(ref.table_id, r'\d+$') as _src_suffix_number
      , regexp_extract(destination_table.table_id, r'\d+$') as _dst_suffix_number
      , starts_with(destination_table.dataset_id, '_') and char_length(destination_table.dataset_id) > 40 as is_temporary
      , starts_with(destination_table.table_id, 'anon') as is_anonymous_query
    )]) as v
    left join unnest([struct(
      if(safe.parse_date('%%Y%%m%%d', _src_suffix_number) is not null, regexp_replace(ref.table_id, r'\d+$', '*'), ref.table_id) as normalized_ref_table
      , if(safe.parse_date('%%Y%%m%%d', _dst_suffix_number) is not null, regexp_replace(destination_table.table_id, r'\d+$', '*'), destination_table.table_id) as normalized_dst_table
    )])
  where
    v.statement_type in (
      'ALTER_TABLE'
      , 'ALTER_VIEW'
      , 'ASSERT'
      , 'CREATE_CLONE_TABLE'
      , 'CREATE_MATERIALIZED_VIEW'
      , 'CREATE_MODEL'
      , 'CREATE_SNAPSHOT_TABLE'
      , 'CREATE_TABLE'
      , 'CREATE_TABLE_AS_SELECT'
      , 'CREATE_VIEW'
      , 'DELETE'
      , 'DROP_MATERIALIZED_VIEW'
      , 'DROP_TABLE'
      , 'DROP_VIEW'
      , 'EXPORT_DATA'
      , 'INSERT'
      , 'MERGE'
      , 'SELECT'
      , 'TRUNCATE'
      , 'UPDATE'
    )
    and v.statement_type is not null
  group by unique_key
)
, relations__examples as (
  select *
  from unnest(array<struct<
    unique_key string
    , dst_project string
    , dst_dataset string
    , dst_table string
    , src_project string
    , src_dataset string
    , src_table string
    , attrs struct<
      job_latest timestamp
      , query string
      , n_user int64
      , n_queries int64
      , n_job int64
      , total_processed_bytes int64
      , total_time_processed__quantiles array<int64>
      , wait_time__quantiles array<int64>
      , total_slots_ms int64
      , total_slots_ms__quantiles array<int64>
    >
  >>[
    /*
      1 <--- 2
         \-- 3 <-. 4
                 |
      7 <- 6 <-5 +
    */
    ('job_1', 'project_1', 'dataset_1', 'table_1', 'project_2', 'dataset_2', 'table_2', null)
    , ('job_2', 'project_1', 'dataset_1', 'table_1', 'project_3', 'dataset_3', 'table_3', null)
    , ('job_3', 'project_3', 'dataset_3', 'table_3', 'project_4', 'dataset_4', 'table_4', null)
    , ('job_4', 'project_5', 'dataset_5', 'table_5', 'project_4', 'dataset_4', 'table_4', null)
    , ('job_5', 'project_7', 'dataset_7', 'table_7', 'project_6', 'dataset_6', 'table_6', null)
    , ('job_6', 'project_6', 'dataset_6', 'table_6', 'project_5', 'dataset_5', 'table_5', null)
  ]) as data
)
, __check as (
  select * from relations__impl
  union all
  select * from relations__examples
  limit 0
)
, relations as (
  select * from `relations__impl`
  -- select * from `relations__examples`
)
, user_query as (
  select
    format('%%s.%%s.%%s', src_project, src_dataset, src_table) as destination
    , -1 as depth
    , dst_project  as dst_project
    , string(null) as dst_dataset
    , string(null) as dst_table
    , relations.* except(unique_key, dst_project, dst_dataset, dst_table)
  from relations
  where starts_with(unique_key, '(User)')
    and not starts_with(src_table, '_')
    and not starts_with(src_dataset, '_script')
  union all
  select * except(_ancestors) from lineage
)

select * from user_query
"""
  , coalesce(
    format('%s.%s.INFORMATION_SCHEMA.%s', project_id, ifnull(location, 'region-us'), ifnull(information_schema_job_table_name, 'JOBS_BY_PROJECT'))
    , format('%s.INFORMATION_SCHEMA.%s',  ifnull(location, 'region-us'), ifnull(information_schema_job_table_name, 'JOBS_BY_PROJECT'))
    , error(format("invalid arguments: %t", (project_id, location, information_schema_job_table_name)))
  ))
)
;
