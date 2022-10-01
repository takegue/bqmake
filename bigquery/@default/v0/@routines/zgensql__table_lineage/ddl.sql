create or replace function `bqmake.v0.zgensql__table_lineage`(
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
  from lineage
    join relations
      on (relations.dst_project, relations.dst_dataset, relations.dst_table)
       = (lineage.src_project, lineage.src_dataset, lineage.src_table)
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
, relations as (
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

    , max(creation_time) as job_latest
    , approx_top_sum(query, unix_seconds(creation_time), 10)[safe_offset(0)].value as query
    , approx_count_distinct(user_email) as n_user
    , approx_count_distinct(query) as n_queries
    , approx_count_distinct(job_id) as n_job
    , sum(total_bytes_processed) as total_bytes

    , approx_quantiles(processed_time_ms, 10) as processed_time__quantiles
    , approx_quantiles(wait_time_ms, 10) as wait_time__quantiles

    , sum(total_slot_ms) as total_slots_ms
    , approx_quantiles(total_slot_ms, 10) as total_slots_ms__quantiles

  from job, unnest(referenced_tables) as ref
    left join unnest([struct(
      extract(millisecond from processed_time)
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
      , destination_table = ref as is_self_reference
      , starts_with(destination_table.dataset_id, '_') and char_length(destination_table.dataset_id) > 40 as is_temporary
      , starts_with(destination_table.table_id, 'anon') as is_anonymous_query
    )])
    left join unnest([struct(
      if(safe.parse_date('%%Y%%m%%d', _src_suffix_number) is not null, regexp_replace(ref.table_id, r'\d+$', '*'), ref.table_id) as normalized_ref_table
      , if(safe.parse_date('%%Y%%m%%d', _dst_suffix_number) is not null, regexp_replace(destination_table.table_id, r'\d+$', '*'), destination_table.table_id) as normalized_dst_table
    )])
  where
    not is_self_reference
    and not statement_type in ('INSERT', 'DELETE', 'ALTER_TABLE', 'DROP_TABLE')
    and statement_type is not null
  group by unique_key
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
  select * from lineage
)

select * from user_query
order by destination, depth
"""
  , coalesce(
    format('%s.%s.%s.', project_id, ifnull(location, 'regin-us'), ifnull(information_schema_job_table_name, 'JOBS_BY_PROJECT'))
    , format('%s.%s.',  ifnull(location, 'regin-us'), ifnull(information_schema_job_table_name, 'JOBS_BY_PROJECT'))
    , error(format("invalid arguments: %t", (project_id, location, information_schema_job_table_name)))
  )
)

)
