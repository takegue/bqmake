CREATE OR REPLACE FUNCTION `v0.zgensql__clineage__analyze`(audit_data STRUCT<project_id STRING, dataset_id STRING, table_name STRING>, capture_interval INTERVAL)
AS (
replace(replace(r"""
    with datasource as (
      select
        metadata.`@type` as type
        , c.timestamp
        , resource
        , protopayload_auditlog.servicedata_v1_bigquery.jobCompletedEvent
        , tableDataRead
        , c as _raw
      from !AUDIT_TABLEDATA! as c
      left join unnest([struct(
        safe.parse_json(protopayload_auditlog.metadataJson) as metadata
        , protopayload_auditlog.servicedata_v1_bigquery.jobCompletedEvent
      )])
      left join unnest([struct(
        struct(
          regexp_extract(string(metadata.tableDataRead.jobName), '[^/]+$') as jobId
          , json_value_array(nullif(to_json_string(metadata.tableDataRead.fields), 'null')) as fields
        ) as tableDataRead
      )])
      where
        timestamp >= current_timestamp() - !CAPTURE_INTERVAL!
        and 
        (
          (
            -- jobCompleteEvent master
            jobCompletedEvent.eventName = 'query_job_completed'
            and contains_substr(jobCompletedEvent.job.jobConfiguration.labels, 'clineage')
          )
          OR (
            -- tableDataRead event
            tableDataRead.jobId is not null
          )
        )
    )
    , tableDataRead as (
      select 
        tableDataRead.jobId
        , resource.labels.dataset_id
        , resource.labels.project_id
        , tableDataRead.fields
        , to_json(datasource._raw) as _raw
      from datasource
    )
    , jobCompleteEvent as (
      select
        timestamp
        , jobCompletedEvent.job.jobName.jobId as jobId
        , struct(
          jobCompletedEvent.job.jobConfiguration as config
          , jobCompletedEvent.job.jobStatistics as statistics
        ) as job
        , to_json(jobCompletedEvent) as _raw
      from datasource
      where 
        jobCompletedEvent.eventName = 'query_job_completed'
        and contains_substr(jobCompletedEvent.job.jobConfiguration.labels, 'clineage')
    )
    , fmt as (
      select 
        any_value(struct(
          vhash, clienage__resource
        )).*
        , array_agg(
          struct(
            field_path as column
            , struct(
              tableDataRead.project_id
              , tableDataRead.dataset_id
              , tableDataRead.fields
            ) as lineage
            , struct(
              job.statistics.totalSlotMs
              , job.statistics.totalProcessedBytes
              , job.statistics.endTime - job.statistics.startTime as leadTime
            ) as stats
          )
          order by field_index
        ) as column_lineage
        , struct(
          min(timestamp) as min
          , max(timestamp) as max
        ) as analyze_span
    --    , job.statistics
      from jobCompleteEvent
      left join tableDataRead using(jobId)
      left join unnest([struct(
        struct(
          `bqutil.fn.get_value`('clineage__catalog',  job.config.labels) as catalog
          , `bqutil.fn.get_value`('clineage__schema',  job.config.labels) as schema
          , `bqutil.fn.get_value`('clineage__table',  job.config.labels) as table
        ) as clienage__resource
        , `bqutil.fn.get_value`('clineage__field_index',  job.config.labels) as field_index
        , replace(`bqutil.fn.get_value`('clineage__field_path',  job.config.labels), '_-_', '.') as field_path
        , `bqutil.fn.get_value`('clineage__vhash',  job.config.labels) as vhash
      )])
      group by format('%t', (clienage__resource, vhash))
      qualify 1 = row_number() over (
        partition by format('%t', clienage__resource)
        order by analyze_span.min desc
      )
    )

    select * from fmt
  """
    , '!AUDIT_TABLEDATA!', format('`%s.%s.%s`', audit_data.project_id, audit_data.dataset_id, audit_data.table_name))
    , '!CAPTURE_INTERVAL!', format('%T', capture_interval)
  )
);