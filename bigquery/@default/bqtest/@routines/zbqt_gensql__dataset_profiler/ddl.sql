CREATE OR REPLACE FUNCTION `bqtest.zbqt_gensql__dataset_profiler`() RETURNS STRING
AS (
(
  select as value `bqmake.v0.zreindent`(
    r"""
    with datasource as (
      select
      normalized_table_name as table_name
      , storage_tier
      , format_date('%Y%m%d', partition_datetime) as partition_id
      , datetime(last_modified_time) - partition_datetime as partition__creation_delay
      , last_modified_time
      , total_rows
      , total_logical_bytes
      , total_billable_bytes
      from `bqtest.INFORMATION_SCHEMA.PARTITIONS`
      left join unnest([struct(
          if(
            safe.parse_datetime('%Y%m%d', partition_id) is not null
            , struct("DAY" as type, safe.parse_datetime('%Y%m%d', partition_id) as datetime)
            , null
            ) as partition_info
            , current_timestamp() - last_modified_time as partition__active_time
            , coalesce(
              parse_datetime('%Y%m%d', regexp_extract(table_name, r'\d+$'))
            ) as sharding_info
        )])
      left join unnest([struct(
          if(sharding_info is not null, regexp_replace(table_name, r'\d+$', '*'), table_name) as normalized_table_name,
            coalesce(partition_info.datetime, sharding_info) as partition_datetime
        )])
      where
      -- Sharding table filter for INFORMATION_SCHEMA's Runtime error
      ifnull(
        parse_datetime('%Y%m%d', regexp_extract(table_name, r'\d+$')) > current_datetime() - interval 365 day
        , true
      )
    )
    select * from datasource
    """, 0)
)
);