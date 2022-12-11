create or replace function `bqtest.zbqt_gensql__dataset_spec`(
  _table_name string
  , test_configs array<struct<
    cte string
    , unique_columns array<string>
    , nonnull_columns array<string>
    , accepted_values_columns array<struct<column string, accepcted_values array<string>>>
  >>
)
returns string
as ((
  select as value format(`bqmake.v0.zreindent`(r"""
    with datasource as (
      select
      normalized_table_name as table_name
      , storage_tier
      , format_date('%%Y%%m%%d', partition_datetime) as partition_id
      , datetime(last_modified_time) - partition_datetime as partition__creation_delay
      , timestamp(partition_datetime + tolerate_delay, %s) as expected_time_to_create_until
      , format_datetime('%%Y%%m%%d', current_timestamp() - tolerate_delay, %s) as exexpected_latest_partition_id
      , total_rows
      , total_logical_bytes
      , total_billable_bytes
      from `bqtest.INFORMATION_SCHEMA.PARTITIONS`
      left join unnest([struct(
          if(
            safe.parse_datetime('%%Y%%m%%d', partition_id) is not null
            , struct("DAY" as type, safe.parse_datetime('%%Y%%m%%d', partition_id) as datetime)
            , null
            ) as partition_info
            , current_timestamp() - last_modified_time as partition__active_time
            , coalesce(
              parse_datetime('%%Y%%m%%d', regexp_extract(table_name, r'\d+$'))
            ) as sharding_info
        )])
      left join unnest([struct(
          if(sharding_info is not null, regexp_replace(table_name, r'\d+$', '*'), table_name) as normalized_table_name,
          coalesce(partition_info.datetime, sharding_info) as partition_datetime
        )])
      where
      -- Sharding table filter for INFORMATION_SCHEMA's Runtime error
      ifnull(
        parse_datetime('%%Y%%m%%d', regexp_extract(table_name, r'\d+$')) > current_datetime() - interval 365 day
        , true
      )
      -- group by table_name
      order by partition_id desc
    )

    , spec__freshness as (
      select
        table_name
        , max(partition_id) = format_datetime('%Y%m%d', current_timestamp() - tolerate_delay, timezone) as expected_freshness1
        , current_timestamp() - max(last_modified_time) < tolerate_fressh_interval as expected_freshness2
      from datasource
      group by table_name
    )

    select * from spec__freshness
""", 0)
    , "Asia/Tokyo"
)))
;

begin
  execute immediate `bqtest.zbqt_gensql__table_spec`(
    "demo_sample_table"
    , [
        (string(null), ["unique_key"], if(false, [''], []), if(false, [('', [''])], []))
      ]
  );
  execute immediate `bqtest.zbqt_gensql__table_spec`(
    "demo_sample_view"
    , [
        (string(null), ["unique_key"], if(false, [''], []), if(false, [('', [''])], []))
      ]
  );
end;
