create or replace procedure `v0.partition_table__update`(
  in destination struct<project_id string, dataset_id string, table_id string>,
  in sources array<struct<project_id string, dataset_id string, table_id string>>,
  in partition_alignments ARRAY<STRUCT<destination STRING, sources ARRAY<STRING>>>,
  in update_job_query STRING,
  in options JSON
)
options(description="""Procedure to check partition stalesns and update partitions if needed.

Arguments
====

- destination: The destination table to check and update partitions.
- sources: The source tables of destination table. The procedure will check if the source tables have new partitions.
- partition_alignments: Partition alignment rules. The procedure will check destination staleness correspoinding to each alignment rule.
- query: The query to update destination table partitions. Its table schema must be same as destination table.
- options: JSON value
    * dry_run: Whether to run the update job as a dry run. [Default: false].
    * tolerate_delay: The delay to tolerate before updating partitions. If newer source partitions are found but its timestamp is within this delay, the procedure will not update partitions. [Default: 0 minutes].
    * max_update_partition_range: The interval to limit the range of partitions to update. This option is useful to avoid updating too many partitions at once. [Default: 1 month].
    * via_temp_table: Whether to update partitions via a temporary table. [Default: false].
    * force_expired_at: The timestamp to force expire partitions. If the destination's partition timestamp is older than this timestamp, the procedure stale the partitions. [Default: null].
    * bq_location: BigQuery Location of job. This is used for query analysis to get dependencies. [Default: "region-us"]
    * backfill_direction: The direction to backfill partitions. [Default: "backward"]
    * auto_recreate: if target table schema change is detected, procedure recreate whole table [Default: "error_if_target_not_exists"]

Examples
===

- Check and update partitions of `my_project.my_dataset.my_table` table.

```
begin
  declare query string;
  declare _sources array<struct<project_id string, dataset_id string, table_id string>> default sources;

  set query = \"\"\"
    select date(timestamp_micros(event_timestamp)) as event_date, event_name, count(1)
    from `bigquery-public-data.ga4_obfuscated_sample_ecommerce.events_*`
    where parse_date('%Y%m%d', _TABLE_SUFFIX) between @begin and @end
    group by event_date, event_name
  \"\"\";

  create schema if not exists `zsandbox`;
  create or replace table `zsandbox.ga4_count`(event_date date, event_name string, records int64)
  partition by event_date;
  call `bqmake.v0.partition_table__update`(
    (null, 'zsandbox', 'ga4_count')
    , [('bigquery-public-data', 'ga4_obfuscated_sample_ecommerce', 'events_*')]
    , `bqmake.v0.alignment_day2day`('2021-01-01', '2021-01-01')
    , query
    , null
  );
end
```
"""
)
begin
  declare _sources array<struct<project_id string, dataset_id string, table_id string>> default sources;
  declare stale_partitions array<string>;
  declare partition_range struct<begin string, `end` string>;
  declare partition_column struct<name string, type string>;
  declare partition_unit string;
  declare table_identifier string;

  -- Options
  declare _options struct<
    dry_run BOOL,
    tolerate_delay INTERVAL,
    max_update_partition_range INTERVAL,
    via_temp_table BOOL,
    force_expired_at timestamp,
    bq_location string,
    backfill_direction int64,
    auto_recreate string
  > default (
    ifnull(bool(options.dry_run), false)
    , ifnull(safe_cast(string(options.tolerate_delay) as interval), interval 0 minute)
    , ifnull(safe_cast(string(options.max_update_partition_range) as interval), interval 1 month)
    , ifnull(bool(options.via_temp_table), false)
    , timestamp(string(options.force_expired_at))
    , ifnull(string(options.bq_location), "region-us")
    , case ifnull(string(options.backfill_direction), "backward")
      when "backward" then 1
      when "forward" then -1
      else  error(format("Invalid backfill_direction: %p", options.backfill_direction))
    end
    , ifnull(string(options.auto_recreate), "error_if_not_exists")
  );

  -- Assert invalid options
  select logical_and(if(
    key in ('dry_run', 'tolerate_delay', 'max_update_partition_range', 'via_temp_table', 'force_expired_at', 'backfill_direction',  'auto_recreate')
    , true
    , error(format("Invalid Option: name=%t in %t'", key, `options`))
  ))
  from unnest(if(`options` is not null, `bqutil.fn.json_extract_keys`(to_json_string(`options`)), [])) as key
  ;

  -- Automatic source tables detection
  if _sources is null then
    call `v0.analyze_query_referenced_tables`(
      _sources, update_job_query, to_json(struct(options.bq_location as default_region))
    );
  end if;

  call `v0.detect_staleness`(
    stale_partitions
    , destination
    , _sources
    , partition_alignments
    , to_json(struct(_options.tolerate_delay, _options.force_expired_at))
  );

  if ifnull(array_length(stale_partitions), 0) = 0 then
    return;
  end if;

  set partition_range = (
    -- Extract first successive partition_range with
    with gap as (
      select
        p
        , ifnull(
          coalesce(
            datetime_diff(lag(partition_hour) over (order by partition_hour desc), partition_hour, hour) > 1
            , date_diff(lag(partition_date) over (order by partition_date desc), partition_date, day) > 1
            , (lag(partition_int) over (order by partition_int desc) - partition_int) > 1
          )
          -- null or __NULL__
          , true
        ) as has_gap
        , update_partition
      from unnest(stale_partitions) p
      left join unnest([struct(
        safe.parse_date('%Y%m%d', p) as partition_date
        , safe.parse_datetime('%Y%m%d%h', p) as partition_hour
        , safe_cast(p as int64) as partition_int
      )])
      left join unnest([struct(
        coalesce(
          format_date('%Y%m%d', date(partition_date - _options.max_update_partition_range))
          , format_datetime('%Y%m%d%h', partition_hour - _options.max_update_partition_range)
          -- FIXME
          , cast(partition_int as string)
        ) as update_partition
      )])
    )
    , first_successive_partitions as (
      select *, if(has_gap, update_partition, null) as max_update_partition from gap
      qualify sum(if(has_gap, 1, 0)) over (
        order by sign(_options.backfill_direction) * safe_cast(p as int64) desc
        ) = 1
    )
    select as struct
      -- if _options.max_update_partition_range is null, then use non-limited partition
      ifnull(greatest(min(p), max(max_update_partition)) , min(p))
      , max(p)
    from first_successive_partitions
  );

  -- Get partition column
  call `v0.partition_table__get_partition_column`(partition_column, destination);

  -- Run Update Job
  if _options.dry_run then
    select
      format('%P', to_json(struct(
        destination
        , _sources
        , _options
        , partition_range
    )))
    ;
    return;
  end if;

  -- Format parition_id into datetime or date parsable string like '2022-01-01'
  set (partition_unit, partition_range) = (
    case
      when safe.parse_datetime('%Y%m%d%H', partition_range.begin) is not null then 'HOUR'
      when safe.parse_datetime('%Y%m%d', partition_range.begin) is not null then 'DAY'
      when safe.parse_datetime('%Y%m', partition_range.begin) is not null then 'MONTH'
      when safe.parse_datetime('%Y', partition_range.begin) is not null then 'YEAR'
      when partition_range.begin = '__NULL__' then 'NO_PARTITION'
      else error(format('Invalid partition_id: %s', partition_range.begin))
    end
    , (
      coalesce(
        format_datetime('%Y-%m-%d %T', safe.parse_datetime('%Y%m%d%H', partition_range.begin))
        , format_datetime('%Y-%m-%d', safe.parse_datetime('%Y%m%d', partition_range.begin))
        , format_datetime('%Y-%m-%d', safe.parse_datetime('%Y%m', partition_range.begin))
        , format_datetime('%Y-%m-%d', safe.parse_datetime('%Y', partition_range.begin))
        , null
      )
      , coalesce(
        format_datetime('%Y-%m-%d %T', safe.parse_datetime('%Y%m%d%H', partition_range.`end`))
        , format_datetime('%Y-%m-%d', safe.parse_datetime('%Y%m%d', partition_range.`end`))
        , format_datetime('%Y-%m-%d', safe.parse_datetime('%Y%m', partition_range.`end`))
        , format_datetime('%Y-%m-%d', safe.parse_datetime('%Y', partition_range.`end`))
        , null
      )
    )
  );

  set table_identifier = coalesce(format(
      '%s.%s.%s'
      , ifnull(destination.project_id, @@project_id)
      , destination.dataset_id
      , destination.table_id
  ), error("Invalid destination"));

  -- Assert or recreate destination table
  case _options.auto_recreate
    when 'error_if_not_exists' THEN
      execute immediate format("select * from `%s` limit 0", table_identifier);
    when 'replace_if_changed' THEN
      begin
        -- Zero scan amount table schema compatibility check
        execute immediate format(
          "select * from `%s` union all %s limit 0", table_identifier, update_job_query
        ) using
          partition_range.`begin` as `begin`
          , partition_range.`end` as `end`
        ;
      exception when error then
        begin 
          declare table_ddl, column_ddl string;
          execute immediate `v0.zgensql__table_recreation`(
            (coalesce(destination.project_id, @@project_id), destination.dataset_id, destination.table_id)
            , update_job_query || ' limit 0'
          ) into table_ddl, column_ddl;
          execute immediate table_ddl 
            using
              partition_range.`begin` as `begin`
              , partition_range.`end` as `end`
          ;
          execute immediate column_ddl;
        end;
      end
    ;
    else 
      select error('Invalid option');
  end case;

  -- Build new data and merge
  if ifnull(_options.via_temp_table, false) then
    execute immediate format("""
      create or replace temp table temp_table as
        %s
      """
      , update_job_query
    ) using
      partition_range.begin as begin
      , partition_range.`end` as `end`
    ;
  end if;

  execute immediate ifnull(format("""
    merge into `%s` as T
      using (%s) as S
        on false
    when not matched by target
      then
        insert row
    when not matched by source
      -- partition filter
      and %s
      then
        delete
    """
      -- Destination
      , table_identifier
      , if(ifnull(_options.via_temp_table, false), 'temp_table', update_job_query)
      , case
          when partition_unit = 'DAY' then
            format(
              "DATE(%s) between @begin and @end"
              , partition_column.name
            )
          when partition_unit in ('HOUR', 'MONTH', 'YEAR') then
            format(
              "%s_TRUNC(%s, %s) between @begin and @end"
              , partition_column.type, partition_column.name, partition_unit
            )
          else 'true'
        end
    )
    , error(format(
      "arguments are invalid: %T", (destination, table_identifier, partition_column, partition_range)
    ))
  )
    using
      partition_range.begin as begin
      , partition_range.`end` as `end`
  ;

  if @@row_count = 0 then
    raise using message = format('No data `end` update: %t', (update_job_query, partition_range));
  end if;
end;