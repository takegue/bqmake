CREATE OR REPLACE PROCEDURE `v0.partition_table__check_and_update`(destination STRUCT<project_id STRING, dataset_id STRING, table_id STRING>, sources ARRAY<STRUCT<project_id STRING, dataset_id STRING, table_id STRING>>, partition_alignments ARRAY<STRUCT<destination STRING, sources ARRAY<STRING>>>, update_job STRUCT<query STRING, dry_run BOOL, tolerate_delay INTERVAL, max_update_interval INT64, via_temp_table BOOL>)
begin
  declare stale_partitions array<string>;
  declare partition_range struct<begins_at string, ends_at string>;
  declare partition_column struct<name string, type string>;
  declare partition_unit string;

  call `v0.partition_table__check_staleness`(
    stale_partitions
    , destination
    , sources
    , partition_alignments
    , to_json(struct(update_job.tolerate_delay))
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
          format_date('%Y%m%d', date(partition_date - interval update_job.max_update_interval day))
          , format_datetime('%Y%m%d%h', partition_hour - interval update_job.max_update_interval hour)
          , cast(partition_int - update_job.max_update_interval as string)
        ) as update_partition
      )])
    )
    , first_successive_partitions as (
      select *, if(has_gap, update_partition, null) as max_update_partition from gap
      qualify sum(if(has_gap, 1, 0)) over (order by p desc) = 1
    )
    select as struct
      -- if update_job.max_update_interval is null, then use non-limited partition
      ifnull(greatest(min(p), max(max_update_partition)) , min(p))
      , max(p)
    from first_successive_partitions
  );
  -- Get partition column
  call `fn.get_table_partition_column`(destination, partition_column);

  -- Run Update Job
  if update_job.dry_run then
    select
      format('%P', to_json(struct(
        destination
        , sources
        , update_job
        , partition_range
    )))
    ;
    return;
  end if;

  -- Format parition_id into datetime or date parsable string like '2022-01-01'
  set (partition_unit, partition_range) = (
    case
      when safe.parse_datetime('%Y%m%d%H', partition_range.begins_at) is not null then 'HOUR'
      when safe.parse_datetime('%Y%m%d', partition_range.begins_at) is not null then 'DAY'
      when safe.parse_datetime('%Y%m', partition_range.begins_at) is not null then 'MONTH'
      when safe.parse_datetime('%Y', partition_range.begins_at) is not null then 'YEAR'
      else error(format('Invalid partition_id: %s', partition_range.begins_at))
    end
    , (
      coalesce(
        format_datetime('%Y-%m-%d %T', safe.parse_datetime('%Y%m%d%H', partition_range.begins_at))
        , format_datetime('%Y-%m-%d', safe.parse_datetime('%Y%m%d', partition_range.begins_at))
        , format_datetime('%Y-%m-%d', safe.parse_datetime('%Y%m', partition_range.begins_at))
        , format_datetime('%Y-%m-%d', safe.parse_datetime('%Y', partition_range.begins_at))
      )
      , coalesce(
        format_datetime('%Y-%m-%d %T', safe.parse_datetime('%Y%m%d%H', partition_range.ends_at))
        , format_datetime('%Y-%m-%d', safe.parse_datetime('%Y%m%d', partition_range.ends_at))
        , format_datetime('%Y-%m-%d', safe.parse_datetime('%Y%m', partition_range.ends_at))
        , format_datetime('%Y-%m-%d', safe.parse_datetime('%Y', partition_range.ends_at))
      )
    )
  );

  if ifnull(update_job.via_temp_table, false) then
    execute immediate format("""
      create or replace temp table temp_table as
        %s
      """
      , update_job.query
    ) using
      partition_range.begins_at as begins_at
      , partition_range.ends_at as ends_at
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
      , ifnull(format(
          '%s.%s.%s'
          , ifnull(destination.project_id, @@project_id)
          , destination.dataset_id
          , destination.table_id
        ), 'invalid destination'
      )
      , if(ifnull(update_job.via_temp_table, false), 'temp_table', update_job.query)
      , case
          when partition_unit = 'DAY' then
            format(
              "DATE(%s) between @begins_at and @ends_at"
              , partition_column.name
            )
          when partition_unit in ('HOUR', 'MONTH', 'YEAR') then
            format(
              "%s_TRUNC(%s, %s) between @begins_at and @ends_at"
              , partition_column.type, partition_column.name, partition_unit
            )
          else 'true'
        end
    )
    , error(format(
      "arguments is invalud: %T", (destination, partition_column.name, partition_range)
    ))
  )
    using
      partition_range.begins_at as begins_at
      , partition_range.ends_at as ends_at
  ;

  if @@row_count = 0 then
    raise using message = format('No data to update: %%', (update_job.query, partition_range));
  end if;
end;