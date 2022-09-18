create or replace procedure `v0.dataset__update_table_labels`(
  in destination struct<project string, dataset string>
)
begin
  declare dst_ref string default format('%s.%s', coalesce(destination.project, @@project_id), destination.dataset);
  declare _deps array<struct<table_catalog string, table_schema string, table_name string>>;
  declare _labels array<struct<key string, value string>>;

  execute immediate format("""
    create or replace temp table `tmp_partitions`
    as
      select * from `%s.INFORMATION_SCHEMA.PARTITIONS`
  """
    , dst_ref
  );

  execute immediate format("""
    create or replace temp table `tmp_table_options`
    as
      with labels as (
        select * from `%s.INFORMATION_SCHEMA.TABLE_OPTIONS` where option_name = 'labels'
      )
      select
        table_catalog, table_schema, table_name
        , table_type
        , ifnull(`bqmake.v0.get_bqlabel_from_option`(option_value), []) as labels
      from `%s.INFORMATION_SCHEMA.TABLES`
      left join labels using(table_catalog, table_schema, table_name)
    """
    , dst_ref
    , dst_ref
  );

  create or replace temp table `table_labels`
  as
    with datasource as (
      select
        src.*,
        ifnull(
            coalesce(
              datetime_diff(lag(partition_hour) over w_partition, partition_hour, hour) > 1
              , date_diff(lag(partition_date) over w_partition, partition_date, day) > 1
              , (lag(partition_int) over w_partition - partition_int) > 1
            )
            -- null or __NULL__
            , true
          ) as has_gap
      from tmp_partitions as src
      left join unnest([struct(
        safe.parse_date('%Y%m%d', partition_id) as partition_date
        , safe.parse_datetime('%Y%m%d%h', partition_id) as partition_hour
        , safe_cast(partition_id as int64) as partition_int
        )])
      window w_partition as (partition by table_catalog, table_schema, table_name order by partition_id desc)
    )
    , new_labels as (
      select
        table_catalog, table_schema, table_name
        , [
          struct("partition-min" as key, min(partition_id) as value)
          , ("partition-max", max(partition_id))
          , ("partition-skip", cast(countif(has_gap) - 1 as string))
        ] as labels
      from datasource
      group by table_catalog, table_schema, table_name
    )
    , update_labels as (
      select
        table_catalog, table_schema, table_name, table_type
        , array(
          select as struct
            key, any_value(coalesce(`new`.value, old.value)) as value
          from unnest(current_labels.labels) as old
          full join (select * from unnest(new_labels.labels)) as `new` using(key)
          group by key
        ) as labels
      from new_labels
      full join tmp_table_options as current_labels using(table_catalog, table_schema, table_name)
    )
  select * from update_labels
  ;

  for record in (select * from table_labels)
  do
    if record.table_type in ("VIEW", "MATERIALIZED VIEW") then
      call `v0.scan_query_referenced_tables`(
        _deps
        , format("select * from `%s.%s.%s`", record.table_catalog, record.table_schema, record.table_name)
        , to_json(struct(true as enable_query_rewrite))
      );
      set _labels = array(
        with newlabels as (
          select
            key,
            case key
              when 'partition-min' then max(value)
              when 'partition-max' then min(value)
              when 'partition-skip' then max(value)
              else null
            end as value
          from unnest(_deps) dep
          left join table_labels using(table_catalog, table_schema, table_name)
          left join unnest(table_labels.labels)
          group by key
          having value is not null
        )

        select as struct
          key
          , coalesce(`new`.value, old.value) as value
        from unnest(record.labels) as old
        full join newlabels as `new` using(key)
      );
    elseif record.table_type in ("BASE TABLE") then
      set _labels = record.labels;
    else
      continue;
    end if;

    execute immediate format("""
      alter %s `%s.%s.%s`
      set options (labels=@labels);
    """
      , case record.table_type
          when 'BASE TABLE' then 'table'
          when 'VIEW' then 'view'
          when 'MATERIALIZED VIEW' then 'materialized view'
          else error(format("Invalid table_type: %t %t", record.table_name, record.table_type))
        end
      , record.table_catalog
      , record.table_schema
      , record.table_name
    )
      using _labels as labels
    ;
  end for;
end;

-- Unit test
begin
  create schema if not exists `zvalidaiton__dataset__update_labels`;

  create or replace table `zvalidaiton__dataset__update_labels.table1`
  partition by date_jst
  as
  select date '2006-01-02' as date_jst
  ;

  create or replace view `zvalidaiton__dataset__update_labels.view1`
  as
  select * from `zvalidaiton__dataset__update_labels.table1`
  ;

  call `v0.dataset__update_table_labels`((null, 'zvalidaiton__dataset__update_labels'))
  ;

  with expected as (
    select
      'labels' as option_name
      , *
    from unnest([
      struct('table1' as table_name, 'partition-min' as key, '20060102' as value)
      , struct('table1' as table_name, 'partition-max' as key, '20060102' as value)
      , struct('table1' as table_name, 'partition-skip' as key, '0' as value)
      , struct('view1' as table_name, 'partition-min' as key, '20060102' as value)
      , struct('view1' as table_name, 'partition-max' as key, '20060102' as value)
      , struct('view1' as table_name, 'partition-skip' as key, '0' as value)
    ])
  )
  , validation as (
    select
      table_name, key
      , expected.value = actual.value as assert
      , format('%t: Expected %t but actual is %t', (table_name, key), expected.value, actual.value) as err
    from expected
    left join `zvalidaiton__dataset__update_labels.INFORMATION_SCHEMA.TABLE_OPTIONS` using(table_name, option_name)
    left join unnest(`v0.get_bqlabel_from_option`(option_value)) as actual using(key)
  )
  , report as (
    select as value
      string_agg(err, ', ')
    from validation
    where not assert
  )

  select
    error(ifnull(report, 'unrechable'))
  from report
  where report is not null
  ;

  drop schema if exists `zvalidaiton__dataset__update_labels` CASCADE;
end