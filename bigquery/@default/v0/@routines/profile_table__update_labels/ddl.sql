
create or replace procedure `v0.profile_table__update_labels`(
  in destination struct<project string, dataset string>
)
begin
  declare dst_ref string default format('%s.%s', coalesce(destination.project, @@project_id), destination.dataset);
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
      select
        table_catalog, table_schema, table_name
        , table_type
        , ifnull(`bqmake.v0.get_bqlabel_from_option_value`(option_value), []) as labels
      from `%s.INFORMATION_SCHEMA.TABLES`
      join `%s.INFORMATION_SCHEMA.TABLE_OPTIONS` using(table_catalog, table_schema, table_name)
      where option_name = 'labels'
    """
    , dst_ref
    , dst_ref
  );

  for record in (
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
          table_catalog, table_schema, table_name
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
    )
  do
    execute immediate format("""
      alter %s `%s.%s.%s`
      set options (labels=@labels);
    """
      , case table_type
          when 'BASE TABLE' then 'table'
          when 'VIEW' then 'view'
          when 'MATERIALIZED VIEW' then 'materialized view'
          else error(format("Invalid table_type: %t", table_type))
        end
      , record.table_catalog
      , record.table_schema
      , record.table_name
    )
      using record.labels as labels
    ;
  end for;
end;

declare labels array<struct<key string, value string>>;
call `v0.profile_table__update_labels`((null, 'zpreview_test'))
