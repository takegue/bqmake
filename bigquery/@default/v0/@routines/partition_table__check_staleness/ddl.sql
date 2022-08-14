CREATE OR REPLACE PROCEDURE bqmake.v0.partition_table__check_staleness(OUT ret ARRAY<STRING>, destination STRUCT<project_id STRING, dataset_id STRING, table_id STRING>, sources ARRAY<STRUCT<project_id STRING, dataset_id STRING, table_id STRING>>, partition_alignments ARRAY<STRUCT<destination STRING, sources ARRAY<STRING>>>, options_json JSON)
OPTIONS(
  description="Extracts partitions that are stale.\n\nArgument\n===\n\n- ret                  : Output variable\n- destination          : destination table\n- sources              : source tables\n- partition_alignments : partition alignments\n- options              : option values in json format\n  * tolerate_staleness : if the partition is older than this value (Default: interval 0 minute)\n  *         null_value : Alignment options. __NULL__ meaning no partition (Default: '__NULL__')\n\n\nStalenss and Stablity Margin Checks\n===\n\n                     past                                      now\nSource Table        : |       |         |                       |\n                              ^ Refresh ^ Refresh\n\nStaleness Timeline  : | Fresh | Ignore  |  Ignore    |   Stale  |\n                                        <------------^ tolerate staleness\n\n\n")
begin
  declare options struct<
    tolerate_staleness interval
    , null_value string
  > default (
    ifnull(cast(string(options_json.tolerate_staleness) as interval), interval 0 minute)
    , ifnull(string(options_json.null_value), '__NULL__')
  );

  -- Prepare metadata from  INFOMARTION_SCHEMA.PARTITIONS
  execute immediate (
    select as value
      "create or replace temp table `_partitions_temp` as "
      || string_agg(
        format("""
          select
            '%s' as label
            , '%s' as argument
            , *
          from `%s.%s.INFORMATION_SCHEMA.PARTITIONS`
          where %s
          """
          , label
          , target.table_id
          , ifnull(target.project_id, @@project_id), target.dataset_id
          , format(if(
            contains_substr(target.table_id, '*')
            , 'starts_with(table_name, replace("%s", "*", ""))'
            , 'table_name = "%s"'
            )
            , target.table_id)
        )
        , '\nunion all'
      )
    from unnest([
      struct('destination' as label, destination as target)
    ] || array(select as struct 'source', s from unnest(sources) s)
    )
  )
  ;

  -- alignment and extract staled partition
  set ret = (
    -- partition_id -> (null, null) -> ('__NULL__', '__NULL__')
    -- partition_id -> (null, date) -> (_pseudo_date, date)
    -- partition_id -> (date, null) -> (date, _pseudo_date)
    -- partition_id -> (date, date) -> (date, date)

    with
    pseudo_partition as (
      SELECT
        label
        , coalesce(
            partition_id
            , if(has_wildcard, regexp_replace(table_name, format('^%s', pattern), ''), null)
            , format_date('%Y%m%d', _pseudo_date)
            , options.null_value
          )
          as partition_id
        , struct(partition_id, table_catalog, table_schema, table_name, last_modified_time)
          as alignment_paylod
      from _partitions_temp
      left join unnest([struct(
        contains_substr(argument, '*') as has_wildcard
        , regexp_replace(argument, r'\*$', '') as pattern
      )])
      left join unnest(
        if(
          partition_id is not null or has_wildcard
          , []
          , (
            select as value
              generate_date_array(
                min(safe.parse_date('%Y%m%d', least(d, s)))
                , max(safe.parse_date('%Y%m%d', greatest(d, s)))
              )
            from unnest(partition_alignments) a
            left join unnest(a.sources) src
            left join unnest([struct(
              nullif(a.destination, options.null_value) as d
              , nullif(src, options.null_value) as s
            )])
          )
        )
      ) as _pseudo_date
      where
        table_name = argument or starts_with(table_name, pattern)
    )
    , argument_alignment as (
      select a.destination as partition_id, array_length(a.sources) as n_sources, source as source_partition_id
      from unnest(partition_alignments) a, unnest(a.sources) as source
    )
    , aligned as (
      select
        struct(
          _v.partition_id
          , destination.alignment_paylod.last_modified_time
        ) as destination
        , source.alignment_paylod as source
        , -- # of source kind * # of source partition
        array_length(sources) * n_sources
          = countif(source.partition_id is not null) over (partition by _v.partition_id)
          as is_ready_every_sources
      from
        argument_alignment
      left join
        (select * from pseudo_partition where label = 'destination') as destination
        using(partition_id)
      left join
        (select * from pseudo_partition where label = 'source') as source
        on source_partition_id = source.partition_id
      left join unnest([struct(
        coalesce(destination.partition_id, argument_alignment.partition_id) as partition_id
      )]) as _v
    )

    select
      array_agg(distinct partition_id order by partition_id)
    from aligned
    left join unnest([ifnull(destination.partition_id, options.null_value)]) as partition_id
    where
      is_ready_every_sources
      and (
        -- Staled if destination partition does not exist
        destination.last_modified_time is null
        -- Staled destination partition only if source partition is enough stable and old
        or (
          destination.last_modified_time - source.last_modified_time >= options.tolerate_staleness
          or (
            source.last_modified_time >= destination.last_modified_time
            and current_timestamp() - source.last_modified_time >= options.tolerate_staleness
          )
        )
      )
  );
end;