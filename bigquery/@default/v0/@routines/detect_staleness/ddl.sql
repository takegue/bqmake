CREATE OR REPLACE PROCEDURE `v0.detect_staleness`(
  out ret array<string>
  , destination struct<project_id string, dataset_id string, table_id string>
  , sources array<struct<project_id string, dataset_id string, table_id string>>
  , partition_alignments array<struct<destination string, sources array<string>>>
  , options_json json
)
OPTIONS(
  description="""Extracts partitions that are stale.

Argument
===

- ret                  : Output variable. array of staled partition like 20220101
- destination          : destination table
- sources              : source tables
- partition_alignments : partition alignments
- options              : option values in json format
  * tolerate_staleness : if the partition is older than this value (Default: interval 0 minute)
  *    force_expire_at : The timestamp to force expire partitions. If the destination's partition timestamp is older than this timestamp, the procedure stale the partitions. [Default: NULL].


Stalenss and Stablity Margin Checks
===

Case 1: Partition staleness with tolerate_staleness option
                     past                              now
Source Table        : |       |               |         |
                              ^ Refresh
Staleness Timeline  : | Fresh | Ignore(Fresh) |  Stale  |
                      +-----------------------^ tolerate staleness


Case 2: Partition staleness timeline with force_expire_at option
                     past                              now
Source Table        : | Fresh                           |
Staleness Timeline  : | Fresh | Stale                   |
                              ^ force_expire_at
""")
begin
  declare null_value string default '__NULL__';
  declare any_value string default '__ANY__';
  declare options struct<
    tolerate_staleness interval
    , force_expire_at timestamp
  > default (
    ifnull(cast(safe.string(options_json.tolerate_staleness) as interval), interval 30 minute)
    , safe.timestamp(safe.string(options_json.force_expire_at))
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
          , format(
            if(
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

  -- Alignment and extract staled partition
  set ret = (
    with
    pseudo_partition as (
      SELECT
        label
        , _pseudo_partitions
        , _pseudo_partition_id as partition_id
        , struct(partition_id, table_catalog, table_schema, table_name, last_modified_time)
          as alignment_paylod
      from _partitions_temp
      left join unnest([struct(
        contains_substr(argument, '*') as has_wildcard
        , regexp_replace(argument, r'\*$', '') as pattern
        , (
          select as value
            ifnull(
              array_agg(distinct nullif(nullif(d, any_value), null_value) ignore nulls)
              , []
            )
          from unnest(partition_alignments) a
          left join unnest(a.sources) src
          left join unnest([a.destination, src]) d
        ) as _pseudo_partitions
      )])
      /*
      * Pesudo partition generation for alignment
      */
      left join unnest(
        case
          --  _TABLE_SUFFIX -> __ANY__, __NULL__, _TABLE_SUFFIX
          when has_wildcard
            then [null_value, any_value, regexp_replace(table_name, format('^%s', pattern), '')]
          --           null -> __ANY__, __NULL__, 20220101, 20220102, ... (alignment range)
          when partition_id is null
            then [null_value, any_value] || _pseudo_partitions
          --       __NULL__ -> __ANY__, __NULL__
          when partition_id = null_value
            then [null_value, any_value]
          -- _PARTITIONTIME -> __ANY__, _PARTITIONTIME
          else
            [any_value, partition_id]
        end
      ) as _pseudo_partition_id
      where
        table_name = argument or starts_with(table_name, pattern)
    )
    , argument_alignment as (
      select
        a.destination as partition_id
        , array_length(a.sources) as n_sources
        , source as source_partition_id
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
    left join unnest([ifnull(destination.partition_id, null_value)]) as partition_id
    where
      is_ready_every_sources
      and (
        -- Staled if destination partition does not exist
        destination.last_modified_time is null
        -- Staled if partition is older than force_expire_at timestamp. If force_expire_at is null, the condition is ignored.
        or ifnull(destination.last_modified_time <= options.force_expire_at, false)
        -- Staled destination partition only if source partition is enough stable and old
        or (
          source.last_modified_time - destination.last_modified_time >= options.tolerate_staleness
          or (
            source.last_modified_time >= destination.last_modified_time
            and current_timestamp() - destination.last_modified_time >= options.tolerate_staleness
          )
        )
      )
  );
end;