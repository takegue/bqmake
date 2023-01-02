create or replace function `v0.zgensql__staleness_check`(
  destination struct<project_id string, dataset_id string, table_id string>
  , sources array<struct<project_id string, dataset_id string, table_id string>>
  , partition_alignments array<struct<destination string, sources array<string>>>
  , options_json json
)
OPTIONS(
  description="""Extracts partitions that are stale.

Argument
===

- destination          : destination table
- sources              : source tables
- partition_alignments : partition alignments
- options              : option values in json format
  * default_project_id : default_project_id
  * tolerate_staleness : if the partition is older than this value (Default: interval 0 minute)
  *    force_expired_at: The timestamp to force expire partitions. If the destination's partition timestamp is older than this timestamp, the procedure stale the partitions. [Default: NULL].


Stalenss and Stablity Margin Checks
===

Case 1: Partition staleness with tolerate_staleness option
                     past                              now
Source Table        : |       |               |         |
                              ^ Refresh
Staleness Timeline  : | Fresh | Ignore(Fresh) |  Stale  |
                      +-----------------------^ tolerate staleness


Case 2: Partition staleness timeline with force_expired_at option
                     past                              now
Source Table        : | Fresh                           |
Staleness Timeline  : | Fresh | Stale                   |
                              ^ force_expired_at
""")
as ((
  select
  replace(replace(replace(replace(replace(replace(replace(
  `bqmake.v0.zreindent`("""
    with
    _partitions as (
      !PARTITIONS!
    )
    , pseudo_partition as (
      SELECT
        label
        , _pseudo_partitions
        , _pseudo_partition_id as partition_id
        , struct(partition_id, table_catalog, table_schema, table_name, last_modified_time)
          as alignment_paylod
      from _partitions
      left join unnest([struct(
        contains_substr(argument, '*') as has_wildcard
        , regexp_replace(argument, r'\\*$', '') as pattern
        , (
          select as value
            ifnull(
              array_agg(distinct nullif(nullif(d, !ANY_VALUE!), !NULL_VALUE!) ignore nulls)
              , []
            )
          from unnest(
            array<struct<destination string, sources array<string>>>[] || !ALIGNMENTS!) a
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
            then [!NULL_VALUE!, !ANY_VALUE!, regexp_replace(table_name, format('^%s', pattern), '')]
          --           null -> __ANY__, __NULL__, 20220101, 20220102, ... (alignment range)
          when partition_id is null
            then [!NULL_VALUE!, !ANY_VALUE!] || _pseudo_partitions
          --       __NULL__ -> __ANY__, __NULL__
          when partition_id = !NULL_VALUE!
            then [!NULL_VALUE!, !ANY_VALUE!]
          -- _PARTITIONTIME -> __ANY__, _PARTITIONTIME
          else
            [!ANY_VALUE!, partition_id]
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
      from
        unnest(array<struct<destination string, sources array<string>>>[] || !ALIGNMENTS!) a
        , unnest(a.sources) as source
    )
    , aligned as (
      select
        struct(
          _v.partition_id
          , destination.alignment_paylod.last_modified_time
        ) as destination
        , source.alignment_paylod as source
        , -- Check fully partition alignment for destination and sources.
          -- It means that # of Records = (# of source kind) * *(# of partition)
          ifnull(
            (
              select count(1)
              from unnest(array<struct<project_id string, dataset_id string, table_id string>>!SOURCES!) s
              where not ends_with(s.dataset_id, 'INFORMATION_SCHEMA')) * n_sources
              = countif(source.partition_id is not null) over (partition by _v.partition_id)
            , true
          )
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
    , _final as (
      select
        array_agg(distinct partition_id order by partition_id)
      from aligned
      left join unnest([ifnull(destination.partition_id, !NULL_VALUE!)]) as partition_id
      where
        is_ready_every_sources
        and ifnull(
          -- Staled if destination partition does not exist
          destination.last_modified_time is null
          -- Staled if partition is older than force_expired_at timestamp. If force_expired_at is null, the condition is ignored.
          or ifnull(destination.last_modified_time <= !FORCE_EXPIRED_AT!, false)
          -- Staled destination partition only if source partition is enough stable and old
          or (
            source.last_modified_time - destination.last_modified_time >= !TOLERATE_STALENESS!
            or (
              source.last_modified_time >= destination.last_modified_time
              and current_timestamp() - destination.last_modified_time >= !TOLERATE_STALENESS!
            )
          )
          , true
        )
      )
    select * from _final
  """, 0)
  , '!PARTITIONS!'
  , (
    select
      ltrim(`bqmake.v0.zreindent`(string_agg(
        format(`bqmake.v0.zreindent`("""
          select
            '%s' as label
            , '%s' as argument
            , *
          from `%s.%s.INFORMATION_SCHEMA.PARTITIONS`
          where %s
          """, 0)
          , label
          , target.table_id
          , coalesce(target.project_id, safe.string(options_json.default_project_id))
          , target.dataset_id
          , format(
            if(
              contains_substr(target.table_id, '*')
              , 'starts_with(table_name, replace("%s", "*", ""))'
              , 'table_name = "%s"'
            )
            , target.table_id)
        )
        , '\nunion all\n'
      ), 2))
    from unnest(
      [
        struct('destination' as label, destination as target)
      ]
      || array(select as struct 'source', s from unnest(sources) s)
    )
    where
      not ends_with(target.dataset_id, 'INFORMATION_SCHEMA')
  ))
  , '!NULL_VALUE!', '"__NULL__"')
  ,  '!ANY_VALUE!', '"__ANY__"')
  , '!SOURCES!', format('%T', sources))
  , '!ALIGNMENTS!', format('%T', partition_alignments))
  , '!TOLERATE_STALENESS!', format('%T', ifnull(cast(safe.string(options_json.tolerate_staleness) as interval), interval 30 minute)))
  , '!FORCE_EXPIRED_AT!', format('timestamp(%T)',safe.timestamp(safe.string(options_json.force_expired_at))))
));
