create or replace function `v0.zgensql__partition_alignment`(
  destination STRUCT<project_id STRING, dataset_id STRING, table_id STRING>
  , sources ARRAY<STRUCT<project_id STRING, dataset_id STRING, table_id STRING>>
  , partition_alignments ARRAY<STRUCT<destination STRING, sources ARRAY<STRING>>>
)
as
((
  with
    sql__information_schema as (
      select as value
          string_agg(
            format("""
              select
                '%s' as label
                , '%s' as argument
                , *
              from `%s.INFORMATION_SCHEMA.PARTITIONS`
              where %s
              """
              , label
              , target.table_id
              , coalesce(
                format('%s.%s', target.project_id, target.dataset_id)
                , target.dataset_id
                , error(format('Invalid target: %t', target))
              )
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
      ] || array(select as struct 'source', s from unnest(sources) s))
    )
    , ret as (
      select
        format(r"""
        with
          _partitions_temp as (
            %s
          )
        , _user_arguments as (
          select as value
            struct(
              cast(%T as ARRAY<STRUCT<destination STRING, sources ARRAY<STRING>>>) as partition_alignments
              , struct(
                "__NULL__" as null_value
                , interval 0 minute as tolerate_staleness
              ) as options
            )
        )
        , pseudo_partition as (
          SELECT
            label
            , coalesce(
                partition_id
                , if(has_wildcard, regexp_replace(table_name, format('^%%s', pattern), ''), null)
                , format_date('%%Y%%m%%d', _pseudo_date)
                , options.null_value
              )
              as partition_id
            , struct(partition_id, table_catalog, table_schema, table_name, last_modified_time)
              as alignment_paylod
          from _partitions_temp, _user_arguments
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
                    min(safe.parse_date('%%Y%%m%%d', least(d, s)))
                    , max(safe.parse_date('%%Y%%m%%d', greatest(d, s)))
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
          from _user_arguments, unnest(partition_alignments) a, unnest(a.sources) as source
        )
        , aligned as (
          select
            struct(
              _v.partition_id
              , destination.alignment_paylod.last_modified_time
            ) as destination
            , source.alignment_paylod as source
            , -- # of source kind * # of source partition
            %d * n_sources
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

        , detection as (
          select
            array_agg(distinct partition_id order by partition_id)
          from aligned, _user_arguments
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
        )
        select * from detection

        """
        , sql__information_schema
        , partition_alignments
        , array_length(sources)
      )
      from sql__information_schema
    )

    select * from ret
));

begin
  declare query string;

  create schema if not exists `zpreview_test__alignment`;

  create or replace table `zpreview_test__alignment.dest1`
  partition by date_jst
  as select date '2006-01-02' as date_jst
  ;

  create or replace table `zpreview_test__alignment.ref1`
  partition by date_jst
  as select date '2006-01-02' as date_jst
  ;

  set query = `v0.zgensql__partition_alignment`(
    (string(null), "zpreview_test__alignment", 'dest1')
    , [
      (string(null), "zpreview_test__alignment", 'ref1')
    ]
    , `v0.alignment_day2day`('2006-01-02', '2006-01-02')
  );

  execute immediate query;
  drop schema if exists `zpreview_test__alignment` cascade;
end
