create or replace function `v0.zgensql__snapshot_scd_type2`(
  destination struct<
    project_id string
    , dataset_id string
    , table_id string
  >
  , snapshot_query string
  , exp_unique_key string
)
options(
  description="""Generate SQL for Snapshots in Slowly Changing Dimensions Type 2
"""
)
as ((
  select as struct
    -- DDL Query
    format("""
        # %s
        create table if not exists `%s`
        partition by DATE(valid_to)
        cluster by valid_from
        as %s
      """
      , header
      , destination_ref
      , snapshot_query
    ) as create_ddl
    , format("""
        # %s
        with source as (
          select
            *
            , ifnull(valid_to < (lead(valid_from) over (partition by unique_key order by valid_to nulls last)), false) as is_insane_valid_column
            , revision_hash is null as is_insale_revision_column
            , unique_key is null as is_insane_unique_key
          FROM %s
        )

        SELECT
          unique_key
          , if(countif(is_insane_valid_column) > 0, max(format('Contradict valid_from with previous valid_to at %%t, %%s', unique_key, revision_hash)), null) as validate_record_lifetime
        from source
        group by unique_key
      """
      , header
      , destination_ref
    ) as validate_query
    , format("""
        # %s
        with grain as (
          SELECT
            date(valid_to) as changed_date, unique_key
            , approx_count_distinct(revision_hash) as n_changed
          from %s
          group by changed_date, unique_key
        )
        , stats as (
          select change_date, approx_quantiles(n_changed, 4) from grain
        )
        select * from stats
      """
      , header
      , destination_ref
    ) as profile_query
    -- DML Query
    , format("""
      # %s
      merge `%s` T
      using
        (
          with
            reference as (
              select * from `%s` where valid_to is null
            )
            , update_data as (
              %s
            )

          /*
          Create records for merge syntax.
          There are three 4 of records for update:
            1. New Record: The record is not in the reference table and exists only in update_data
            2. Changed Record
              2.1 New one
              2.1 Old one
            3. Deleted Records
            4. Unchanged Records

          This SQL will generate only 1., 2. and 3. records.
          */
          select
            M.* replace(
              if(
                action in ('CHANGED', 'DELETE') and ix = 0
                , @timestamp
                , M.valid_to
              ) as valid_to
            )
          from reference as R
          full join update_data as U using(unique_key)
          left join unnest([struct(
            format('%%t', R.entity) != format('%%t', U.entity) as will_update
            , case
              when U.unique_key is not null and R.unique_key is not null
                then if(
                  format('%%t', R.entity) != format('%%t', U.entity)
                  , 'CHANGED'
                  , 'UNCHANGED'
                )
              when U.unique_key is not null and R.unique_key is null
                then 'NEW'
              when U.unique_key is null and R.unique_key is not null
                then 'DELETE'
              else error('UNKNOWN')
            end as action
          )])
          join unnest(
            case action
              when 'CHANGED' then [R, U]
              when 'NEW' then [U]
              when 'DELETE' then [R]
              when 'UNCHANGED' then []
            end
          ) as M with offset ix
          where
            action in ('CHANGED', 'NEW', 'DELETE')
        ) as M
        on
          T.valid_to is null
          and M.unique_key = T.unique_key
          and M.valid_to is not null
        -- Deactivate current record for update
        when matched
          then
            update set T.valid_to = M.valid_to
        -- Insert new for update
        when not matched by target
          then
            insert row
          """
            , header
            , destination_ref
            , destination_ref
            , snapshot_query
          ) as update_dml
          -- TVF DDL for Access
    , format("""
        # %s
        create or replace table function `%s`(_at timestamp)
        as
          select * from `%s`
          where
            -- when _at is null, use latest revision
            (`_at` is null and valid_to is null)
            or (
              _at is not null
              and (valid_from <= `_at` and ifnull(`_at` < valid_to, true))
            )
      """
      , header
      , destination_ref
      , destination_ref
    ) as access_tvf_ddl
  from unnest([struct(
    coalesce(
      format('%s.%s.%s', destination.project_id, destination.dataset_id, destination.table_id)
      , format('%s.%s', destination.dataset_id, destination.table_id)
      , error(format("Invalid Destination: %t", destination))
    ) as destination_ref
    , "Code generated by `bqmake.v0.generate_sql__scd_type2`; DO NOT EDIT." as header
  )])
  left join unnest([struct(
    ifnull(
      format("""
        select
          %s as unique_key
          , revision_hash
          , @timestamp as valid_from
          , timestamp(null) as valid_to
          , entity
        from
          (%s) as entity
          , (select as value generate_uuid()) as revision_hash
        """
        , exp_unique_key
        , snapshot_query
      )
      , error(format("Invalid argument: %t", snapshot_query))
    ) as snapshot_query
  )])
))
;
