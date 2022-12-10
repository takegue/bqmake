create or replace function `v0.zgensql__partial_snapshot_scd_type2`(
  snapshot_repository struct<
    project_id string
    , dataset_id string
    , table_id string
  >
  , snapshot_query string
  , exp_unique_key string
)
options(
  description="""Generate Partial Snapshot SQL for Slowly Changing Dimensions Type 2
"""
)
as (
  format(v0.zreindent("""
      with repository as (
        select unique_key, entity from `%s`
        where valid_to is null
      )
      , partial as (
        with snapshot as (
          %s
        )
        select unique_key, entity from snapshot
      )
      select
        entity.*
      from source as S
      full join partial as R using(unique_key)
      left join unnest([coalesce(R.entity, S.entity)]) as entity
    """, 0)
    , format(
      '%s.%s.%s'
      , snapshot_repository.project_id
      , snapshot_repository.dataset_id
      , snapshot_repository.table_id
    )
    , `v0.zreindent`(
      `v0.zgensql__snapshot_scd_type2`(
          null
          , snapshot_query
          , exp_unique_key
      ).snapshot_query
      , 0
    )
  )
);
