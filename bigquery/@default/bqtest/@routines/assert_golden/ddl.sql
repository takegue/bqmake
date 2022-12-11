create or replace procedure `bqtest.assert_golden`(
  snapshot_store_table struct<project_id string, dataset_id string, table_id string>
  , query string
  , query_unique_key string
  , is_update bool
)
options(
  description="""
  """
)
begin
  declare _is_update bool default ifnull(is_update, false);
  begin
    call `v0.retry_query_until_success`(
      format(
        "select * from `%s.%s.%s`"
        , snapshot_store_table.project_id
        , snapshot_store_table.dataset_id
        , snapshot_store_table.table_id
      )
      , interval 0 minute
    );
    exception when error then
      call `v0.log`(format("Create snapshot_store_table: %t", snapshot_store_table));
      call `v0.snapshot_table__init`(
        snapshot_store_table
        , (
          query_unique_key
          , query
          , null
        )
        , to_json(struct(
          false as enable_snapshot_monitor
          , false as enable_entity_monitor
          , false as enable_timetravel_tvf
        ))
      );
  end;

  if not ifnull(is_update, false) then
    -- Show Changes
    execute immediate format("create or replace temp table `snapshot_comparision_result` as %s"
      , `bqmake.v0.zgensql__snapshot_scd_type2`(
        snapshot_store_table
        , query, query_unique_key
      ).diff_query
    )
    using current_timestamp() as timestamp;
    assert not exists(select * from `snapshot_comparision_result`);
  else
    -- Save Changes
    call `bqmake.v0.snapshot_table__update`(
      snapshot_store_table
      , null
      , (
        query_unique_key
        , query
        , null
      )
      , to_json(struct(
        current_timestamp() as force_expired_at
      ))
    );
  end if;
end
;

call `bqtest.assert_golden`(
  (null, "bqtest", "snapshot_routines_all")
  , -- Profiling query
  `bqtest.zbqt_gensql__udf_snapshot`([
    `bqmake.v0.zreindent`("""
      `bqtest.zbqt_gensql__remake_view`(
        'demo_sample_view', '__test_count', [('datasource', 'datasource_sampled')]
      )
    """, 0)
    ]
    , "zzsrepo__snapshot_routines_all"
  )
  , 'signature'
  , false
);
