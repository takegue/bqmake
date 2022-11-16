create or replace procedure `bqtest.assert_golden`(
  snapshot_store_table struct<project_id string, dataset_id string, table_id string>
  , query string
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
        , coalesce(snapshot_store_table.project_id, @@project_id)
        , snapshot_store_table.dataset_id
        , snapshot_store_table.table_id
      )
      , interval 0 minute
    );
    exception when error then
      call `bqtest.log`(format("Create snapshot_store_table: %t", snapshot_store_table));
      call `v0.snapshot_table__init`(
        snapshot_store_table
        , (
          "format('%t', (partition_key, group_keys))"
          , query
          , null
        )
        , to_json(struct(
          false as enable_snapshot_monitor
          , false as enable_entity_monitor
        ))
      );
  end;

  if not ifnull(is_update, false) then
    -- Show Changes
    execute immediate format("create or replace temp table `snapshot_comparision_result` as %s"
      , `bqmake.v0.zgensql__snapshot_scd_type2`(
      snapshot_store_table
      , query, "format('%t', (partition_key, group_keys))"
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
        "format('%t', (partition_key, group_keys))"
        , query
        , null
      )
      , null
    );
  end if;
end
