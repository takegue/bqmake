begin
  declare destination struct<project_id string , dataset_id string, table_id string>;


  drop schema if exists `zpreview__snapshot_test_schema_change` cascade;
  create schema if not exists `zpreview__snapshot_test_schema_change`;
  set destination = (null, "zpreview__snapshot_test_schema_change", "target");

  call `v0.snapshot_table__update`(
    destination
    , null
    , (
      "key"
      , "select * from unnest(array<struct<key int64, value string >>[(1, 'hoge')])"
      , null
    )
    , to_json(struct(
      current_timestamp() as force_expired_at
      , "replace_if_changed" as auto_recreate 
    ))
  );
  execute immediate "select entity.value from zpreview__snapshot_test_schema_change.zzsrepo__target";

  call `v0.snapshot_table__update`(
    destination
    , null
    , (
      "key"
      , "select * from unnest(array<struct<key int64, value string, value2 string>>[(1, 'hoge', 'piyo')])"
      , null
    )
    , to_json(struct(
      current_timestamp() as force_expired_at
      , "replace_if_changed" as auto_recreate 
    ))
  );

  select
    (
      select if(count(1) > 0, null, error("New store no exists")) from `zpreview__snapshot_test_schema_change.zzsrepo__target`
    )
    , (
      select if(count(1) > 0, null, error("Backup no exists")) from `bqmake-dev.zpreview__snapshot_test_schema_change.zzsrepo__target__*`
    )
  ;

  drop schema `zpreview__snapshot_test_schema_change` cascade;
exception when error then
  drop schema `zpreview__snapshot_test_schema_change` cascade;
  raise using message = @@error.message;
end;
