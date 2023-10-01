begin
  declare temp_schema, init_sql, defer_sql string;
  declare destination struct<project_id string , dataset_id string, table_id string>;

  set (temp_schema, init_sql, defer_sql) = `v0.zgensql__temporary_dataset`(false);
  execute immediate init_sql;

  set destination = (null,temp_schema, "target");

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
  execute immediate replace(
    "select entity.value from @dataset.zzsrepo__target"
    , "@dataset"
    , temp_schema
  );

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

  execute immediate replace(
    """
      select
        (
          select if(count(1) > 0, null, error("New store no exists")) from `@dataset.zzsrepo__target`
        )
        , (
          select if(count(1) > 0, null, error("Backup no exists")) from `@dataset.zzsrepo__target__*`
        )
      ;
    """
    , "@dataset"
    , temp_schema
  );
  execute immediate defer_sql;
exception when error then
  execute immediate defer_sql;
  raise using message = @@error.message;
end;
