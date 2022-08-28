create or replace procedure `v0.get_partition_column`(
  out ret_column struct<name string, type string>
  , in target struct<
    project_id string
    , dataset_id string
    , table_id string
  >
)
begin
  declare name string;
  declare type string;

  execute immediate format("""
    select as struct
      column_name as name
      , data_type as type
    from `%s.%s.INFORMATION_SCHEMA.COLUMNS`
    where
      is_partitioning_column = "YES"
      and table_name = "%s"
  """
    , ifnull(target.project_id, @@project_id)
    , target.dataset_id
    , target.table_id
  ) into name, type
  ;

  set ret_column = (name, type);
end
