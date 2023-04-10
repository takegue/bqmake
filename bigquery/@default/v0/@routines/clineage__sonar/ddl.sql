CREATE OR REPLACE PROCEDURE `bqmake.v0.clineage__sonar`(OUT scan_query STRING, IN target_dataset STRUCT<prject_id STRING, dataset_id STRING>, IN target_table_names ARRAY<STRING>)
OPTIONS(
  description="Induct column linage analysis using audit log.\n\nArguments\n===\n\n- analyze_query: \n- taget_dataset: \n\n\nExmaples\n===\n\ndeclare scan_query string;\n\ncall `v0.clineage__sonar`(scan_query, ('bqmake', 'v0'), []);\nexecute immediate scan_query;\n")
begin
  declare clineage_query array<struct<
    table_catalog STRING, table_schema STRING, table_name STRING, field_index int64, field_path STRING, vhash STRING, query STRING
  >>;

  execute immediate `v0.zgensql__clineage__sonar_queries`(
    (coalesce(target_dataset.prject_id, @@project_id), target_dataset.dataset_id)
    , target_table_names
  ) into clineage_query
  ;

  -- Analyze Column Lineage by BigQuery
  for r in (select * from unnest(clineage_query)) do
    set @@query_label = array_to_string(
      [
        format("clineage__catalog:%s", r.table_catalog),
        format("clineage__schema:%s", r.table_schema),
        format("clineage__table:%s", r.table_name),
        format("clineage__field_index:%d", r.field_index),
        format("clineage__field_path:%s", replace(r.field_path, ".", "_-_")),
        format("clineage__vhash:c%s", r.vhash)
      ]
      , ","
    );
    execute immediate r.query;
    set @@query_label = null;
  end for
  ;

  -- Output column lineage
  set scan_query = `v0.zgensql__clineage__analyze`(
    ("bqmake", "_auditlog", "cloudaudit_googleapis_com_data_access")
    , interval 3 hour
  );
end;