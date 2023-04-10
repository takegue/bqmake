CREATE OR REPLACE FUNCTION `v0.zgensql__clineage__sonar_queries`(target_dataset STRUCT<project_id STRING, dataset_id STRING>, table_names ARRAY<STRING>) RETURNS STRING
AS (
replace(replace(
    """
    -- Query single column for column lineage
    with lineage_sql as (
      select as struct
        table_catalog, table_schema, table_name
        , row_number() over (partition by table_catalog, table_schema, table_name) as field_index
        , field_path
        , vhash
        , trim(format(r"select %s from `%s.%s.%s` limit 1"
          , field_path, table_catalog, table_schema, table_name
        )) as query
      from `!METADATA_COLUMN_FILED_PATH!`
        , (select generate_uuid() as vhash)
      where
        array_length(!TABLE_NAMES!) = 0
        OR table_name in unnest(!TABLE_NAMES!)
    )
    select array_agg(c) from lineage_sql as c
    """
    , "!TABLE_NAMES!", format('%T', table_names))
    , "!METADATA_COLUMN_FILED_PATH!", format('%s.%s.INFORMATION_SCHEMA.COLUMN_FIELD_PATHS', target_dataset.project_id, target_dataset.dataset_id)
  )
);