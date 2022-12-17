
execute immediate 'create materialized view if not exists `bqtest.demo_sample_partition_table__cache` options(enable_refresh = true) as\n'
  || `bqtest.zbqt_gensql__table_profiler`(
    "demo_sample_partition_table"
    , null
    , to_json(struct(true as materialized_view_mode))
  );
