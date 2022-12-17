### Golden/Snapshot testing

Golden testing query using snapshot utilities.

```sql
declare store_golden_table struct<p string, s string, t string>;
declare profiler_sql string

set store_golden_table = ("<your_project>", "<your_dataset>", "zsnapshot_profile__demo_sample_table")
set profiler_sql = `bqmake.your_dataset.zbqt_gensql__table_profiler`(
    "demo_sample_table"
    , null
    , to_json(struct(true as materialized_view_mode))
)

-- For testing
call `bqmake.v0.assert_golden`(
  store_golden_table
  , profiler_sql
  , false
);

-- For update golden data
call `bqmake.v0.assert_golden`(
  store_golden_table
  , profiler_sql
  , true
);
```
