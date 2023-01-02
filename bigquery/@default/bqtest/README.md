## BigQuery Testing Tools

### Instalation sql generator into your dataset

```sql
-- In demo examle
call `bqmake.bqtest.init__bqtest_dataset`(("<your_project>", "<your_dataset>"));
```

### DBT-like testing

```sql
-- In demo examle
execute immediate `bqtest.zbqt_gensql__table_spec`(
"demo_sample_table"
, [
    (null,
    -- unique keys
    ["unique_key"]
    -- non-null columns
    , if(false, [''], [])
    -- Check column value
    , if(false, [('', [''])], []))
  ]
);

-- In your dataset
execute immediate `<your_project>.<your_dataset>.zbqt_gensql__table_spec`(
"demo_sample_table"
, [
    (null,
    -- unique keys
    ["unique_key"]
    -- non-null columns
    , if(false, [''], [])
    -- Check column value
    , if(false, [('', [''])], []))
  ]
);
```


### Golden/Snapshot testing

```sql
declare store_golden_table struct<p string, s string, t string>;
declare profiler_sql string

set store_golden_table = ( "<your_project>", "<your_dataset>", "zsnapshot_profile__demo_sample_table")
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


## Example lineage



<!--- BQMAKE_DATASET: BEGIN -->
```mermaid
graph LR
subgraph "fa:fa-sitemap bigquery-public-data"
subgraph "fa:fa-database austin_311"
	CNg3(fa:fa-table 311_service_requests)
end
end
subgraph "fa:fa-database bqtest"
	1+1w(fa:fa-table demo_sample_table)
	qt1O(fa:fa-table mateview1)
end
1+1w --> qt1O
CNg3 --> 1+1w
```
<!--- BQMAKE_DATASET: END -->