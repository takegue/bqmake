
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
call `bqtest.assert_golden`(
  store_golden_table
  , profiler_sql
  , false
);

-- For update golden data
call `bqtest.assert_golden`(
  store_golden_table
  , profiler_sql
  , true
);
```


## Example lineage

<!--- BQMAKE_DATASET: BEGIN -->
```mermaid
graph LR
subgraph bqmake.bqtest
	1+1w(demo_sample_table)
	Gtda(zsnapshot_profile__demo_sample_table)
	lhos(monitor__zsnapshot_profile__demo_sample_table__snapshot_job)
	leEX(monitor__zsnapshot_profile__demo_sample_view__entity)
	TIeN(zsnapshot_profile__demo_sample_view)
	F3Xr(monitor__zsnapshot_profile__demo_sample_view__snapshot_job)
end
subgraph bigquery-public-data.austin_311
	CNg3(311_service_requests)
end
subgraph bigquery-public-data.google_trends
	1imV(top_terms)
end
CNg3 --> 1+1w
1+1w --> Gtda
Gtda --> lhos
TIeN --> leEX
1imV --> TIeN
TIeN --> F3Xr
```
<!--- BQMAKE_DATASET: END -->