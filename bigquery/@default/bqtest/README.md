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
	/VpU(fa:fa-table zzsrepo__zgolden_routines)
	1+1w(fa:fa-table demo_sample_table)
	2KhS(fa:fa-table zzsrepo__zsnapshot_profile__demo_sample_view)
	7hM4(fa:fa-table demo_sample_table__cache)
	Gtda(fa:fa-table zsnapshot_profile__demo_sample_table)
	PnMI(fa:fa-table zzsrepo__zsnapshot_profile__demo_sample_table)
	Rybc(fa:fa-table zzsrepo__snapshot_routines_all)
	TIeN(fa:fa-table zsnapshot_profile__demo_sample_view)
	YtHn(fa:fa-table zzsrepo__zsnapshot_routines_all)
	Z+1n(fa:fa-table demo_sample_partition_table)
	e0BA(fa:fa-table snapshot_routines_all)
	fFLB(fa:fa-table zsnapshot_routines_all)
	jknI(fa:fa-table demo_sample_partition_table__cache)
	qQZt(fa:fa-table mateview)
	qhIW(fa:fa-table zgolden_routines)
	qt1O(fa:fa-table mateview1)
end
/VpU --> /VpU
/VpU --> qhIW
1+1w --> 2KhS
1+1w --> PnMI
1+1w --> qQZt
1+1w --> qt1O
2KhS --> TIeN
7hM4 --> 2KhS
7hM4 --> PnMI
CNg3 --> 1+1w
PnMI --> Gtda
Rybc --> Rybc
Rybc --> e0BA
YtHn --> YtHn
YtHn --> fFLB
Z+1n --> jknI
Z+1n --> qt1O
```
<!--- BQMAKE_DATASET: END -->