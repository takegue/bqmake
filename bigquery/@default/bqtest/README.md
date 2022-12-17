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


## Example lineage

<!--- BQMAKE_DATASET: BEGIN -->
```mermaid
graph LR
subgraph bqmake.bqtest
	eZPC(bqmake)
	1+1w(demo_sample_table)
	NcSI(demo_mat_wiki)
	Z+1n(demo_sample_partition_table)
	Pv1l(demo_sample_shards_*)
	7hM4(demo_sample_table__cache)
	SsQj(demo_wiki)
	qQZt(mateview)
	qt1O(mateview1)
	E124(mateview2)
	z4a/(mateview3)
	2zH/(mateview_cache)
	xksx(monitor__zsnapshot_profile__demo_sample_table__entity)
	Gtda(zsnapshot_profile__demo_sample_table)
	PnMI(zzsrepo__zsnapshot_profile__demo_sample_table)
	lhos(monitor__zsnapshot_profile__demo_sample_table__snapshot_job)
	leEX(monitor__zsnapshot_profile__demo_sample_view__entity)
	TIeN(zsnapshot_profile__demo_sample_view)
	F3Xr(monitor__zsnapshot_profile__demo_sample_view__snapshot_job)
	e0BA(snapshot_routines_all)
	Rybc(zzsrepo__snapshot_routines_all)
	PZ0M(temp_mateview)
	mo9z(zsnapshot_routines__all)
	fFLB(zsnapshot_routines_all)
end
subgraph bigquery-public-data.austin_311
	CNg3(311_service_requests)
end
subgraph bigquery-public-data.google_trends
	0JIj(top_rising_terms)
	1imV(top_terms)
end
subgraph bigquery-public-data.wikipedia
	2fna(pageviews_2022)
end
1+1w --> eZPC
CNg3 --> 1+1w
1+1w --> NcSI
0JIj --> Z+1n
Z+1n --> 7hM4
1+1w --> 7hM4
2fna --> SsQj
1+1w --> qQZt
Z+1n --> qt1O
1+1w --> qt1O
1+1w --> E124
Z+1n --> z4a/
Z+1n --> 2zH/
Gtda --> xksx
PnMI --> Gtda
1+1w --> Gtda
7hM4 --> Gtda
1+1w --> PnMI
7hM4 --> PnMI
Gtda --> lhos
TIeN --> leEX
1imV --> TIeN
TIeN --> F3Xr
Rybc --> e0BA
Z+1n --> PZ0M
```
<!--- BQMAKE_DATASET: END -->