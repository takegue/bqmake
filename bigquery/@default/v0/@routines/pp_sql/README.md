Pretty print a table-like object as a SQL format representation

Arguments:
===

- table_like_object: array<any type>

Returns:
===

valid SQL string to generate table values

Examples:
===

```sql
with cte as (
  select *
  from unnest(
    array<struct<
      int int64
      , str string
      , float float64
      , boolean bool
      , ts timestamp
      , dt datetime
      , d date
      , json json
      , arr array<int64>
      , record struct<int int64>>
    >[
      (1, 'hoge', 3.21234556, null, null, null, null, null, null, null),
      (null, string(null), float64(null), bool(null), '2023-01-01', '2023-01-01', '2023-01-01', to_json(null), [1, 2], struct(1))
    ]
  )
)

select
  v0.pp_sql(array_agg(cte)),
from cte
--> /*
# AUTO-GENERATED
  select * 
  from unnest(array<struct<int INT64, str STRING, float FLOAT64, boolean INT64, ts TIMESTAMP, dt DATETIME, d DATE, json JSON, arr ARRAY<INT64>, record STRUCT<INT64>>>[
    (1, "hoge", 3.21234556, NULL, NULL, NULL, NULL, NULL, NULL, NULL),
    (NULL, NULL, NULL, NULL, TIMESTAMP "2023-01-01 00:00:00+00", DATETIME "2023-01-01 00:00:00", DATE "2023-01-01", JSON "null", [1, 2], STRUCT(1))
  ])
*/
```

