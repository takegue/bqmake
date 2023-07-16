Pretty print table
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
  v0.pp_table(array_agg(cte)),
from cte
-->
/*-----+--------+-------+---------+----------------------+---------------------+------------+------+-------+-----------+
| int  | str    | float | boolean | ts                   | dt                  | d          | json | arr   | record    |
+------+--------+-------+---------+----------------------+---------------------+------------+------+-------+-----------+
|    1 | "hoge" |   3.2 |    null |                 null |                null |       null | null |  null |      null |
| null |   null |  null |    null | 2023-01-01T00:00:00Z | 2023-01-01T00:00:00 | 2023-01-01 | null | [1,2] | {"int":1} |
+------+--------+-------+---------+----------------------+---------------------+------------+------+-------+-----------*/
```
