create or replace function `v0.pp_sql`(
  table_like_object any type
)
options(
  description="""Pretty print a table-like object as a SQL format representation
  """
)
as ((
  select as value
    header
  from unnest([struct(
    array(
      select as struct
        `v0.ztypeof_columns`(format('%T', _sample), to_json_string(_sample)) as value
      from unnest(table_like_object) as _sample
      order by array_length(regexp_extract_all(format('%T', _sample), r'NULL'))
      limit 3
    ) as _signatures
  )])
  left join unnest([struct(
    format('array<struct<%t>>[\n  %s\n]', 
      array_to_string(
        array(
          select
            if(
              max(s.column_name) is not null
              , format(
                  '%s %s'
                  , ifnull(max(nullif(s.column_name, 'UNKNOWN')), 'UNKNOWN')
                  , ifnull(max(nullif(s.type, 'UNKNOWN')), 'UNKNOWN')
                )
              , ifnull(max(nullif(s.type, 'UNKNOWN')), 'UNKNOWN')
            )
          from unnest(_signatures) as _signature
          left join unnest(_signature.value) as s with offset as ix
          group by ix
        )
        , ', '
      )
      , array_to_string(
        array(
          select 
            format('%T', _row)
          from unnest(table_like_object) as _row
        )
        , ",\n  "
      )
    ) as header
  )])
))
;
with cte as (
  select *
  from unnest([
    (null, string(null), float64(null), bool(null), '2023-01-01', '2023-01-01', '2023-01-01', to_json(null), [1, 2], struct(1)),
    (1, 'hoge', 3.21234556, null, null, null, null, null, null, null)
  ])
)

select
  any_value(`v0.ztypeof_columns`(format('%T', cte), to_json_string(cte))) as _hoge
  , `v0.pp_sql`(array_agg(cte)) 
from cte
