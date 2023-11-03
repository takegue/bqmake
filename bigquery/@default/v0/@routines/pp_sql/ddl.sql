create or replace function `v0.pp_sql`(
  table_like_object any type
)
options(
  description="""Pretty print a table-like object as a SQL format representation

Arguments:
===

- table_like_object: array<any type>
- options: JSON
    * max_cue_rows: max number of rows to infer the type. default: 3
    * unknown_value: filling value if type inference is not working. default: 'UNKNOWN'
  """
)
as ((
  select as value
    header
  from unnest([struct(
    'int64' as unknown_value
    -- 'INT64' as unknown_value
    , array( 
      select as struct
        `v0.ztypeof_columns`(format('%T', _sample), to_json_string(_sample)) as value
      from unnest(table_like_object) as _sample with offset ix
      order by array_length(regexp_extract_all(format('%T', _sample), r'NULL'))
      limit 10
    ) as _signatures
  )])
  left join unnest([struct(
    format("""-- AUTO-GENERATED SQL
select * 
from unnest(array<struct<%t>>[
  %s
])
"""
    , array_to_string(
        array(
          select
            if(
              max(s.column_name) is not null
              , format(
                  '%s %s'
                  , ifnull(max(nullif(s.column_name, 'UNKNOWN')), unknown_value)
                  , ifnull(max(nullif(s.type, 'UNKNOWN')), unknown_value)
                )
              , ifnull(max(nullif(s.type, 'UNKNOWN')), unknown_value)
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
 
begin
  execute immediate (
      with cte as (
      select *
      from unnest([
        (null, string(null), float64(null), bool(null), '2023-01-01', '2023-01-01', '2023-01-01', to_json(null), [1, 2], struct(1)),
        (1, 'hoge', 3.21234556, null, null, null, null, null, null, null)
      ])
    )

    select as value `v0.pp_sql`(array_agg(cte)) from cte
  );
end;
