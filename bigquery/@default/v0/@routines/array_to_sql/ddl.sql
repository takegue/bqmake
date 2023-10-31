create function `v0.array_to_sql`(
  table_like_object any type
)
as ((
  select as value
    header
  from unnest([struct(
    (
      select as value
        format('%T', _sample) as _type_sample
      from unnest(table_like_object) as _sample
      order by array_length(regexp_extract_all(_type_sample, r'NULL'))
      limit 1
    ) as _representype_value
  )])
  left join unnest([struct(
    `v0.ztypeof_columns`(_representype_value) as _types
    , `bqutil.fn.json_extract_keys`(to_json_string(table_like_object[safe_offset(0)])) as _keys
  )])
  left join unnest([struct(
    format('array<struct<%t>>[\n  %s\n]', 
      array_to_string(
        array(
          select format('%s %s', k, t) 
          from unnest(_keys) as k with offset as ix
          left join unnest(_types) as t with offset as ix using(ix)
        )
        , ','
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
