Pretty print a table-like object as a SQL format representation

Arguments:
===

- table_like_object: array<any type>
- options: JSON
    * max_cue_rows: max number of rows to infer the type. default: 3
    * unknown_value: filling value if type inference is not working. default: 'UNKNOWN'
  