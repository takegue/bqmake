declare query string;

set query = `bqmake.bqtest.zreplace_table_identifiers`(
  `v0.zgensql__table_lineage`(null, null, null)
  , [
    ('relations__impl', 'relations__examples')
  ]);

execute immediate
  array_to_string(
    [
      "create temp table ret as"
      , query
      , "order by destination, depth, dst_project, dst_dataset, dst_table"
    ]
    , '\n'
  )
  using 
    10 as max_depth
    , "2023-01-01" as `begin`
    , "2023-01-01" as `end`
;

call `bqmake.v0.assert_golden`(
  (null, "zgolden", "zgensql__table_lineage")
  , "select * from ret "
  , query_unique_key => "format('%t', row_number() over ())"
  , is_update => @update_golden
);
