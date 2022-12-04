create or replace function `bqtest.zgensql__udf_table_testing`(
  signature array<string>
) as ((
  format(`bqmake.v0.zreindent`("""
    select
      *
    from
      unnest([
        struct(string(null) as signature, string(NULL) as ret)
        , %s
      ])
    where
      signature is not null
    """
    , 0)
    , ltrim(`bqmake.v0.zreindent`(array_to_string(array(
      select
        format("(%t, format('%%T', %s))", format("%T", s), s)
      from unnest(signature) as s
      )
      , '\n, '
    ), 6))
  )
));

begin
  execute immediate `bqtest.zgensql__udf_table_testing`([
    `bqmake.v0.zreindent`("""
      `bqtest.zgensql__udf_table_testing`([
        "1"
      ])
    """, 0)
  ]);

  -- For explicit dependency;
  select `bqtest.sure_eq`('hoge', 'hoge', 'string');

  execute immediate `bqtest.zgensql__udf_table_testing`([
    "`bqtest.sure_eq`('hoge', 'hoge', 'string')"
    , "`bqtest.sure_eq`(('a', 'b'), ('a', 'b'), 'struct')"
    , "`bqtest.sure_eq`(1, 1, 'integer')"
  ]);
end;
