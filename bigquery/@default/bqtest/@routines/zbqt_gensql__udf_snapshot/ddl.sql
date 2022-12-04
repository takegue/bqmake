create or replace function `bqtest.zbqt_gensql__udf_snapshot`(
  signature array<string>
  , repository_table string
) as ((
  with base_table as (
    select as value
      format("select entity.* from `%s.%s.%s` where valid_to is null", table_catalog, table_schema, table_name)
    from `bqtest.INFORMATION_SCHEMA.TABLES`
    where table_name = repository_table
  )

  select
    format(`bqmake.v0.zreindent`("""
      select
        signature
        , ret
      from
        unnest([
          struct(string(null) as signature, string(NULL) as ret)
          , %s
        ]) as R
        %s
      where
        signature is not null
    """
    , 0)
    , ifnull(
      ltrim(`bqmake.v0.zreindent`(array_to_string(array(
        select
          format("(%t, format('%%T', %s))", format("%T", s), s)
        from unnest(signature) as s
        )
        , '\n, '
      ), 6))
      , 'NULL'
    )
    , ifnull(format(
      ltrim(`v0.zreindent`("""
        full join (%s) as S using(signature)
        left join unnest([coalesce(R.signature, S.signature)]) as signature
        left join unnest([coalesce(R.ret, S.ret)]) as ret
      """
      , 4))
      , (select as value * from base_table limit 1)
    ), '')
  )
));

begin
  execute immediate `bqtest.zbqt_gensql__udf_snapshot`(
    [
    `bqmake.v0.zreindent`("""
      `bqtest.zgensql__udf_table_testing`([
        "1"
      ], '')
      """
      , 0
    )
    ]
  , "zsnapshot_routines_all"
  );
end;
