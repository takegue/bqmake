create or replace function `bqtest.zbqt_gensql__udf_snapshot`(
  signature array<string>
  , repository_table string
) as ((
  with base_table as (
    select as value
      format("select * from `%s.%s.%s`", table_catalog, table_schema, table_name)
    from `bqtest.INFORMATION_SCHEMA.TABLES`
    where table_name = repository_table
  )

  select
    format(`bqmake.v0.zreindent`("""
      select
        signature
        , ret
      from
        unnest(
          array<struct<signature string, ret string>>[
            %s
          ]
        ) as R
        %s
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
  , "zgolden_routines"
  );
end;
