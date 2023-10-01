CREATE OR REPLACE FUNCTION `bqtest.zgensql__udf_table_testing`(signature ARRAY<STRING>, repository_table STRING)
AS (
(
  with base_table as (
    select as value
      format("select entity.* from `%s.%s.%s`", table_catalog, table_schema, table_name)
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
    , ltrim(`bqmake.v0.zreindent`(array_to_string(array(
      select
        format("(%t, format('%%T', %s))", format("%T", s), s)
      from unnest(signature) as s
      )
      , '\n, '
    ), 6))
    , ifnull(format(
      `v0.zreindent`("""
        right join (%s) as S using(signature)
        left join unnest([coalesce(R.signature, S.signature)]) as signature
        left join unnest([coalesce(R.ret, S.ret)]) as ret
      """
      , 2)
      , (select as value * from base_table limit 1)
    ), '')
  )
)
);