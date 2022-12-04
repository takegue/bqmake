create or replace function `v0.zreindent`(str string, indent int64)
as (
 `v0.zindent`(rtrim(ltrim(`v0.zdeindent`(str), '\n')), indent)
)
;

begin
  select
    bqtest.error_eq(
      trim(
        `v0.zreindent`(
"""
  --SQL template
  select '\\n'
  hoge
"""
        , 4
      )
    )
    ,
trim("""
    --SQL template
    select '\\n'
    hoge
""")
  , "v0.zindent"
  );

end
