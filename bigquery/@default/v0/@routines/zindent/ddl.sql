create or replace function `v0.zindent`(str string, indent int64)
as (
  regexp_replace(str, r'^|\n', r'\0' || repeat(' ', ifnull(indent, error("v0.zindent: argument `indent` should'nt be null"))))
)
;

begin
  select
    bqtest.error_eq(
      trim(
        `v0.zindent`(
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
