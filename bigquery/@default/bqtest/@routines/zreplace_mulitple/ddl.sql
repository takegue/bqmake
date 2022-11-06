create or replace function `bqtest.zreplace_mulitple`(
  value string
  , replaces ARRAY<struct<from_value string, to_value string>>
)
returns string
language js
as """
  if(!value || !replaces) {
    return null;
  }
  return replaces.reduce(
    (acc, {from_value, to_value}) => acc.replaceAll(from_value, to_value),
    value
  )
"""
;

begin
  select `bqtest.zassert`(
    'c',
    `bqtest.zreplace_mulitple`('a', [('a', 'b'), ('b', 'c')])
  );
end
