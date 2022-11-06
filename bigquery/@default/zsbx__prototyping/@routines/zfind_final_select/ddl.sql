create or replace function `zsbx__prototyping.zfind_final_select`(sql string)
returns int64
language js
as """
function find_final_select(sql) {
  const left = ["("];
  const right = [")"];
  let surroundCnt = 0;
  let buffer = [];

  const kw = "SELECT";
  const kw_matcher = new RegExp(kw, "i");

  for (let ix = 0; ix < sql.length; ix++) {
    const c = sql[ix];
    if (left.includes(c)) {
      surroundCnt++;
    } else if (right.includes(c)) {
      surroundCnt--;
    }

    if (c.match(/[a-z0-9]/i)) {
      buffer.push(c);
    } else {
      buffer.length = 0;
    }

    if (surroundCnt === 0 && buffer.join("").match(kw_matcher)) {
      return ix - kw.length;
    }
  }
}

if(typeof sql !== 'string'){
  return null;
}

return find_final_select(sql);
"""
;

begin
  select
    left(input, `zsbx__prototyping.zfind_final_select`(input))
  from unnest([
    struct(
      r"""
      with
      datasource as (
        select * from `bigquery-public-data.austin_311.311_service_requests`
      )
      , __test_count as (
        select count(1) from datasource
      )
      select * from __test_count
      """
      as expected
      , """
      with
      datasource as (
        select * from `bigquery-public-data.austin_311.311_service_requests`
      )
      , __test_count as (
        select count(1) from datasource
      )
      select * from datasource
      """
      as input
    )
  ])
  ;
end
