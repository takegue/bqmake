create or replace function `bqtest.zfind_ctes`(sql string)
returns array<string>
language js
as r"""
function find_cte(sql) {
  const left = ["("];
  const right = [")"];
  let surroundCnt = 0;
  let stackSELECT = [];
  let buffer = [];

  const tokens = [];
  const ret = [];

  for (let ix = 0; ix < sql.length; ix++) {
    const c = sql[ix];
    if (left.includes(c)) {
      surroundCnt++;
    } else if (right.includes(c)) {
      surroundCnt--;

      if (
        stackSELECT.length &&
        startedSELECT[stackSELECT.length - 1] < surroundCnt
      ) {
        stackSELECT.pop();
      }
    }

    if (c.match(/[a-z0-9]/i)) {
      buffer.push(c);
    }

    // Push token
    if (c.trim() == "") {
      const token = buffer.join("");
      buffer.length = 0;
      if (token) {
        tokens.push(["UNKNOWN", surroundCnt, token]);
      }

      if (token.match(/SELECT/i)) {
        startedSELECT = surroundCnt;
      }

      // Analyze Tokens
      if (token.match(/^AS$/i)) {
        _ = tokens[tokens.length - 1];
        identifier_or_expression = tokens[tokens.length - 2][2];
        if (surroundCnt == 0 && stackSELECT.length == 0) {
          // CTE identifier
          ret.push(identifier_or_expression);
        }
      }
    }
  }
  return ret;
}

return find_cte(sql ?? "");
"""
;

begin
  select
    `bqtest.zassert`(format('%t', `bqtest.zfind_ctes`(input)), format('%t', expected))
  from unnest([
    struct(
      r"""
      WITH cte1 AS (select 1 as \`fuga-fuga-fuga\` from fuga)
      , cte2 as (select [1, 2, 3] from (select * from \`cte1\`) as hoge)
      select cte1
      """
      as input
      , ['cte1', 'cte2'] as expected
    )
  ])
  ;
end
