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

    if (c.match(/[@_a-z0-9]/i)) {
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
  call `bqmake.v0.assert_golden`(
    (null, "bqtest", "zgolden_routines")
    , -- Profiling query
    `bqtest.zbqt_gensql__udf_snapshot`([
        "`bqtest.zfind_ctes`(r'WITH cte1 AS (select 1), cte2 as (select [1, 2, 3] from (select * from \`cte1\`) as hoge) select cte1')"
      ]
      , "zgolden_routines"
    )
    , 'signature'
    , @update_golden > 0
  );
end
