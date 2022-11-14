create or replace function `bqtest.zreplace_table_identifier`(
  sql string
  , replacement struct<from_value string, to_value string>
)
returns string
language js
as r"""
function replace_identifier(sql, replacements) {
  const left = ["(", "["];
  const unipairs = ["`", "'", '"', '\"\"\"', "'''"];
  let buffer = [];

  const tokens = [];
  const regionsStack = [];
  const lexChars = new RegExp(/^[@a-z0-9_.]+/, "i");
  const isLexical = (t) => lexChars.test(t);
  const isSpace = (t) => /\s+/gi.test(t);

  let c, p;
  // tokenize
  for (let ix = 0; ix < sql.length; ix++) {
    p = c;
    c = sql[ix];

    if (isLexical(c)) {
      const m = sql.substr(ix, 255).match(lexChars);
      if (!m) {
        continue;
      }
      tokens.push({
        token: m[0],
        left: ix,
        right: ix + m[0].length - 1,
        type: "TOKEN",
      });

      ix += m[0].length - 1;
      continue;
    } else if (unipairs.includes(c)) {
      const maxLookahead = 3;
      let regionStart = sql.substr(ix, maxLookahead + 1);
      for (let s = maxLookahead; s >= 0; s--) {
        regionStart = regionStart.substr(0, s);
        if (unipairs.includes(regionStart)) break;
      }
      let endAhead = regionStart.length;
      while (true) {
        let ss = sql.substr(ix + endAhead);
        endAhead += ss.search(regionStart);
        if (endAhead < 0) {
          throw Exception("not found");
        }
        if (ss[endAhead - 2] === "\\") {
          endAhead += 2;
          continue;
        }
        break;
      }
      endAhead += regionStart.length;

      tokens.push({
        token: sql.substr(ix, endAhead),
        left: ix,
        right: ix + endAhead - 1,
        type: "REGION",
      });

      ix += endAhead - 1;
      continue;
    } else if (!isSpace(c)) {
      const m = sql.substr(ix).search(/\s|[@a-z0-9_.'"`]/i);
      if (m < 0) {
        continue;
      }
      const token = sql.substr(ix, m);
      tokens.push({
        token: token,
        left: ix,
        right: ix + token.length - 1,
        type: "SYMBOLS",
      });
      ix += token.length - 1;
    }
  }

  for (let ix = 0; ix < tokens.length; ix++) {
    if (ix < 2) {
      continue;
    }
    maybe_kw_from_or_join = tokens[ix - 1];
    maybe_kw_identifier = tokens[ix];
    if (
      maybe_kw_from_or_join.token.match(new RegExp("^from$", "i")) &&
      !left.includes(maybe_kw_identifier.token)
    ) {
      tokens[ix].type = "TABLE_IDENTIFIER";
    }
    // join
    if (
      maybe_kw_from_or_join.token.match(new RegExp("^join$", "i")) &&
      !left.includes(maybe_kw_identifier.token)
    ) {
      tokens[ix].type = "TABLE_IDENTIFIER";
    }
    // cross join (,)
  }

  {
    const ret = [];
    let lastWrote = 0;
    for (const { token, left, right, type } of tokens) {
      ret.push(sql.substr(lastWrote, left - lastWrote));

      let text = sql.substr(left, right - left + 1);
      if (type === "TABLE_IDENTIFIER") {
        text = text.replace(replacements[0], replacements[1]);
      }
      ret.push(text);
      lastWrote = right + 1;
    }
    ret.push(sql.substr(lastWrote + 1, sql.length - lastWrote + 1));

    return ret.join("");
  }
}

return replace_identifier(
  sql,
  [replacement.from_value, replacement.to_value]
)
"""
;

begin
  select
    `bqtest.zreplace_table_identifier`(input, ("cte1", "hoge"))
  from unnest([
    struct(
      r"""
      WITH cte1 AS (select 1 as `fuga-fuga-fuga` from fuga)
      , cte2 as (select [1, 2, 3] from (select * from `cte1`) as hoge)
      , cte3 as (select "", "cte1 \"" from (select * from cte1 as cte1) as hoge)
      , cte4 as (select [1, 2, 3] from (select * from cte1) as hoge)
      , cte5 as (
        select 1
        from
          (select 1 from cte3)
          , cte1 as hoge
      )
      , cte6 as (
        select 1
        from cte4
        left join cte1 on true
      )
      select cte1
      """
      as input
    )
  ])
  ;
end
