create or replace function `bqtest.zreplace_table_identifier`(
  sql string
  , replacements array<struct<from_value string, to_value string>>
)
returns string
language js
as r"""
function replace_identifier(sql, replacements) {
  const left = ["(", "["];
  const unipairs = ["`", "'", '"'];
  let buffer = [];

  const tokens = [];
  const regionsStack = [];
  const lex_characters = new RegExp(/^[@a-z0-9]+/, "i");
  const isNonLexical = (t) => !lex_characters.test(t);

  let c, p;
  for (let ix = 0; ix < sql.length; ix++) {
    p = c;
    c = sql[ix];

    // Escaping
    if (c === "\\") {
      continue;
    }
    if (p === "\\") {
      c = p + c;
    }
    buffer.push({ char: c, left: ix - c.length + 1, right: ix });

    // Lexical analysis
    if (isNonLexical(c)) {
      // If found lexical boundary then consume buffer
      const bufferToken = buffer
        .map((t) => t.char).join("").replace(/\s+/g, "");
      const leftPos = buffer[0].left;

      let gap = 0;

      // tokenize
      for (const m of bufferToken.matchAll(/[@a-z0-9]+/gi)) {
        const token = m[0];
        const tix = m.index;
        if (tix - gap > 0) {
          const b = bufferToken.substr(gap, tix - gap);
          tokens.push({
            token: b,
            left: leftPos + gap,
            right: leftPos + tix - gap + 1,
            type: "UNKNOWN",
          });
        }
        tokens.push({
          token: token,
          left: leftPos + tix,
          right: leftPos + token.length - 1,
          type: "UNKNOWN",
        });
        gap = tix + token.length;
      }
      if (bufferToken.length - gap > 0) {
        const b = bufferToken.substr(gap, bufferToken.length - gap);
        tokens.push({
          token: b,
          left: leftPos + gap,
          right: leftPos + bufferToken.length - 1,
          type: "UNKNOWN",
        });
      }
      buffer.length = 0;

      const lastToken = tokens[tokens.length - 1];
      {
        let poped = null;
        if (lastToken && unipairs.includes(lastToken.token)) {
          if (
            regionsStack.length > 0 &&
            regionsStack[regionsStack.length - 1].token === lastToken.token
          ) {
            poped = regionsStack.pop();
          } else {
            regionsStack.push(lastToken);
          }
        }

        if (poped && unipairs.includes(poped.token)) {
          const rightPos = lastToken.right;
          tokens.pop();

          const words = [lastToken.token];
          let t = tokens.pop();

          while (lastToken.token !== t.token) {
            words.push(t.token);
            t = tokens.pop();
          }
          words.push(t.token);
          const leftPos = t.left;

          tokens.push({
            token: words.reverse().join(""),
            left: leftPos,
            right: rightPos,
            type: "LITERAL",
          });
        }
      }
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
    for (const { left, right, type } of tokens) {
      ret.push(sql.substr(lastWrote, left - lastWrote));

      let text = sql.substr(left, right - left + 1);
      if (type === "TABLE_IDENTIFIER") {
        if (text.match(replacements[0])) {
          text = text.replace(replacements[0], replacements[1]);
        }
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
  replacements.map(({from_value, to_value}) => [[from_value, to_value]])
)
"""
;

begin
  select
    `bqtest.zreplace_table_identifier`(input, [("cte1", "hoge")])
  from unnest([
    struct(
      r"""
      WITH cte1 AS (select 1 as \`fuga-fuga-fuga\` from fuga)
      , cte2 as (select [1, 2, 3] from (select * from \`cte1\`) as hoge)
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
