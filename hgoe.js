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

function replace_identifier(sql, replacements) {
  const left = ["(", "["];
  const unipairs = ["`", "'", '"', '"""', "'''"];
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
        right: ix + m[0].length,
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
        // console.log(ss, endAhead, ss[endAhead - 0]);
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
        right: endAhead,
        type: "REGION",
      });

      ix += endAhead - 1;
      continue;
    } else if (!isSpace(c)) {
      const m = sql.substr(ix).search(/\s|[@a-z0-9_.]/i);
      if (m < 0) {
        continue;
      }
      const token = sql.substr(ix, m);
      tokens.push({
        token: token,
        left: ix,
        right: ix + token.length,
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

const input = `
CREATE FUNCTION bqmake.bqtest.zgensql__view_test(_table_name STRING, test_configs ARRAY<STRUCT<cte STRING, unique_columns ARRAY<STRING>, nonnull_columns ARRAY<STRING>, accepted_values_columns ARRAY<STRUCT<column STRING, accepcted_values ARRAY<STRING>>>>>)
AS (
(
with views as (
  select view_definition
  from \`zpreview_proto.INFORMATION_SCHEMA.VIEWS\`
  where
    table_name = _table_name
)
, switched as (
  select
    view_definition
    , rtrim(left(
        view_definition
        , \`bqtest.zfind_final_select\`(view_definition)
    ))
    -- test case CTEs
    || array_to_string(array(
        select
          format(
            ', __test_%s as (\n%s\n)'
            , cte
            , \`bqtest.zgensql__table_test\`(
              cte
              , config.unique_columns
              , config.nonnull_columns
              , config.accepted_values_columns
            )
          )
        from unnest(\`bqtest.zfind_ctes\`(view_definition)) as cte
        left join unnest(test_configs) as config using(cte)
        where config.cte is not null
      )
      , '\n'
    )
    -- final select
    || '\n'
    || array_to_string(array(
        select
          format('select * from __test_%s', cte)
          , """ hoge hoge hoge """
          , "hoge \\"hoge"
        from unnest(\`bqtest.zfind_ctes\`(view_definition)) as cte
        left join unnest(test_configs) as config using(cte)
        where config.cte is not null
      )
      , '\nunion all\n'
    ) as sql
  from views
)

select as value sql from switched
)
);
`;

console.log(
  replace_identifier(input, [
    [
      "zpreview_proto.INFORMATION_SCHEMA.VIEWS",
      "zztemp_2323c3b0ea1b4a1b9391c9cb5060b8f7.INFORMATION_SCHEMA.VIEWS",
    ],
  ]),
);
