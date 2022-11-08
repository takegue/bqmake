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
      if (token.match(/AS/i)) {
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
  const right = [")", "]"];
  const unipairs = ["`", "'", '"'];
  let buffer = [];

  const tokens = [];
  const regionsStack = [];
  const ret = [];
  const lex_characters = new RegExp("[@a-z0-9]", "i");

  for (let ix = 0; ix < sql.length; ix++) {
    const c = sql[ix];
    const p = buffer.length > 0 ? buffer[buffer.length - 1] : "";
    const isNonLexical = !c.match(lex_characters);

    // token boundary check
    if (
      (isNonLexical ^ !p.match(lex_characters)) ||
      (isNonLexical && c !== p)
    ) {
      const token = buffer.join("").replace(/\s+/, "");
      buffer.length = 0;
      if (token) {
        let poped = null;
        // Analyze tokens
        if (left.includes(token)) {
          regionsStack.push(c);
        } else if (right.includes(token)) {
          poped = regionsStack.pop();
        } else if (unipairs.includes(token)) {
          if (regionsStack[regionsStack.length - 1] === token) {
            poped = regionsStack.pop();
          } else {
            regionsStack.push(token);
          }
        }

        tokens.push([
          "UNKNOWN",
          regionsStack.length + (poped ? 1 : 0),
          token,
        ]);

        if (unipairs.includes(poped)) {
          tokens.pop();

          const words = [poped];
          let t = tokens.pop();
          while (poped !== t[2]) {
            words.push(t[2]);
            t = tokens.pop();
          }
          words.push(t[2]);

          tokens.push([
            "IDENTIFIER",
            regionsStack.length,
            words.reverse().join(""),
          ]);
        }

        if (tokens.length > 1) {
          maybe_kw_from = tokens[tokens.length - 2];
          maybe_kw_identifier = tokens[tokens.length - 1];
        }
      }
    }
    buffer.push(c);
  }

  console.log(tokens);

  return ret;
}

const input = `WITH cte1 AS (select 1 as \`fuga-fuga-fuga\` from hoge)
, cte2 as (select [1, 2, 3] from (select * from \`cte1\`) as hoge)
select cte1
`;

console.log(replace_identifier(input, ["cte1", "hoge1"]));
