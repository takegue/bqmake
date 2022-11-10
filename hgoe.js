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
  const lex_characters = new RegExp(/^[@a-z0-9]/, "ig");
  const isNonLexical = (t) => !lex_characters.test(t) && t !== "\\";
  const isEscape = (t) => t === "\\";

  const boundaries = "()[]`\"'";

  let c, p;
  for (let ix = 0; ix < sql.length; ix++) {
    p = c;
    c = sql[ix];

    // Escaping
    if (p == "\\") {
      c = buffer.pop().char + c;
    }
    buffer.push({ char: c, left: ix - c.length + 1, right: ix });

    // Lexical analysis
    if (isNonLexical(c)) {
      // If found lexical boundary then consume buffer
      const bufferToken = buffer.map((t) => t[1]).join("").replace(/\s+/, "");
      let gap = 0;
      for (const [token, tix] of bufferToken.matchAll(lex_characters)) {
        if (tix - gap > 0) {
          bufferToken.substr(token, tix - gap);
        }
        tokens.push(token);
        gap = tix;
      }
      buffer.length = 0;
    }

    // Syntax analysis
  }
  console.log(tokens);

  // token boundary check
  /*
    if (
      !isEscape(c) && (
        (
          isNonLexical(n) ^ isNonLexical(c)
        ) ||
        (isNonLexical(n) && n !== c)
      )
    ) {
      console.log("flush", token, buffer);
      const posRange = buffer.length > 0
        ? [buffer[0][0], buffer[buffer.length - 1][0]]
        : null;
      buffer.length = 0;
      if (token) {
        let poped = null;
        // Analyze tokens
        if (left.includes(token)) {
          regionsStack.push(n);
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
          posRange,
        ]);

        if (unipairs.includes(poped)) {
          tokens.pop();

          const words = [poped];
          let t = tokens.pop();
          const rightPos = t[3][1];

          while (poped !== t[2]) {
            words.push(t[2]);
            t = tokens.pop();
          }
          words.push(t[2]);
          const leftPos = t[3][0];

          tokens.push([
            "IDENTIFIER",
            regionsStack.length,
            words.reverse().join(""),
            [leftPos, rightPos],
          ]);
        }

        if (tokens.length > 1) {
          // from
          maybe_kw_from = tokens[tokens.length - 2];
          maybe_kw_identifier = tokens[tokens.length - 1];
          if (
            maybe_kw_from[2].match(new RegExp("^from$", "i")) &&
            maybe_kw_from[1] === maybe_kw_identifier[1]
          ) {
            tokens[tokens.length - 1][0] = "TABLE_IDENTIFIER";
          }

          // join

          // cross join (,)
        }
      }
    }
    buffer.push([ix, n]);
  }

  console.log(tokens);

  let lastWrote = 0;
  for (const [type, depth, surface, pos] of tokens) {
    ret.push(sql.substr(lastWrote, pos[0] - lastWrote));
    let text = sql.substr(pos[0], pos[1] - pos[0] + 1);
    if (type === "TABLE_IDENTIFIER") {
      console.log(text);
      if (text.match(replacements[0])) {
        text = text.replace(replacements[0], replacements[1]);
      }
    }
    ret.push(text);
    lastWrote = pos[1] + 1;
  }
  ret.push(sql.substr(lastWrote, sql.length - lastWrote));

  return ret.join("");
  */
}

const input = `WITH cte1 AS (select 1 as \`fuga-fuga-fuga\` from hoge)
, cte2 as (select [1, 2, 3] from (select * from \`cte1\`) as hoge)
, cte3 as (select "cte1 \\"" from (select * from cte1) as hoge)
, cte4 as (select [1, 2, 3] from (select * from cte1) as hoge)
select cte1
`;

console.log(replace_identifier(input, ["cte1", "hoge1"]));
