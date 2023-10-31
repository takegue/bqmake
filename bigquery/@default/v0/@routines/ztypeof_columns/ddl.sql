create or replace function `v0.ztypeof_columns`(
  sql_formatted_row_str string,
  row_json_str string
)
returns array<struct<column_name string, type string>>
language js
as r"""
function identify_type_of_bigquery(s) {
  const regexps = [
    [/^[A-Z]+ "/, s => s.match(/^([A-Z]+) /)[1]],
    [/^-?[0-9]*$/, () => "INT64"],
    [/^(-?[0-9]+[.e].*|CAST\("([^"]*)" AS FLOAT64\))$/, () => "FLOAT64"],
    [/^true|false$/, () => "BOOL"],
    [/^"|%"/, () => "STRING"],
    [/^b"/, () => "BYTES"],
    [/^\[/, () => "ARRAY"],
    [/^(STRUCT)?\(/, () => "STRUCT"],
    [/^ST_/, () => "GEOGRAPHY"],
    [/^NULL$/, () => "UNKNOWN"],
  ];

  for (let ix = 0; ix < regexps.length; ix++) {
    const [regexp, type_f] = regexps[ix];
    if (!regexp.test(s)) {
      continue
    }

    const type = type_f(s);
    if(!["STRUCT", "ARRAY"].includes(type)) {
      return type
    }

    // ARRAY, STRUCT
    nested = (() => {
      if (type == "ARRAY") {
        return _parse(s)[0]
      }
      return _parse(s).join(", ").trim()
    })()

    return `${type}<${nested}>`
  }
}

function _parse(sql) {
  const left = ["(", "[", "{"];
  const right = [")", "]", "}"];
  const quoteChars = ['"', "'", "`"];
  const delimiters = [","];

  let quotes = [];
  let surroundCnt = 0;
  let buffer = [];

  const tokens = [];
  const ret = [];

  for (let ix = 0; ix < sql.length; ix++) {
    const c = sql[ix];
    if (surroundCnt > 0) {
      buffer.push(c);
    }

    if (left.includes(c)) {
      surroundCnt++;
    } else if (right.includes(c)) {
      surroundCnt--;
    } else if(quoteChars.includes(c)) {
      if(quotes.length > 0 && quotes[quotes.length - 1] == c) {
        quotes.pop();
      } else {
        quotes.push(c);
      }
    } 

    // Push token
    if (
      (quotes.length === 0 && surroundCnt == 1 && c === ",")
      || (surroundCnt === 0 && c === ")")
    ) {
      const token = buffer.slice(0, buffer.length - 1).join("");
      buffer.length = 0;
      ret.push(identify_type_of_bigquery(token.trim()));
    }
  }

  if (buffer.length) {
    const token = buffer.slice(0, buffer.length - 1).join("");
    buffer.length = 0;
    ret.push(identify_type_of_bigquery(token.trim()));
  }
  return ret;
}

function _get_signature(sql_formatted_row_str, row_json) {
  const _types = _parse(sql_formatted_row_str)
  _keys = Object.keys(JSON.parse(row_json)).filter(k => k)
  
  const ret = _types.map((t, ix) => ({column_name: _keys[ix], type: t}))
  return ret
}

_test = () => {
  test = '(1, "hoge", BIGNUMERIC "123456", 3.21234556, NULL, TIMESTAMP "2023-01-01 00:00:00+00", DATETIME "2023-01-01 00:00:00", DATE "2023-01-01", JSON "null", [0], (1, "a", BIGNUMERIC "1234567"))'
  json_arg2 = '{"int":1,"str":"hoge","float":3.21234556,"boolean":null,"ts":null,"dt":null,"d":null,"json":null,"arr":null,"record":null}'
  console.log(_get_signature(test, json_arg2))
}

// _test() 
try {
  return _get_signature(sql_formatted_row_str, row_json_str) 
} catch {
  return null
}
"""
