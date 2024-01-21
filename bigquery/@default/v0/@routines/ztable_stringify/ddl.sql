create or replace function `v0.ztable_stringify`(json_like_string string)
returns string
language js
as r"""
const prettyPrintTable = (data) => {

  // Identify each column key types
  const estimateTypeFromExamples = (examples) => {
    const identifyType = (val) => {
      if (val === null) {
        return null
      }
      if (typeof val === 'number') {
        if (Number.isInteger(val)) {
          return 'INTEGER'
        }
        return 'FLOAT'
      }
      if (typeof val === 'boolean') {
        return 'BOOLEAN'
      }

      // Check parsable date expression
      if (typeof val === 'string') {
        const maybedate = Date.parse(val)
        if (isNaN(maybedate)) {
          return 'STRING'
        }

        // sure that val is parsable date expression
        if(val.endsWith('Z')) {
          return 'TIMESTAMP'
        }
        if(val.includes('T')) {
          return 'DATETIME'
        }
        // DATE or TIME
        if (val.includes('-')) {
          return 'DATE'
        }
        return 'TIME'
      }

      if (val instanceof Array) {
        return 'ARRAY'
      }
      if (val instanceof Object) {
        return 'RECORD_OR_JSON'
      }
      return null
    }

    for (const val of examples) {
      const type = identifyType(val)
      if (type === null) {
        continue
      }
      return type
    }
  }

  const defaultFormetter = (val) => val ? val.toString() : "null"
  // If long string is included, set allowMultiline to true
  const allowMultiline = (() => {
    for (const row of data) {
      for (const key in row) {
        const val = JSON.stringify(row[key])
        if (val && val.length > 30) {
          return true
        }
      }
    } 
    return false
  })();

  const formatter = {
    'INTEGER': defaultFormetter,
    'FLOAT': defaultFormetter,
    'BOOLEAN': defaultFormetter,
    'STRING': (val) => typeof(val) === 'string' ? `"${val}"` : "null",
    'DATE': defaultFormetter,
    'TIME': defaultFormetter,
    'DATETIME': defaultFormetter,
    'TIMESTAMP': defaultFormetter,
    'ARRAY': (val) => JSON.stringify(val, null, allowMultiline ? 2 : null),
    'RECORD_OR_JSON': (val) => JSON.stringify(val, null, allowMultiline ? 2 : null),
  };

  const estimatedTypes = (() => {
      const agg = data.reduce((acc, cur) => {
        for (const key in cur) {
          const val = cur[key]
          if (acc[key] === undefined) {
            acc[key] = [val]
          } else {
            acc[key].push(val)
          }
        }
        return acc
      }, {});

      return Object.entries(agg).reduce((acc, [key, examples]) => {
        const type = estimateTypeFromExamples(examples)
        const typeLength = Math.max(
          key.length + 3,
          ...examples.map((e) => {
            const f = formatter[type] ;
            const s = (f ? f : defaultFormetter)(e)
            // Calculate max line length of string by delimited by new line
            const len = (s.split("\n")).reduce((acc, cur) => {
              const _new = cur && cur.length ? cur.length + 2: null
              return acc < _new ? _new : acc ;
             }, 0);
            return len;
          }));
          acc[key] = {
            type,
            typeLength: typeLength ? typeLength: 4,
            examples
          }
          return acc
        }, {}
      )
  })()

  const formatTable = (data, estimatedTypes) => {
    const sep = '|'
    const sepGrid = '+'
    const header = Object.keys(data[0])
    const headerRow = header.map((h) => ` ${h}`.padEnd(estimatedTypes[h].typeLength, ' '))
    const headerSepRow = header.map((h) => ''.padEnd(estimatedTypes[h].typeLength, '-'))

    const formatField = ([key, value]) => {
      const estimated = estimatedTypes[key]
      const length = estimated.typeLength

      const f = formatter[estimated.type];
      const formated = (f ? f : defaultFormetter)(value);
      return `${formated}`
    }

    const formatRow = (row) => {
      const formatedFields = Object.entries(row).map(formatField)
      const fields = formatedFields.map(s => s.split('\n'))
      // transpose fields aligned with longet elements of arary
      // argmax of elemtens length
      const longest = fields.reduce((acc, cur) => {
        return acc.length < cur.length ? cur : acc ;
      })

      const transposedFields = longest.map((_, colIndex) => fields.map(row => row[colIndex] ?? ""))
      return transposedFields.map((columns) => {
          c = columns.map(
            (c, colIndex) => {
              const estimated = estimatedTypes[Object.keys(row)[colIndex]]
              const length = estimated.typeLength
              return ` ${c}`.padEnd(length, ' ') 
            }).join(sep)
          return `|${c}|`
        }).join('\n')
    }
    const table = [
      `/*${headerSepRow.join(sepGrid).substring(1)}+`,
      `|${headerRow.join(sep)}|`,
      `+${headerSepRow.join(sepGrid)}+`,
      data.map((row) => {
        return `${formatRow(row)}`
      }).join(`\n`),
      `+${headerSepRow.join(sepGrid).substring(0)}*/`
    ].join('\n')
    return table
  }

  return formatTable(data, estimatedTypes)
}

const _test = () => {
  const data = [
    // long record
    {"int":1,"str":"hoge","float":3.2,"boolean":null,"ts":null,"dt":null,"d":null,"json":{"int":null,"str":null,"float":null,"boolean":null,"ts":"2023-01-01T00:00:00Z","dt":"2023-01-01T00:00:00","d":"2023-01-01","json":null,"arr":[1,2],"record":{"int":1}},"arr":null,"record":null},
    // short
    {"int":1,"str":"hoge","float":3.2,"boolean":null,"ts":null,"dt":null,"d":null,"json":null,"arr":null,"record":null},
    {"int":null,"str":null,"float":null,"boolean":null,"ts":"2023-01-01T00:00:00Z","dt":"2023-01-01T00:00:00","d":"2023-01-01","json":null,"arr":[1,2],"record":{"int":1}},
  ]
  const ret1 = prettyPrintTable(data.slice(1,))
  console.log({ret1, assert: ret1 === `
/*-----+--------+--------+----------+----------------------+---------------------+------------+-------+-------+-----------+
| int  | str    | float  | boolean  | ts                   | dt                  | d          | json  | arr   | record    |
+------+--------+--------+----------+----------------------+---------------------+------------+-------+-------+-----------+
| 1    | "hoge" | 3.2    | null     | null                 | null                | null       | null  | null  | null      |
| null | null   | null   | null     | 2023-01-01T00:00:00Z | 2023-01-01T00:00:00 | 2023-01-01 | null  | [1,2] | {"int":1} |
+------+--------+--------+----------+----------------------+---------------------+------------+-------+-------+-----------*/
`.trim()})

  const ret2 = prettyPrintTable(data, true)
  console.log({ret2, assert: ret2 === `
/*-----+--------+--------+----------+----------------------+---------------------+------------+---------------------------------+------+------------+
| int  | str    | float  | boolean  | ts                   | dt                  | d          | json                            | arr  | record     |
+------+--------+--------+----------+----------------------+---------------------+------------+---------------------------------+------+------------+
| 1    | "hoge" | 3.2    | null     | null                 | null                | null       | {                               | null | null       |
|      |        |        |          |                      |                     |            |   "int": null,                  |      |            |
|      |        |        |          |                      |                     |            |   "str": null,                  |      |            |
|      |        |        |          |                      |                     |            |   "float": null,                |      |            |
|      |        |        |          |                      |                     |            |   "boolean": null,              |      |            |
|      |        |        |          |                      |                     |            |   "ts": "2023-01-01T00:00:00Z", |      |            |
|      |        |        |          |                      |                     |            |   "dt": "2023-01-01T00:00:00",  |      |            |
|      |        |        |          |                      |                     |            |   "d": "2023-01-01",            |      |            |
|      |        |        |          |                      |                     |            |   "json": null,                 |      |            |
|      |        |        |          |                      |                     |            |   "arr": [                      |      |            |
|      |        |        |          |                      |                     |            |     1,                          |      |            |
|      |        |        |          |                      |                     |            |     2                           |      |            |
|      |        |        |          |                      |                     |            |   ],                            |      |            |
|      |        |        |          |                      |                     |            |   "record": {                   |      |            |
|      |        |        |          |                      |                     |            |     "int": 1                    |      |            |
|      |        |        |          |                      |                     |            |   }                             |      |            |
|      |        |        |          |                      |                     |            | }                               |      |            |
| 1    | "hoge" | 3.2    | null     | null                 | null                | null       | null                            | null | null       |
| null | null   | null   | null     | 2023-01-01T00:00:00Z | 2023-01-01T00:00:00 | 2023-01-01 | null                            | [    | {          |
|      |        |        |          |                      |                     |            |                                 |   1, |   "int": 1 |
|      |        |        |          |                      |                     |            |                                 |   2  | }          |
|      |        |        |          |                      |                     |            |                                 | ]    |            |
+------+--------+--------+----------+----------------------+---------------------+------------+---------------------------------+------+------------*/
`.trim()}) 
}

// _test()

try {
  return prettyPrintTable(JSON.parse(json_like_string))
}
catch {
  return null;
}

""";
