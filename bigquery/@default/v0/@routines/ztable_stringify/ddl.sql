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
  const formatter = {
    'INTEGER': defaultFormetter,
    'FLOAT': defaultFormetter,
    'BOOLEAN': defaultFormetter,
    'STRING': (val) => typeof(val) === 'string' ? `"${val}"` : "null",
    'DATE': defaultFormetter,
    'TIME': defaultFormetter,
    'DATETIME': defaultFormetter,
    'TIMESTAMP': defaultFormetter,
    'ARRAY': (val) => JSON.stringify(val),
    'RECORD_OR_JSON': (val) => JSON.stringify(val),
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
          key.length + 2,
          ...examples.map((e) => {
            const f = formatter[type] ;
            const s = (f ? f : defaultFormetter)(e)
            return s && s.length ? s.length + 2: null;
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
      return `${formated} `.padStart(length, ' ')
    }
    const formatRow = (row) => {
      return Object.entries(row).map(formatField).join(sep)
    }
    const table = [
      `/*${headerSepRow.join(sepGrid).substring(1)}+`,
      `|${headerRow.join(sep)}|`,
      `+${headerSepRow.join(sepGrid)}+`,
      ...data.map((row) => {
        return `|${formatRow(row)}|`
      }),
      `+${headerSepRow.join(sepGrid).substring(0)}*/`
    ].join('\n')
    return table
  }

  return formatTable(data, estimatedTypes)
}

const _test = () => {
  const data = [
    {"int":1,"str":"hoge","float":3.2,"boolean":null,"ts":null,"dt":null,"d":null,"json":null,"arr":null,"record":null},
    {"int":null,"str":null,"float":null,"boolean":null,"ts":"2023-01-01T00:00:00Z","dt":"2023-01-01T00:00:00","d":"2023-01-01","json":null,"arr":[1,2],"record":{"int":1}}
  ]
  const ret = prettyPrintTable(data)
  return ret === `
/*-----+--------+-------+---------+----------------------+---------------------+------------+------+-------+-----------+
| int  | str    | float | boolean | ts                   | dt                  | d          | json | arr   | record    |
+------+--------+-------+---------+----------------------+---------------------+------------+------+-------+-----------+
|    1 | "hoge" |   3.2 |    null |                 null |                null |       null | null |  null |      null |
| null |   null |  null |    null | 2023-01-01T00:00:00Z | 2023-01-01T00:00:00 | 2023-01-01 | null | [1,2] | {"int":1} |
+------+--------+-------+---------+----------------------+---------------------+------------+------+-------+-----------*/
`.trim()

}
try {
  return prettyPrintTable(JSON.parse(json_like_string))
}
catch {
  return null;
}
""";
