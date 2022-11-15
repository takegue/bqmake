create or replace function `v0.zjson_entries_recursive`(json string)
returns array<struct<key string, value string>>
language js
as r"""
function json_entries_recursive(s) {
  function _json_entries_recursive(obj) {
    const genNamespace = (namespace, child) => (
      [namespace, child].join(".").replace(/^\./, "")
    );
    const kv_entries = (namespace, obj) => {
      if (Array.isArray(obj)) {
        return [{ key: namespace, value: JSON.stringify(obj) }];
      }
      if (typeof obj === "object") {
        if (obj === null) {
          return [{ key: namespace, value: null }];
        }
        return Object.entries(obj).reduce((acc, [key, value], ix) => {
          const new_namespace = genNamespace(namespace , key ? key : `_f${ix}`);
          if (typeof value === "object") {
            return [...acc, ...kv_entries(new_namespace, value)];
          }
          return [...acc, { key: new_namespace, value }];
        }, []);
      }
      return [{ key: namespace, value: obj }];
    };

    return kv_entries(null, obj);
  }

  if (!s) {
    return null;
  }
  const o = JSON.parse(s);
  if (!o) {
    return null;
  }
  return _json_entries_recursive(o);
};

return json_entries_recursive(json);
"""
;

begin
  select `v0.zjson_entries_recursive`(to_json_string(struct('hoge' as a, [1,2,3] as hoge, struct(123.21 as a))));
end
