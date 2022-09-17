create or replace function `v0.get_bqlabel_from_option`(
  label_option_value string
)
returns array<struct<key string, value string>>
options(description="""Get BigQuery Labels from INFORMATION_SCHEMA.TABLE_OPTIONS's option_value

Arguments
====
  label_option_value: label's option_value in BigQuery INFORMATION_SCHEMA Format like `'[STRUCT("hoge", "fuga"), STRUCT("hoge", "fugb")]'`
""")
as (
  array(
    select as struct
      string(label[0]), string(label[1]) as value
    from unnest(json_extract_array(safe.parse_json(replace(replace(replace(label_option_value, "STRUCT", ""), '(', '['), ')', ']')))) as label
  )
);

select `v0.get_bqlabel_from_option`('[STRUCT("hoge", "fuga"), STRUCT("hoge", "fugb")]')
