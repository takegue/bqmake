create temp function `bqtest.zdeindent`(str string)
as ((
  select
    string_agg(
      regexp_replace(line, r'^'|| repeat(' ', max_deindent), '')
      , '\n'
    )
  from unnest([str])
  left join unnest([struct(
    split(str, '\n') as lines
  )]) as v
  left join unnest([struct(
    (select min(ifnull(char_length(regexp_extract(line, r'^\s+')), 0)) from v.lines as line) as max_deindent
  )])
  left join unnest(lines) as line
));


begin
  select
  `bqtest.zdeindent`(
    """
    --SQL template
    select
      fuga fuga
    from hogehoge
    """
  )
end
