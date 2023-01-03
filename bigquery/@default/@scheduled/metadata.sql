call `bqmake.v0.dataset__update_description`(
  ["v0", 'bqtest']
  , (
    current_timestamp() - interval 7 day
    , current_timestamp()
    , null
  )
);
