begin
  declare destination struct<project_id string , dataset_id string, table_id string>;
  declare unique_key string default 'station_id';
  declare snapshot_query struct<create_ddl string, validate_ddl string, profile_query string, udpate_dml string, access_tvf_ddl string>;
  declare previous_process_bytes int64;

  create schema if not exists `zpreview__snapshot`;
  set destination = (null, "zpreview__snapshot", "stations_scd_type2");

  call `v0.snapshot_table__init`(
      destination
      , (
        unique_key
        , "select * from `bigquery-public-data.austin_bikeshare.bikeshare_stations` limit 0"
        , timestamp '2022-01-01'
      )
      , null
    )
  ;
  assert @@row_count is null;

  for t in (select as struct
      timestamp('2022-01-01' + interval idx day) as ts
      , *
      from unnest([
        struct(string(null) as msg, 0 as expected_row_count, "" as query)
        , ("Added all records", 102, "select * from `bigquery-public-data.austin_bikeshare.bikeshare_stations`")

        , ("Update all records", 204, 'select * replace("invalid status" as status) from `bigquery-public-data.austin_bikeshare.bikeshare_stations`')
        , ("Data Reset", 204, "select * from `bigquery-public-data.austin_bikeshare.bikeshare_stations`")

        , ("Change one record status", 2, 'select * replace(if(station_id in (2499), "closed", status) as status) from `bigquery-public-data.austin_bikeshare.bikeshare_stations`')
        , ("Data Reset", 2, "select * from `bigquery-public-data.austin_bikeshare.bikeshare_stations`")

        , ("Delete one record", 1, 'select * from `bigquery-public-data.austin_bikeshare.bikeshare_stations` limit 101')
        , ("Data Reset", 1, "select * from `bigquery-public-data.austin_bikeshare.bikeshare_stations`")

        , ("New one record", 1, 'select S.* replace(station_id as station_id) from `bigquery-public-data.austin_bikeshare.bikeshare_stations` as S left join unnest(if(station_id = 2499, [station_id, 249999], [station_id])) as station_id')
        , ("Data Reset", 1, "select * from `bigquery-public-data.austin_bikeshare.bikeshare_stations`")

      ]) with offset idx
      where msg is not null
  )
  do
    set previous_process_bytes = @@script.bytes_processed;

    call `bqmake.v0.snapshot_table__update`(
      destination
      , null
      , (unique_key, t.query, t.ts)
      , to_json(struct(
        current_timestamp() as force_expired_at
      ))
    )
    ;
    select if(
      t.expected_row_count = @@row_count
      , format("#%d Passed: %s", t.idx, t.msg)
      , error(format('#%d Failed: %s (%t != %t)', t.idx, t.msg, t.expected_row_count, @@row_count))
    );

  end for;

  exception when error then
    drop schema `zpreview__snapshot` cascade;
end;
