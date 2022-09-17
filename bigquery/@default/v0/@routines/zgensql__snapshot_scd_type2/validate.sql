declare destination struct<project_id string , dataset_id string, table_id string>;
declare unique_key string default 'station_id';
declare snapshot_query struct<create_ddl string, validate_ddl string, profile_query string, udpate_dml string, access_tvf_ddl string>;
declare previous_process_bytes int64;

set destination = ('project-id-7288898082930342315', "sandbox", "stations_scd_type2");

drop table if exists `project-id-7288898082930342315.sandbox.stations_scd_type2`;
execute immediate `bqmake.v0.zgensql__snapshot_scd_type2`(
    destination
    , "select * from `bigquery-public-data.austin_bikeshare.bikeshare_stations` limit 0"
    , unique_key
  ).create_ddl
    using timestamp '2022-01-01' as timestamp
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

  execute immediate `bqmake.v0.zgensql__snapshot_scd_type2`(
    destination
    , t.query
    , unique_key
  ).update_dml
    using timestamp '2022-01-02' as timestamp
  ;
  select if(
    t.expected_row_count = @@row_count
    , format("#%d Passed: %s", t.idx, t.msg)
    , error(format('#%d Failed: %s (%t != %t)', t.idx, t.msg, t.expected_row_count, @@row_count))
  );

  assert @@script.bytes_processed - previous_process_bytes < 32 * 1024
    as "UPDATE process bytes must be less than latest data size + update data size"
  ;
end for;
