begin
  declare temp_schema, init_sql, defer_sql string;
  set (temp_schema, init_sql, defer_sql) = (`v0.zgensql__temporary_dataset`(false));
  execute immediate init_sql;
  begin
    -- Prepare fixtures
    execute immediate format("""
      create table `%s.bikeshare_stations`
      like bigquery-public-data.austin_bikeshare.bikeshare_stations
      """
      , temp_schema
    );
    execute immediate format("""
      alter table `%s.bikeshare_stations`
        alter column station_id set options (description = 'station id')
      """
      , temp_schema
    );

    -- Scenario
    call `v0.partition_table__update`(
      (null, temp_schema, 'bikeshare_stations')
      , [('bigquery-public-data', "austin_bikeshare", "bikeshare_stations")]
      , [('__NULL__', ["__NULL__"])]
      , "select 1 as `new_column`, * from bigquery-public-data.austin_bikeshare.bikeshare_stations"
      , to_json(struct(
        "replace_if_changed" as auto_recreate
      ))
    );
    set @@dataset_id = temp_schema;
    assert exists(
      select 
        description
      from `INFORMATION_SCHEMA.COLUMN_FIELD_PATHS`
      where table_name = "bikeshare_stations"
        and column_name = 'station_id'
        and description is not null
    );

    -- Tear down fixtures
    execute immediate defer_sql;
  exception when error then
    execute immediate defer_sql;
    raise using message = @@error.message;
  end;
end;
