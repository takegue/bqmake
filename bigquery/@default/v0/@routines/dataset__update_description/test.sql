declare name, init_sql, defer_sql string;
set (name, init_sql, defer_sql) = (
  "zztemp__dataset__update_description"
  , "create schema if not exists zztemp__dataset__update_description"
  , "drop schema if exists zztemp__dataset__update_description cascade"
);

begin
  execute immediate init_sql;
  /*
   * Lineage via temporary table
   */
  create temp table `temp_table1`
  as select 1 as a from `project-id-7288898082930342315.sandbox.sample_table`
  ;

  create temp table `temp_table2`
  as
    select a * 10 as A
    from
      temp_table1
      , (select count(1) from `project-id-7288898082930342315.sandbox.sample_clone_table`)
  ;

  -- Cyclic relation
  create or replace temp table `temp_table1`
  as
    select A as a from temp_table2
  ;

  execute immediate
    format(
      "create or replace table `%s.sample_lineage` as select * from temp_table2"
      , name
    );

    call `bqmake.v0.dataset__update_description`(
      [name]
      , (
        current_timestamp() - interval 1 hour
        , current_timestamp()
        , null
      ));

    call `bqmake.v0.assert_golden`(
      (@@project_id, "zgolden", "bqmake___v0___dataset__update_description")
      , format("select catalog_name, option_value from `%s.INFORMATION_SCHEMA.SCHEMATA_OPTIONS` where option_name = 'description' and schema_name = %T", @@project_id, name)
      , "format('%T', (catalog_name))"
      , @update_golden
    );
  execute immediate defer_sql;

exception when error then
  execute immediate defer_sql;
  raise using message = format('Failed to update dataset description: %s', @@error.message);
end
