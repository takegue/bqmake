begin
  declare name, init_sql, defer_sql string;
  set (name, init_sql, defer_sql) = (`bqmake.bqtest.zgensql__temporary_dataset`());

  execute immediate init_sql;
  /*
   * Lineage via temporary table
   *
   */
  create temp table `temp_table1`
  as select 1 as a from sandbox.sample_table
  ;

  create temp table `temp_table2`
  as select a * 10 as A from temp_table1, (select count(1) from sandbox.sample_clone_table)
  ;

  create or replace table `sandbox.sample_lineage`
  as select * from temp_table2;

  begin
    call `bqmake.v0.dataset__update_description`(
      [name]
      , (
        current_timestamp() - interval 1 hour
        , current_timestamp()
        , null
      ));
    exception when error then
      execute immediate defer_sql;
      raise using message = format('Failed to update dataset description: %s', @@error.message);
  end;
end
