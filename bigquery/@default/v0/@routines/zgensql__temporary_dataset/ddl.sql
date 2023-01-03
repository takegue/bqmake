create or replace function `v0.zgensql__temporary_dataset`()
returns struct<name string, init_sql string, defer_sql string>
as ((
  select as struct
    name
    , format("create schema if not exists `%s` options(default_table_expiration_days=0.5)", name)
    , format("drop schema if exists `%s` cascade", name)
  from unnest([
    '_temp_' || replace(generate_uuid(), '-', '')
  ]) as name
));

begin
  declare name, init_sql, defer_sql string;
  set (name, init_sql, defer_sql) = (`v0.zgensql__temporary_dataset`());
  execute immediate init_sql;
  execute immediate defer_sql;
end
