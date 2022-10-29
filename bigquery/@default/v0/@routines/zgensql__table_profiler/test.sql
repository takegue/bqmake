create schema if not exists `zpreview`;

-- ingestion_table
create or replace table `zpreview.ingestion_table` (date_jst date)
partition by _PARTITIONDATE
;
insert into `zpreview.ingestion_table` (
  _PARTITIONTIME, date_jst
)
select timestamp('2022-10-01'), current_date('Asia/Tokyo')
;

begin
  declare ret string default null;
  declare destination struct<project string, dataset string, table string> default (null, 'zpreview', 'ingestion_table');
  declare group_keys array<string> default null;
  declare options_json json default null;
  call `bqmake.v0.zgensql__table_profiler`(
    ret, destination, null , to_json(struct(true as materialized_view_mode))
  );
  assert ret is not null
    as "zpreview.ingestion_table: Profiler SQL Generation for materialized_view_mode is failed";
  execute immediate "create materialized view `zpreview.ingestion_table__materialized_index` as " || ret;

  call `bqmake.v0.zgensql__table_profiler`(
    ret, destination, null , to_json(struct(false as materialized_view_mode))
  );
  assert ret is not null
    as "zpreview.ingestion_table: Profiler SQL Generation for view is failed";
  execute immediate "create view `zpreview.ingestion_table__view` as " || ret;
end;

-- sharding tables
begin
  declare ret string default null;
  declare destination struct<project string, dataset string, table string> default ("bigquery-public-data", "ga4_obfuscated_sample_ecommerce", "events_*");
  declare group_keys array<string> default null;
  declare options_json json default null;
  call `bqmake.v0.zgensql__table_profiler`(
    ret, destination, null , to_json(struct(true as materialized_view_mode))
  );
  assert ret is not null
    as "bigquery-public-data.ga4_obfuscated_sample_ecommerce.events_*: Profiler SQL Generation is failed";
  execute immediate format('with _dry_run as (%s) select 1', ret);
end;

drop schema zpreview cascade;
