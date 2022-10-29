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
  execute immediate "create materialized view `zpreview.ingestion_table__materialized_index` as " || ifnull(ret, error("step#1 error"));

  call `bqmake.v0.zgensql__table_profiler`(
    ret, destination, null , to_json(struct(false as materialized_view_mode))
  );

  execute immediate "create view `zpreview.ingestion_table__view` as " || ifnull(ret, error("step#2 error"));
end;

drop schema zpreview cascade;
