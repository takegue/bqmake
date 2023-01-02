declare ret array<string>;
create schema if not exists `zpreview_test2`;

begin
  create or replace table `zpreview_test2.ref1`
  partition by date_jst
  as select date '2006-01-02' as date_jst
  ;

  create or replace table `zpreview_test2.dest1`
  partition by date_jst
  as
  select date '2006-01-02' as date_jst
  ;

  create or replace table `zpreview_test2.dest_no_partition`
  as
  select date '2006-01-02' as date_jst
  ;

  create or replace table `zpreview_test2.ref_20060102`
  partition by date_jst
  as select date '2006-01-02' as date_jst
  ;

  create or replace table `zpreview_test2.ref_20060103`
  partition by date_jst
  as select date '2006-01-03' as date_jst
  ;

  create or replace table `zpreview_test2.ref_no_partition`
  as select date '2006-01-02' as date_jst
  ;

  execute immediate
    `v0.zgensql__staleness_check`(
      (null, "zpreview_test2", "dest1")
      , []
      , [("__ANY__", ["__ANY__"])]
      , to_json(struct(interval 0 hour as tolerate_staleness, @@project_id as default_project_id))
    ) into ret
  ;

  assert ret[safe_offset(0)] is not null
    as "Empty sourcs will be always staleness";

  -- INFORMATIN_SCHEMA sourcs
  execute immediate
    `v0.zgensql__staleness_check`(
    (null, "zpreview_test2", "dest1")
    , [(string(null), "zpreview_test2.INFORMATION_SCHEMA", "PARTITIONS")]
    , [("__ANY__", ["__ANY__"])]
      , to_json(struct(interval 0 hour as tolerate_staleness, @@project_id as default_project_id))
    ) into ret
  ;

  assert ret[safe_offset(0)] is not null
    as "INFORMATIN_SCHEMA sourcs will get always staled ";

  execute immediate
    `v0.zgensql__staleness_check`(
    (null, "zpreview_test2", "dest1")
    , [(string(null), "zpreview_test2", "ref1")]
    , [("20060102", ["20060102"])]
      , to_json(struct(interval 0 hour as tolerate_staleness, @@project_id as default_project_id))
    ) into ret
  ;

  assert ret[safe_offset(0)] is null
    as "Not stale partition: dest1 > ref1"
  ;

  execute immediate
    `v0.zgensql__staleness_check`(
    (null, "zpreview_test2", "dest1")
    , [(string(null), "zpreview_test2", "ref1")]
    , [("20060102", ["20060102"])]
    , to_json(struct(
          interval 0 hour as tolerate_staleness
          , @@project_id as default_project_id
          , current_timestamp() as force_expired_at
      ))
    ) into ret
  ;

  assert ret[safe_offset(0)] = '20060102'
    as "Stale partition: force_expired_at option"
  ;

  execute immediate
    `v0.zgensql__staleness_check`(
    (null, "zpreview_test2", "dest1")
    , [
        (string(null), "zpreview_test2", "ref1")
        , (string(null), "zpreview_test2", "ref_20060103")
      ]
    , [("20060102", ["20060102"])]
    , to_json(struct(
          interval 0 hour as tolerate_staleness
          , @@project_id as default_project_id
      ))
    ) into ret
  ;

  assert ret[safe_offset(0)] is null
    as "Not stale partition: Inconsistent sources"
  ;

  execute immediate
    `v0.zgensql__staleness_check`(
    (null, "zpreview_test2", "dest1")
    , [
        (string(null), "zpreview_test2", "ref1")
        , (string(null), "zpreview_test2", "ref_20060103")
      ]
    , [("20060102", ["__ANY__"])]
    , to_json(struct(
          interval 0 hour as tolerate_staleness
          , @@project_id as default_project_id
      ))
    ) into ret
  ;

  assert ret[safe_offset(0)] = '20060102'
    as "Satle partition: __ANY__"
  ;

  execute immediate
    `v0.zgensql__staleness_check`(
      (null, "zpreview_test2", "dest1")
      , [
          (string(null), "zpreview_test2", "ref1")
          , (string(null), "zpreview_test2", "ref_20060102")
        ]
      , [("20060102", ["20060102"])]
      , to_json(struct(
            interval 0 hour as tolerate_staleness
            , @@project_id as default_project_id
        ))
    )
    into ret
  ;

  assert ret[safe_offset(0)] = '20060102'
    as "Stale partition under some source's partition is fresher than destination: ref_20060102 > dest1 > ref1"
  ;

  execute immediate
    `v0.zgensql__staleness_check`(
      (null, "zpreview_test2", "dest1")
      , [(string(null), "zpreview_test2", "ref_no_partition")]
      , [('20060102', ["__NULL__"])]
        , to_json(struct(
          interval 0 hour as tolerate_staleness
          , @@project_id as default_project_id
        ))
    )
    into ret
  ;


  assert ret[safe_offset(0)] = '20060102'
    as "Stale partition under non-partitioned source: dest1 > ref_no_partition"
  ;

  execute immediate
    `v0.zgensql__staleness_check`(
      (null, "zpreview_test2", "dest1")
      , [(string(null), "zpreview_test2", "ref_no_partition")]
      , [('20060102', ["20060102"])]
        , to_json(struct(
          interval 0 hour as tolerate_staleness
          , @@project_id as default_project_id
        ))
    )
    into ret
  ;

  assert ret[safe_offset(0)] = '20060102'
    as "Stale partition between alignment between Non-partition table and partition table"
  ;


  execute immediate
    `v0.zgensql__staleness_check`(
      (null, "zpreview_test2", "dest_no_partition")
      , [(string(null), "zpreview_test2", "ref_no_partition")]
      , [('__NULL__', ["__NULL__"])]
      , to_json(struct(
        interval 0 hour as tolerate_staleness
        , @@project_id as default_project_id
      ))
    )
    into ret
  ;

  assert ret[safe_offset(0)] = '__NULL__'
    as "Stale non-partitioned table under non-partitioned source: dest_no_partition > ref_no_partition"
  ;

  execute immediate
    `v0.zgensql__staleness_check`(
      (null, "zpreview_test2", "dest_no_partition")
      , [
        (string(null), "zpreview_test2", "ref_no_partition")
        , (string(null), "zpreview_test2", "INFORMATION_SCHEMA.VIEWS")
        , (string(null), "zpreview_test2", "INFORMATION_SCHEMA.PARTITIONS")
        , (string(null), "zpreview_test2", "INFORMATION_SCHEMA.TABLES")
      ]
      , [('__NULL__', ["__NULL__"])]
      , to_json(struct(
        interval 0 hour as tolerate_staleness
        , @@project_id as default_project_id
      ))
    )
    into ret
  ;

  assert ret[safe_offset(0)] = '__NULL__'
    as "Stale non-partitioned table under non-partitioned source: dest_no_partition > ref_no_partition"
  ;

  drop schema if exists `zpreview_test2` cascade;
exception when error then
  drop schema if exists `zpreview_test2` cascade;
  raise using message = @@error.message;
end
