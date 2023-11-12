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

  execute immediate (
    select as value
      "with "
      || string_agg(
        format('cte%d as (%s) ', ix, sql)
        , ',\n'
      )
      || format("""
        select array<struct<
          expected array<string>
          , actual array<string>>
        >[
          %s
        ]
      """
      , string_agg(
        format('(select as value struct(cast(%T as array<string>) as expected, cast(cte%d as struct<ret array<string>>).ret as actual) from cte%d)', expected, ix, ix)
        , ',\n'
      ))
    from unnest(
      array<struct< msg string, expected array<string>,sql string>>[
        (
          "Empty sourcs will be always staleness"
          , ['__ANY__']
          , `v0.zgensql__staleness_check`(
            (null, "zpreview_test2", "dest1")
            , []
            , [("__ANY__", ["__ANY__"])]
            , to_json(struct(interval 0 hour as tolerate_staleness, @@project_id as default_project_id))
          )
        )
        , (
          "INFORMATIN_SCHEMA sourcs will get always staled"
          , ['__ANY__']
          , `v0.zgensql__staleness_check`(
            (null, "zpreview_test2", "dest1")
            , [(string(null), "zpreview_test2.INFORMATION_SCHEMA", "PARTITIONS")]
            , [("__ANY__", ["__ANY__"])]
              , to_json(struct(interval 0 hour as tolerate_staleness, @@project_id as default_project_id))
          )
        )
        , (
          "Not stale partition: dest1 > ref1"
          , []
          , `v0.zgensql__staleness_check`(
            (null, "zpreview_test2", "dest1")
            , [(string(null), "zpreview_test2", "ref1")]
            , [("20060102", ["20060102"])]
              , to_json(struct(interval 0 hour as tolerate_staleness, @@project_id as default_project_id))
          )
        )
        , (
          "Stale partition: force_expired_at option"
          , ["20060102"]
          , `v0.zgensql__staleness_check`(
            (null, "zpreview_test2", "dest1")
            , [(string(null), "zpreview_test2", "ref1")]
            , [("20060102", ["20060102"])]
            , to_json(struct(
                  interval 0 hour as tolerate_staleness
                  , @@project_id as default_project_id
                  , current_timestamp() as force_expired_at
              ))
          )
        )
        , (
          "Stale partition: Inconsistent sources"
          , ["20060102"]
          , `v0.zgensql__staleness_check`(
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
          )
        )
        , (
          "Satle partition: __ANY__"
          , []
          , `v0.zgensql__staleness_check`(
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
          )
        )
        , (
          "Stale partition under some source's partition is fresher than destination: ref_20060102 > dest1 > ref1"
          , ['20060102']
          , `v0.zgensql__staleness_check`(
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
        )
        , (
          "Stale partition under non-partitioned source: dest1 > ref_no_partition"
          , ["20060102"]
          , `v0.zgensql__staleness_check`(
            (null, "zpreview_test2", "dest1")
            , [(string(null), "zpreview_test2", "ref_no_partition")]
            , [('20060102', ["__NULL__"])]
              , to_json(struct(
                interval 0 hour as tolerate_staleness
                , @@project_id as default_project_id
              ))
          )
        )
        , (
          "Stale partition between alignment between Non-partition table and partition table"
          , ['20060102']
          , `v0.zgensql__staleness_check`(
            (null, "zpreview_test2", "dest1")
            , [(string(null), "zpreview_test2", "ref_no_partition")]
            , [('20060102', ["20060102"])]
              , to_json(struct(
                interval 0 hour as tolerate_staleness
                , @@project_id as default_project_id
              ))
          )
        )
        , (
          "Stale non-partitioned table under non-partitioned source: dest_no_partition > ref_no_partition"
          , ['__NULL__']
          , `v0.zgensql__staleness_check`(
            (null, "zpreview_test2", "dest_no_partition")
            , [(string(null), "zpreview_test2", "ref_no_partition")]
            , [('__NULL__', ["__NULL__"])]
            , to_json(struct(
              interval 0 hour as tolerate_staleness
              , @@project_id as default_project_id
            ))
          )
        )
        , (
          "Stale non-partitioned table under non-partitioned source: dest_no_partition > ref_no_partition"
          , ['__NULL__']
          , `v0.zgensql__staleness_check`(
            (null, "zpreview_test2", "dest_no_partition")
            , [
              (string(null), "zpreview_test2", "ref_no_partition")
              , (string(null), "zpreview_test2.INFORMATION_SCHEMA", "VIEWS")
              , (string(null), "zpreview_test2.INFORMATION_SCHEMA", "PARTITIONS")
              , (string(null), "zpreview_test2.INFORMATION_SCHEMA", "TABLES")
            ]
            , [('__NULL__', ["__NULL__"])]
            , to_json(struct(
              interval 0 hour as tolerate_staleness
              , @@project_id as default_project_id
            ))
          )
        )
      ]
    ) with offset ix
  )
  ;


  -- drop schema if exists `zpreview_test2` cascade;
exception when error then
  -- drop schema if exists `zpreview_test2` cascade;
  raise using message = @@error.message;
end
