declare ret array<string>;

create schema if not exists `zpreview_test`;

create or replace table `zpreview_test.ref1`
partition by date_jst
as select date '2006-01-02' as date_jst
;

create or replace table `zpreview_test.dest1`
partition by date_jst
as
select date '2006-01-02' as date_jst
;

create or replace table `zpreview_test.dest_no_partition`
as
select date '2006-01-02' as date_jst
;

create or replace table `zpreview_test.ref_20060102`
partition by date_jst
as select date '2006-01-02' as date_jst
;

create or replace table `zpreview_test.ref_20060103`
partition by date_jst
as select date '2006-01-03' as date_jst
;

create or replace table `zpreview_test.ref_no_partition`
as select date '2006-01-02' as date_jst
;


call `v0.detect_staleness`(
  ret
  , (null, "zpreview_test", "dest1")
  , [(string(null), "zpreview_test", "ref1")]
  , [("20060102", ["20060102"])]
  , to_json(struct(interval 0 hour as tolerate_staleness))
);

assert ret[safe_offset(0)] is null
  as "Not stale partition: dest1 > ref1"
;

call `v0.detect_staleness`(
  ret
  , (null, "zpreview_test", "dest1")
  , [(string(null), "zpreview_test", "ref1")]
  , [("20060102", ["20060102"])]
  , to_json(struct(interval 0 hour as tolerate_staleness, current_timestamp() as force_expire_at))
);

assert ret[safe_offset(0)] = '20060102'
  as "Stale partition: force_expire_at option"
;

call `v0.detect_staleness`(
  ret
  , (null, "zpreview_test", "dest1")
  , [
      (string(null), "zpreview_test", "ref1")
      , (string(null), "zpreview_test", "ref_20060103")
    ]
  , [("20060102", ["20060102"])]
  , to_json(struct(interval 0 hour as tolerate_staleness))
);

assert ret[safe_offset(0)] is null
  as "Not stale partition: Inconsistent sources"
;

call `v0.detect_staleness`(
  ret
  , (null, "zpreview_test", "dest1")
  , [
      (string(null), "zpreview_test", "ref1")
      , (string(null), "zpreview_test", "ref_20060103")
    ]
  , [("20060102", ["__ANY__"])]
  , to_json(struct(interval 0 hour as tolerate_staleness))
);

assert ret[safe_offset(0)] = '20060102'
  as "Satle partition: __ANY__"
;


call `v0.detect_staleness`(
  ret
  , (null, "zpreview_test", "dest1")
  , [
      (string(null), "zpreview_test", "ref1")
      , (string(null), "zpreview_test", "ref_20060102")
    ]
  , [("20060102", ["20060102"])]
  , to_json(struct(interval 0 hour as tolerate_staleness))
);

assert ret[safe_offset(0)] = '20060102'
  as "Stale partition under some source's partition is fresher than destination: ref_20060102 > dest1 > ref1"
;

call `v0.detect_staleness`(
  ret
  , (null, "zpreview_test", "dest1")
  , [(string(null), "zpreview_test", "ref_no_partition")]
  , [('20060102', ["__NULL__"])]
  , to_json(struct(interval 0 hour as tolerate_staleness))
);

assert ret[safe_offset(0)] = '20060102'
  as "Stale partition under non-partitioned source: dest1 > ref_no_partition"
;

call `v0.detect_staleness`(
  ret
  , (null, "zpreview_test", "dest1")
  , [(string(null), "zpreview_test", "ref_no_partition")]
  , [('20060102', ["20060102"])]
  , to_json(struct(interval 0 hour as tolerate_staleness))
);

assert ret[safe_offset(0)] = '20060102'
  as "Stale partition between alignment between Non-partition table and partition table"
;


call `v0.detect_staleness`(
  ret
  , (null, "zpreview_test", "dest_no_partition")
  , [(string(null), "zpreview_test", "ref_no_partition")]
  , [('__NULL__', ["__NULL__"])]
  , to_json(struct(interval 0 hour as tolerate_staleness))
);

assert ret[safe_offset(0)] = '__NULL__'
  as "Stale non-partitioned table under non-partitioned source: dest_no_partition > ref_no_partition"
;

drop schema if exists `zpreview_test` CASCADE;
