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

create or replace table `zpreview_test.ref2`
partition by date_jst
as select date '2006-01-03' as date_jst
;

create or replace table `zpreview_test.ref_20060102`
partition by date_jst
as select date '2006-01-02' as date_jst
;

create or replace table `zpreview_test.ref_no_partition`
as select date '2006-01-02' as date_jst
;


call `v0.detect_staleness`(
  ret
  , (null, "zpreview_test", "dest1")
  , [(string(null), "zpreview_test", "ref1")]
  , [("20060102", ["20060102"])]
  , null
);

assert ret[safe_offset(0)] is null;

call `v0.detect_staleness`(
  ret
  , (null, "zpreview_test", "dest2")
  , [(string(null), "zpreview_test", "ref_1")]
  , [("20060102", ["20060102"])]
  , to_json(struct(interval 0 hour as tolerate_staleness, null as null_value))
);

assert ret[safe_offset(0)] is null;


call `v0.detect_staleness`(
  ret
  , (null, "zpreview_test", "dest1")
  , [(string(null), "zpreview_test", "ref1")]
  , [("20060102", ["20060102"])]
  , to_json(struct(interval 0 hour as tolerate_staleness, null as null_value))
);

assert ret[safe_offset(0)] = '20060102';

call `v0.detect_staleness`(
  ret
  , (null, "zpreview_test", "dest1")
  , [
      (string(null), "zpreview_test", "ref1")
      , (string(null), "zpreview_test", "ref2")
    ]
  , [("20060102", ["20060102"])]
  , to_json(struct(interval 0 hour as tolerate_staleness, null as null_value))
);

assert ret[safe_offset(0)] is null
  as "invalidate destination partition under some source's partition is available";

call `v0.detect_staleness`(
  ret
  , (null, "zpreview_test", "dest1")
  , [(string(null), "zpreview_test", "ref_no_partition")]
  , [('20060102', ["__NULL__"])]
  , to_json(struct(interval 0 hour as tolerate_staleness, null as null_value))
);

assert ret[safe_offset(0)] = '20060102';

call `v0.detect_staleness`(
  ret
  , (null, "zpreview_test", "dest_no_partition")
  , [(string(null), "zpreview_test", "ref_no_partition")]
  , [('__NULL__', ["__NULL__"])]
  , to_json(struct(interval 0 hour as tolerate_staleness, null as null_value))
);

assert ret[safe_offset(0)] = '__NULL__';


drop schema if exists `zpreview_test` CASCADE;
