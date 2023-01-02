CREATE OR REPLACE PROCEDURE `v0.detect_staleness`(
  out ret array<string>
  , destination struct<project_id string, dataset_id string, table_id string>
  , sources array<struct<project_id string, dataset_id string, table_id string>>
  , partition_alignments array<struct<destination string, sources array<string>>>
  , options_json json
)
OPTIONS(
  description="""Extracts partitions that are stale.

Argument
===

- ret                  : Output variable. array of staled partition like 20220101
- destination          : destination table
- sources              : source tables
- partition_alignments : partition alignments
- options              : option values in json format
  * tolerate_staleness : if the partition is older than this value (Default: interval 0 minute)
  *   force_expired_at : The timestamp to force expire partitions. If the destination's partition timestamp is older than this timestamp, the procedure stale the partitions. [Default: NULL].


Stalenss and Stablity Margin Checks
===

Case 1: Partition staleness with tolerate_staleness option
                     past                              now
Source Table        : |       |               |         |
                              ^ Refresh
Staleness Timeline  : | Fresh | Ignore(Fresh) |  Stale  |
                      +-----------------------^ tolerate staleness


Case 2: Partition staleness timeline with force_expired_at option
                     past                              now
Source Table        : | Fresh                           |
Staleness Timeline  : | Fresh | Stale                   |
                              ^ force_expired_at
""")
begin
  execute immediate `v0.zgensql__staleness_check`(
    destination
    , sources
    , partition_alignments
    , to_json(struct(
      safe.timestamp(safe.string(options_json.force_expired_at)) as force_expired_at
      , ifnull(cast(safe.string(options_json.tolerate_staleness) as interval), interval 30 minute) as tolerate_staleness
      , @@project_id as default_project_id
    ))
  ) into ret
end;