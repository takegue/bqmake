Extracts partitions that are stale.

Argument
===

- destination          : destination table
- sources              : source tables
- partition_alignments : partition alignments
- options              : option values in json format
  * default_project_id : default_project_id
  * tolerate_staleness : if the partition is older than this value (Default: interval 0 minute)
  *    force_expired_at: The timestamp to force expire partitions. If the destination's partition timestamp is older than this timestamp, the procedure stale the partitions. [Default: NULL].


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
