Procedure to check partition stalesns and update partitions if needed.

Arguments
====

- destination: The destination table to check and update partitions.
-     sources: The source tables of destination table referecend by update_job.query. The procedure will check if the source tables have new partitions.
               If null is given, the procedure will automatically detect the source tables from update_job.query.
-  update_job:
  * unique_key: The unique key of the update job. The procedure will check if the update job is already running.
  * query: The query to update the destination table.
  * snapshot_timestamp: The timestamp to use for the snapshot. If null is given, the procedure will use the current timestamp.
- options: JSON value
  * dry_run: Whether to run the update job as a dry run. [Default: false].
  * tolerate_delay: The delay to tolerate before updating partitions. If newer source partitions are found but its timestamp is within this delay, the procedure will not update partitions. [Default: 30 minutes].
  * force_expired_at: The timestamp to force expire partitions. If the destination's partition timestamp is older than this timestamp, the procedure stale the partitions. [Default: null].
  * bq_location: BigQuery Location of job. This is used for query analysis to get dependencies. [Default: "region-us"]

Examples
===

```
call `bqmake.v0.snapshot_table__check_and_update`(
  destination
  , null
  (
    "staion_id"
    , "select * from `bigquery-public-data.austin_bikeshare.bikeshare_stations` limit 0"
    , current_timestamp()
  )
  , to_json(struct(
    current_timestamp() as force_expired_at
  ))
)
```
