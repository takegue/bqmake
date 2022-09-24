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

