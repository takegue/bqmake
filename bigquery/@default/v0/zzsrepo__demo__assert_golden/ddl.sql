CREATE TABLE IF NOT EXISTS `v0.zzsrepo__demo__assert_golden`
(
  unique_key INT64,
  revision_hash STRING,
  valid_from TIMESTAMP,
  valid_to TIMESTAMP,
  entity STRUCT<unique_key INT64, c STRING>
)
PARTITION BY DATE(valid_to)
CLUSTER BY valid_from;