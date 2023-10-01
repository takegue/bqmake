CREATE TABLE IF NOT EXISTS `bqtest.zzsrepo__zgolden_routines`
(
  unique_key STRING,
  revision_hash STRING,
  valid_from TIMESTAMP,
  valid_to TIMESTAMP,
  entity STRUCT<signature STRING, ret STRING>
)
PARTITION BY DATE(valid_to)
CLUSTER BY valid_from;