CREATE TABLE IF NOT EXISTS `zgolden.zzsrepo__bqmake___v0___dataset__update_description`
(
  unique_key STRING,
  revision_hash STRING,
  valid_from TIMESTAMP,
  valid_to TIMESTAMP,
  entity STRUCT<catalog_name STRING, option_value STRING>
)
PARTITION BY DATE(valid_to)
CLUSTER BY valid_from;