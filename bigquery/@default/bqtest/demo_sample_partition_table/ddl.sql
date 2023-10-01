CREATE TABLE IF NOT EXISTS `bqtest.demo_sample_partition_table`
(
  dma_name STRING,
  dma_id INT64,
  term STRING,
  week DATE,
  score INT64,
  rank INT64,
  percent_gain INT64,
  refresh_date DATE
)
PARTITION BY week;