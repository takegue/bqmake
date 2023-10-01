CREATE MATERIALIZED VIEW IF NOT EXISTS  `bqtest.demo_sample_partition_table__cache`
AS with datasource as (
  select
    week as partition_key
    , null as group_keys
    , *
  from `bqmake.bqtest.demo_sample_partition_table.`
)

, restricted_view as (
  select
    partition_key
    , group_keys
    , count(1) as count
    
    -- dma_name (f1)
    , countif(dma_name is not null) as dma_name__nonnull
    , approx_count_distinct(dma_name) as dma_name__unique
    , hll_count.init(dma_name) as dma_name__hll
    , avg(CHARACTER_LENGTH(dma_name)) as dma_name__avg_len
    , min(CHARACTER_LENGTH(dma_name)) as dma_name__min_len
    , max(CHARACTER_LENGTH(dma_name)) as dma_name__max_len
  

    -- dma_id (f2)
    , countif(dma_id is not null) as dma_id__nonnull
    , approx_count_distinct(dma_id) as dma_id__unique
    , hll_count.init(dma_id) as dma_id__hll
    , sum(cast(dma_id as bignumeric)) as dma_id__sum
    , sum(cast(dma_id as bignumeric) * cast(dma_id as bignumeric)) as dma_id__sum2
    , avg(dma_id) as dma_id__avg
    , min(dma_id) as dma_id__min
    , max(dma_id) as dma_id__max
  

    -- term (f3)
    , countif(term is not null) as term__nonnull
    , approx_count_distinct(term) as term__unique
    , hll_count.init(term) as term__hll
    , avg(CHARACTER_LENGTH(term)) as term__avg_len
    , min(CHARACTER_LENGTH(term)) as term__min_len
    , max(CHARACTER_LENGTH(term)) as term__max_len
  

    , countif(week is not null) as week__nonnull
    , hll_count.init(string(date(week))) as week__day_hll
    , min(week) as week__min
    , max(week) as week__max
  

    -- score (f5)
    , countif(score is not null) as score__nonnull
    , approx_count_distinct(score) as score__unique
    , hll_count.init(score) as score__hll
    , sum(cast(score as bignumeric)) as score__sum
    , sum(cast(score as bignumeric) * cast(score as bignumeric)) as score__sum2
    , avg(score) as score__avg
    , min(score) as score__min
    , max(score) as score__max
  

    -- rank (f6)
    , countif(rank is not null) as rank__nonnull
    , approx_count_distinct(rank) as rank__unique
    , hll_count.init(rank) as rank__hll
    , sum(cast(rank as bignumeric)) as rank__sum
    , sum(cast(rank as bignumeric) * cast(rank as bignumeric)) as rank__sum2
    , avg(rank) as rank__avg
    , min(rank) as rank__min
    , max(rank) as rank__max
  

    -- percent_gain (f7)
    , countif(percent_gain is not null) as percent_gain__nonnull
    , approx_count_distinct(percent_gain) as percent_gain__unique
    , hll_count.init(percent_gain) as percent_gain__hll
    , sum(cast(percent_gain as bignumeric)) as percent_gain__sum
    , sum(cast(percent_gain as bignumeric) * cast(percent_gain as bignumeric)) as percent_gain__sum2
    , avg(percent_gain) as percent_gain__avg
    , min(percent_gain) as percent_gain__min
    , max(percent_gain) as percent_gain__max
  

    , countif(refresh_date is not null) as refresh_date__nonnull
    , hll_count.init(string(date(refresh_date))) as refresh_date__day_hll
    , min(refresh_date) as refresh_date__min
    , max(refresh_date) as refresh_date__max
  
  from datasource
  group by partition_key, group_keys
)
select * from restricted_view;