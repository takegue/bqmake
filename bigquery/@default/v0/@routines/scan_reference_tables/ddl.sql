declare ret array<struct<project_id string, dataset_id string, table_id string>>;

create or replace procedure `fn.get_query_referenced_tables`(
  out ret array<struct<project_id string, dataset_id string, table_id string>>
  , in query string
  , in options JSON
)
options(
  description='''Get referenced tables by query

  Arguments:
  ===
    ret: output variable to store the referenced tables
    query: query to analyize
    optiosn:
      enable_query_rewrite: Enable dynamic query rewrite to minimize slot or billing scan amount (default: false)
      default_region: region-us (default: "region-us")
  '''
)
begin
  declare last_job_id string;
  declare enable_query_rewrite bool default ifnull(bool(options.enable_query_rewrite), false);
  declare default_region string default ifnull(string(options.default_region), "region-us");

  begin
    execute immediate
      if(
        enable_query_rewrite
        , format("with Q as (%s) select error('Intentional Error')", query)
        , query
      )
    ;
  exception when error then
  end;
  set last_job_id = @@last_job_id;

  execute immediate ifnull(format("""
      select
        if(
          cache_hit
          , error("Inproper reference due to cache_hit=true. Avoid to use query cached. Referer https://cloud.google.com/bigquery/docs/cached-results#cache-exceptions")
          , referenced_tables
        )
      from `%s.%s.INFORMATION_SCHEMA.JOBS_BY_USER`
      where
        job_id = "%s"
        and date(creation_time) = current_date()
      order by start_time desc
      limit 1
      """
      , @@project_id
      , ifnull(default_region, 'region-us')
      , last_job_id
    )
    , error(format("Invalid Argumenrts: %t", (@@project_id, default_region, last_job_id)))
  )
    into ret;
end
;

-- Test
call `fn.get_query_referenced_tables`(ret, "select * from sandbox.sample_view", JSON '{"enable_query_rewrite": true}');
select ret;
