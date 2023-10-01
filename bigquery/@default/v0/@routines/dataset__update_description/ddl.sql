create or replace procedure `v0.dataset__update_description`(
  in target_schemata array<string>
  , in lineage_parameter struct<
    `begin` timestamp
    , `end` timestamp
    , location string
  >
)
begin
  declare dst_ref string default @@project_id;

  -- Prepare Current Schema
  execute immediate format("""
    create or replace temp table `tmp_schema_options`
    as
    with schema_description as (
      select
        catalog_name
        , schema_name
        , string(parse_json(option_value)) as description
      from `%s.INFORMATION_SCHEMA.SCHEMATA_OPTIONS`
      where option_name = 'description'
    )
    select
      *
    from `%s.INFORMATION_SCHEMA.SCHEMATA`
    left join schema_description
      using(catalog_name, schema_name)
  """
    , dst_ref
    , dst_ref
  );

  begin
    -- Force create query reference for lineage generation
    execute immediate (
      select as value
        "create or replace temp table `_temp_tables` as "
        || string_agg(
          format("""
            select
              *
            from `%s.%s.INFORMATION_SCHEMA.TABLES`
            """
            , dst_ref
            , schema
          )
          , '\nunion all'
        )
      from unnest(ifnull(
        target_schemata
        , array(select distinct schema_name from tmp_schema_options)
      )) as schema
    )
    ;

    begin
      execute immediate (
        SELECT
          array_to_string(
            [
            'with'
            , string_agg(
                format(
                  "cte_%s as (select 1 from `%s.%s.%s`)"
                  , substr(generate_uuid(), 0, 6)
                  , table_catalog
                  , table_schema
                  , table_name
                )
                , '\n, '
              )
              , 'select 1'
            ]
            , '\n'
          )
        FROM `_temp_tables`
      );
    exception when error then
    end;
  end;

  execute immediate format("""
    create or replace temp table `tmp_lineage`
    as
      %s
    """
    , `bqmake.v0.zgensql__table_lineage`(
      @@project_id
      , ifnull(lineage_parameter.location, 'region-us')
      , null
    )
  ) using
    ifnull(lineage_parameter.`begin`, current_timestamp() - interval 30 day) as `begin`
    , ifnull(lineage_parameter.`end`, current_timestamp()) as `end`
    , 50 as `max_depth`
  ;

  -- Lineage Generation
  create or replace temp table `tmp_mermaid`
  as
    with
    datasource as (
      with _source as (
        select *
        from `tmp_lineage`
        left join unnest([struct(
          if(starts_with(dst_dataset, '_script'), '(#tenporary)', dst_dataset) as dst_dataset_n
          , if(starts_with(src_dataset, '_script'), '(#tenporary)', src_dataset) as src_dataset_n
        )])
        left join unnest([struct(
          substr(to_base64(md5(format('%s.%s.%s', dst_project, dst_dataset_n, dst_table))), 0, 4) as dst_hash
          , substr(to_base64(md5(format('%s.%s.%s', src_project, src_dataset_n, src_table))), 0, 4) as src_hash
          , split(destination, '.')[safe_offset(0)] || '.' ||  split(destination, '.')[safe_offset(1)] as unit
        )])
        where
          ifnull(not starts_with(src_table, 'INFORMATION_SCHEMA'), true)
      )
      , exclude_temp_dataset as (
        select
          destination, dst_project, dst_dataset, src_project, src_dataset
          , any_value(dst_dataset_n) as dst_dataset_n
          , any_value(src_dataset_n) as src_dataset_n
          , max(attrs.job_latest) as latest_ts
        from _source
        group by destination, dst_project, dst_dataset, src_project, src_dataset
        qualify 1 = row_number() over (
          partition by
            destination, dst_project, dst_dataset_n, src_project, src_dataset_n
          order by latest_ts desc
        )
      )
      select
        * replace(_source.dst_dataset_n as dst_dataset, _source.src_dataset_n as src_dataset)
      from _source
      join exclude_temp_dataset using(destination, dst_project, dst_dataset, src_project, src_dataset)
    )
    , mermaid_nodes as (
      with mermeid_dataset_subgraph as (
        select
          unit
          , project
          , dataset
          , format('subgraph "fa:fa-database %s"\n', dataset)
          || string_agg(distinct mermaid_node, '\n' order by mermaid_node)
          || '\nend'
          as mermaid_subgraph
        from datasource
        left join unnest([
          struct(dst_hash as _hash, dst_project as project, dst_dataset as dataset, dst_table as table)
          , struct(src_hash as _hash, src_project as project, src_dataset as dataset, src_table as table)
        ]) id
        left join unnest([
          format('\t%s(fa:fa-table %s)', _hash, table)
        ]) as mermaid_node
        group by unit, project, dataset
      )
      , mermaid_project_subgprah as (
        select
          unit
          , project
          , if(
              starts_with(unit, project)
              -- Project-intra lineage:
              , string_agg(mermaid_subgraph, '\n' order by dataset)
              -- Project-inter lineage:
              , format('subgraph "fa:fa-sitemap %s"\n', project)
                || string_agg(mermaid_subgraph, '\n' order by dataset)
                || '\nend'
           ) as mermaid_subgraph
        from mermeid_dataset_subgraph
        group by unit, project
      )
      select
        unit
        , string_agg(mermaid_subgraph, '\n' order by project) as nodes
      from mermaid_project_subgprah
      group by unit
    )
    , mermaid_relations as (
      SELECT
        unit
        , string_agg(
          distinct mermaid_relation, '\n' order by mermaid_relation
        ) as relations
      FROM datasource
      left join unnest([format('%s --> %s', src_hash, dst_hash)]) as mermaid_relation
      where depth >= 0
      group by unit
    )

    select
      unit
      , format("graph LR\n%s\n%s", nodes, relations) as mermaid
    from mermaid_nodes
    left join mermaid_relations using(unit)
  ;

  -- Update dataset description
  for record in (
    with newone as (
      select
        * replace(
          ltrim(array_to_string(
            [
              coalesce(substr(description, 0, header_position - 1) || substr(description, footer_position + char_length(footer) + 1), description)
            ]
            || if(
              mermaid is not null,
              [
                header
                , format('```mermaid\n%s\n```', mermaid)
                , footer
              ]
              , []
            )
            , '\n'
          ))
          as description
        )
      from tmp_schema_options
      join unnest(target_schemata) as schema_name
        using(schema_name)
      left join unnest([struct(
        '<!--- BQMAKE_DATASET: BEGIN -->' as header
        , '<!--- BQMAKE_DATASET: END -->' as footer
       )])
      left join unnest([struct(
        nullif(instr(description, header), 0) as header_position
        , nullif(instr(description, footer), 0) as footer_position
        , format('%s.%s', catalog_name, schema_name) as unit
      )])
      left join tmp_mermaid using(unit)
      where
       schema_name is not null
    )

    select
      catalog_name
      , schema_name
      , description
    from newone
  )
  do

    execute immediate format("""
      alter schema `%s.%s`
      set options (description=@description);
    """
      , record.catalog_name
      , record.schema_name
    )
      using record.description as description
    ;
  end for;
end;
