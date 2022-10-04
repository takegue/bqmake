create or replace procedure `bqmake.v0.dataset__update_description`(
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
  ;

  -- Lineage Generation
  create or replace temp table `tmp_mermaid`
  as
    with datasource as (
      select *
      from `tmp_lineage`
      left join unnest([struct(
        substr(to_base64(md5(format('%s.%s.%s', dst_project, dst_dataset, dst_table))), 0, 4) as dst_hash
        , substr(to_base64(md5(format('%s.%s.%s', src_project, src_dataset, src_table))), 0, 4) as src_hash
        ,
          split(destination, '.')[safe_offset(0)] || '.' ||  split(destination, '.')[safe_offset(1)] as unit
      )])
    where
      ifnull(not starts_with(src_table, 'INFORMATION_SCHEMA'), true)
    )
    , mermaid_nodes as (
      with mermeid_dataset_subgraph as (
        select
          unit
          , format('subgraph %s.%s\n', project, dataset)
          || string_agg(
            distinct format('\t%s(%s)', _hash, table)
            , '\n'
          )
          || '\nend'
          as mermaid_subgraph
        from datasource
        left join unnest([
          struct(dst_hash as _hash, dst_project as project, dst_dataset as dataset, dst_table as table)
          , struct(src_hash as _hash, src_project as project, src_dataset as dataset, src_table as table)
        ]) id
        group by unit, project, dataset
      )
      select
        unit
        , string_agg(mermaid_subgraph, '\n') as nodes
      from mermeid_dataset_subgraph
      group by unit
    )
    , mermaid_relations as (
      SELECT
        unit
        , string_agg(
          distinct format('%s --> %s', src_hash, dst_hash)
          , '\n'
        ) as relations
      FROM datasource
      where depth >= 0
      group by unit
    )

    SELECT
      unit
      , format("graph LR\n%s\n%s", nodes, relations) as mermaid
    FROM mermaid_nodes
    left join mermaid_relations using(unit)
  ;

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
      left join unnest(target_schemata) as schema_name
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

-- Unit test
begin
  call `bqmake.v0.dataset__update_description`(null, null);
end
