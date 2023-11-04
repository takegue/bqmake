CREATE OR REPLACE FUNCTION `v0.zgensql__table_recreation`(
  target_table struct<catalog string, schema string, name string>
  , new_query string
)
OPTIONS(
  description="""Private function to build DDL for table recreation with metadata.

`CREATE TABLE LIKE` is not suitable when the source table query and target table schema is different.
This SQL generator's goal is to generate DDL for table recreation with metadata like `CREATE TABLE LIKE` operator.
"""
)
AS (REPLACE(REPLACE(REPLACE(REPLACE(
  r"""
  with table_definition as (
    select as value 
      ddl 
    from `!TABLE_SCHEMA!.INFORMATION_SCHEMA.TABLES`
    where table_name = !TABLE_NAME!
  )
  , alter_column_options as (
    select as value
      "ALTER TABLE `!TABLE_IDENTITY!`\n"
      || string_agg(
        format(
          "  ALTER COLUMN IF EXISTS `%s` SET OPTIONS(%s)"
          , field_path
          , array_to_string([
            'description=' || format('%T', description)
            , 'rounding_mode=' || format('%s', rounding_mode)
          ], ',')
        )
        , ',\n'
      )
    from `!TABLE_SCHEMA!.INFORMATION_SCHEMA.COLUMN_FIELD_PATHS`
    where table_name = !TABLE_NAME!
      and (description is not null or rounding_mode is not null)
      -- ALTER COLUMN syntax don't support subfields structure
      and field_path = column_name
  )

  select as value
    struct(
      array_to_string(
        [
        "CREATE OR REPLACE TABLE `!TABLE_IDENTITY!`"
      ]
      || ifnull(
          array(
            select as value line 
            from unnest(split(table_definition, '\n')) as line
            where 
              starts_with(line, 'PARTITION BY') 
              or starts_with(line, 'CLUSTER BY')
              or starts_with(line, 'DEFAULT COLLATE')
          )
        , []
      )
      || (
        [
          "OPTIONS("
        ] 
        || table_options
        || [")"]
      )
      || [
        "AS",
        !TABLE_NEW_QUERY!
      ]
      , '\n')
      as new_table_ddl
      , alter_column_options as column_metadata_ddl
    )
  from table_definition, alter_column_options
  left join unnest([struct(
    ifnull(
        array(
          select as value line 
          from unnest(split(table_definition, '\n')) as line
          where 
            starts_with(line, '  description=')
        )
      , []
    ) as table_options
  )])
"""
  , '!TABLE_SCHEMA!', coalesce(format('%s.%s', target_table.catalog, target_table.schema), target_table.schema, error('Invalid schema argument')))
  , '!TABLE_NAME!', format('%T', target_table.name))
  , '!TABLE_IDENTITY!', coalesce(
      format('%s.%s.%s', target_table.catalog, target_table.schema, target_table.name)
      , format('%s.%s', target_table.schema, target_table.name)
      , format('%s', target_table.name)
      , error('Invalid schema argument')))
  , '!TABLE_NEW_QUERY!', format('%T', new_query))
);

begin
  call `bqmake.v0.assert_golden`(
    ("bqmake", "bqtest", "zgolden_routines"),  -- Profiling query
    `bqtest.zbqt_gensql__udf_snapshot`(
      [
        """`v0.zgensql__table_recreation`(("bigquery-public-data", "san_francisco", "bikeshare_stations"), "SELECT *, 'new' as n FROM `bigquery-public-data.san_francisco.bikeshare_stations` LIMIT 0")"""
      ],
      "zgolden_routines"
    ),
    'signature',
    true
  );
end
