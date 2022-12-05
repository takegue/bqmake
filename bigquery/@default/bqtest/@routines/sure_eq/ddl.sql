create or replace function `bqtest.sure_eq`(actual ANY TYPE, expected ANY TYPE, msg STRING)
as (
  if(
    ifnull(expected = actual, expected is null and actual is null)
    , actual
    , error(format("Assertion failed: %s (actual: %T, expected: %T)", msg, expected, actual))
  )
);

begin
  call `bqtest.assert_golden`(
    (null, "bqtest", "zsnapshot_routines_all")
    , -- Profiling query
    `bqtest.zbqt_gensql__udf_snapshot`([
        "`bqtest.sure_eq`('hoge', 'hoge', 'string')"
        , "`bqtest.sure_eq`(('a', 'b'), ('a', 'b'), 'struct')"
        , "`bqtest.sure_eq`(format('%t', ['a', 'b']), format('%t', ['a', 'b']), 'array')"
        , "`bqtest.sure_eq`(null, null, 'null')"
      ]
      , "zsnapshot_routines_all"
    )
    , 'signature'
    , true
  );

  call `bqtest.should_error`("""
    select `bqtest.sure_eq`('hoge', 'fuga', "string")"""
  );
  call `bqtest.should_error`("""
    select `bqtest.sure_eq`(null, 'fuga', "string")"""
  );
end