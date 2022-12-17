create or replace procedure `bqtest.should_error`(
  query string
)
begin
  execute immediate query;
  assert false;
  exception when error then
    -- Error log
    select
      @@error.message
      , @@error.statement_text
      , @@error.formatted_stack_trace
      , @@error.stack_trace
    ;
    if trim(query) != @@error.statement_text then
       raise using message = format('Query: %T must throw exception', query);
    else
       select "bqtest.should_errro", query, @@error.message as error_message;
    end if;
end;

call `bqtest.should_error`("select error('error')");
