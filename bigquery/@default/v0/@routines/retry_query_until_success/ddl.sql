create or replace procedure `bqmake.v0.retry_query_until_success`(
  query string
  , timeout interval
)
begin
  declare retry_interval_seconds int64 default 15;
  declare started_at, waited_at timestamp;

  set started_at = current_timestamp();
  while ifnull(timeout, interval 10 minute) > current_timestamp() - started_at do
    begin
      execute immediate query;
      return;
     exception when error then
    end;

    set waited_at = current_timestamp();
    while retry_interval_seconds > timestamp_diff(current_timestamp(), waited_at, second) do
    end while;
    set retry_interval_seconds = retry_interval_seconds * 2;
  end while;

  raise using message = "Timeout";
end;
