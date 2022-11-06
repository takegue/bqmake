create function `zsbx__prototyping.zassert`(expected ANY TYPE, actual ANY TYPE)
as (
  if(expected = actual, null, error(format("Expected %T: %T", expected, actual)))
);


begin
  select `zsbx__prototyping.zassert`('hoge', 'hoge')
end
