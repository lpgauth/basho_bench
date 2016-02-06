-module(basho_bench_driver_shackle).
-include_lib("basho_bench/include/basho_bench.hrl").

-export([
    new/1,
    run/4
]).

%% public
new(_Id) ->
    shackle_app:start(),
    arithmetic_tcp_client:start(),
    [5 = arithmetic_tcp_client:add(2, 3) || _ <- lists:seq(1, 25)],
    {ok, undefined}.

run(add, _Key, _Value, State) ->
    5 = arithmetic_tcp_client:add(2, 3),
    {ok, State}.
