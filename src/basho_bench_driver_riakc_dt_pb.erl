% -------------------------------------------------------------------
%%
%% basho_bench_driver_riakc_dt_pb: Driver for riak protocol buffers client wrt
%%                                 to riak datatypes
%%
%% Copyright (c) 2016 Basho Techonologies
%%
%% This file is provided to you under the Apache License,
%% Version 2.0 (the "License"); you may not use this file
%% except in compliance with the License.  You may obtain
%% a copy of the License at
%%
%%   http://www.apache.org/licenses/LICENSE-2.0
%%
%% Unless required by applicable law or agreed to in writing,
%% software distributed under the License is distributed on an
%% "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
%% KIND, either express or implied.  See the License for the
%% specific language governing permissions and limitations
%% under the License.
%%
%% -------------------------------------------------------------------

-module(basho_bench_driver_riakc_dt_pb).

-export([new/1,
         run/4]).

-include("basho_bench.hrl").

-record(state, { pid,
                 bucket,
                 last_key,
                 batch_size,
                 preload,
                 preloaded_sets,
                 preloaded_sets_num,
                 last_preload_nth,
                 max_vals_for_preload,
                 run_one_set
               }).

-define(DEFAULT_SET_KEY, <<"bench_set">>).

new(Id) ->
    %% Make sure the path is setup such that we can get at riak_client
    case code:which(riakc_pb_socket) of
        non_existing ->
            ?FAIL_MSG("~s requires riakc_pb_socket module to be available on code path.\n",
                      [?MODULE]);
        _ ->
            ok
    end,

    Ips  = basho_bench_config:get(riakc_dt_pb_ips, [{127,0,0,1}]),
    Port  = basho_bench_config:get(riakc_dt_pb_port, 8087),
    Bucket  = basho_bench_config:get(riakc_dt_pb_bucket, {<<"riak_dt">>,
                                                          <<"test">>}),
    BatchSize = basho_bench_config:get(riakc_dt_pb_sets_batchsize, 1000),
    Preload = basho_bench_config:get(riakc_dt_preload_sets, false),
    PreloadNum = basho_bench_config:get(riakc_dt_preload_sets_num, 10),
    MaxValsForPreloadSet = basho_bench_config:get(
                             riakc_dt_max_vals_for_preload, 100),
    RunOneSet = basho_bench_config:get(riakc_dt_run_one_set, false),

    %% Choose the target node using our ID as a modulus
    Targets = basho_bench_config:normalize_ips(Ips, Port),
    lager:info("Ips: ~p Targets: ~p\n", [Ips, Targets]),
    {TargetIp, TargetPort} = lists:nth((Id rem length(Targets)+1), Targets),
    ?INFO("Using target ~p:~p for worker ~p\n", [TargetIp, TargetPort, Id]),
    case riakc_pb_socket:start_link(TargetIp, TargetPort, get_connect_options()) of
        {ok, Pid} ->
            PreloadedSets = case Preload of
                                true ->
                                    preload_sets(PreloadNum, Pid, Bucket);
                                false ->
                                    []
                            end,
            if RunOneSet ->
                    Set0 = riakc_set:new(),
                    %% can't be unmodified
                    Set1 = riakc_set:add_element(<<"69">>, Set0),
                    Result = riakc_pb_socket:update_type(
                               Pid, Bucket, ?DEFAULT_SET_KEY, riakc_set:to_op(Set1)
                              ),
                    case Result of
                        ok ->
                            ?INFO("~p created", [?DEFAULT_SET_KEY]);
                        {error, Reason} ->
                            ?ERROR("~p not created b/c ~p", [?DEFAULT_SET_KEY,
                                                             Reason])
                    end;
               true -> ok
            end,
            {ok, #state{ pid = Pid,
                         bucket = Bucket,
                         last_key=undefined,
                         batch_size = BatchSize,
                         preload = Preload,
                         preloaded_sets = PreloadedSets,
                         preloaded_sets_num = PreloadNum,
                         last_preload_nth = 0,
                         max_vals_for_preload = MaxValsForPreloadSet,
                         run_one_set = RunOneSet
                       }};
        {error, Reason} ->
            ?FAIL_MSG("Failed to connect riakc_pb_socket to ~p:~p: ~p\n",
                      [TargetIp, TargetPort, Reason])
    end.

run({set, insert}, _KeyGen, ValueGen, #state{pid=Pid, bucket=Bucket,
                                             run_one_set=true}=State) ->
    FetchResult = riakc_pb_socket:fetch_type(Pid, Bucket, ?DEFAULT_SET_KEY),
    case FetchResult of
        {ok, Set0} ->
            Val = ValueGen(),
            Set1 = riakc_set:add_element(Val, Set0),
            Result = riakc_pb_socket:update_type(Pid, Bucket, ?DEFAULT_SET_KEY,
                                                 riakc_set:to_op(Set1)),
            case Result of
                ok ->
                    {ok, State};
                {error, Reason} ->
                    {error, Reason, State}
            end;
        {error, {notfound, _}} ->
            {ok, State};
        {error, FetchReason} ->
            {error, FetchReason, State}
    end;
run({set, insert}, _KeyGen, ValueGen,
    #state{pid=Pid, bucket=Bucket, preload=true, preloaded_sets=PreloadedSets,
           preloaded_sets_num=PreloadedSetsNum, last_preload_nth=LastNth,
           max_vals_for_preload=MaxValsForPreloadSet}=State) ->
    NextNth = case LastNth >= PreloadedSetsNum of
                  true -> 1;
                  false -> LastNth + 1
              end,
    SetKey = lists:nth(NextNth, PreloadedSets),
    FetchResult = riakc_pb_socket:fetch_type(Pid, Bucket, SetKey),
    case FetchResult of
        {ok, Set0} ->
            Val = ValueGen(),
            SetSize = riakc_set:size(Set0),
            if SetSize < MaxValsForPreloadSet ->
                    Set1 = riakc_set:add_element(Val, Set0),
                    Result = riakc_pb_socket:update_type(Pid, Bucket, SetKey,
                                                         riakc_set:to_op(Set1)),
                    case Result of
                        ok ->
                            {ok, State#state{last_preload_nth=NextNth}};
                        {error, Reason} ->
                            {error, Reason, State}
                    end;
               true -> {ok, State#state{last_preload_nth=NextNth}}
            end;
        {error, {notfound, _}} ->
            {ok, State};
        {error, FetchReason} ->
            {error, FetchReason, State}
    end;
run({set, insert}, KeyGen, ValueGen, #state{pid=Pid, bucket=Bucket}=State) ->
    SetKey = KeyGen(),
    Val = ValueGen(),
    Set0 = riakc_set:new(),
    Set1 = riakc_set:add_element(Val, Set0),
    Result = riakc_pb_socket:update_type(Pid, Bucket, SetKey, riakc_set:to_op(Set1)),
    case Result of
        ok ->
            {ok, State};
        {error, Reason} ->
            {error, Reason, State}
    end;

run({set, batch_insert}, KeyGen, ValueGen, #state{pid=Pid, bucket=Bucket,
                                                  last_key=LastKey,
                                                  batch_size=BatchSize}=State) ->
    {SetKey, Members} = gen_set_batch(KeyGen, ValueGen, LastKey, BatchSize),
    Set0 = riakc_set:new(),
    SetLast = lists:foldl(fun(Elem, Sets) ->
                              Sets ++ [riakc_set:add_element(Elem, Set0)]
                         end, [], Members),
    Result = riakc_pb_socket:update_type(Pid, Bucket, SetKey,
                                         riakc_set:to_op(lists:last(SetLast))),
    case Result of
        ok ->
            {ok, State#state{last_key=SetKey}};
        {error, Reason} ->
            {error, Reason, State}
    end;
run({set, read}, KeyGen, _ValueGen, #state{pid=Pid, bucket=Bucket}=State) ->
    SetKey = KeyGen(),
    Result = riakc_pb_socket:fetch_type(Pid, Bucket, SetKey),
    case Result of
        {ok, _} ->
            {ok, State};
        {error, {notfound, _}} ->
            {ok, State};
        {error, Reason} ->
            {error, Reason, State}
    end;
run({set, remove}, KeyGen, ValueGen, #state{pid=Pid, bucket=Bucket}=State) ->
    SetKey = KeyGen(),
    Val = ValueGen(),
    {ok, Set0} = riakc_pb_socket:fetch_type(Pid, Bucket, SetKey),
    Set1 = riakc_set:del_element(Val, Set0),
    Result = riakc_pb_socket:update_type(Pid, Bucket, SetKey, riakc_set:to_op(Set1)),
    case Result of
        ok ->
            {ok, State};
        {error, Reason} ->
            {error, Reason, State}
    end;

run({set, is_element}, KeyGen, ValueGen, #state{pid=Pid, bucket=Bucket}=State) ->
    SetKey = KeyGen(),
    Val = ValueGen(),
    Result = riakc_pb_socket:fetch_type(Pid, Bucket, SetKey),
    case Result of
        {ok, Set} ->
            riakc_set:is_element(Val, Set),
            {ok, State};
        {error, {notfound, _}} ->
            {ok, State};
        {error, Reason} ->
            {error, Reason, State}
    end.

%%%===================================================================
%%% Private
%%%===================================================================

get_connect_options() ->
    basho_bench_config:get(pb_connect_options, [{auto_reconnect, true}]).

%% @private generate a tuple w/ a set-key and a batch of members from the valgen
gen_set_batch(KeyGen, ValueGen, LastKey, BatchSize) ->
    case {LastKey, gen_members(BatchSize, ValueGen)} of
        {_, []} ->
            %% Exhausted value gen, new key
            Key = KeyGen(),
            ?DEBUG("New set ~p~n", [Key]),
            basho_bench_keygen:reset_sequential_int_state(),
            {Key, gen_members(BatchSize, ValueGen)};
        {undefined, List} ->
            %% We have no active set, so generate a
            %% key. First run maybe
            Key = KeyGen(),
            ?DEBUG("New set ~p~n", [Key]),
            {Key, List};
        Else ->
            Else
    end.

%% @private generate as many elements as we can from the valgen, if it
%% exhausts, return the results we did get.
gen_members(BatchSize, ValueGen) ->
    accumulate_members(BatchSize, ValueGen, []).

%% @private generate as many elements as we can from the valgen, if it
%% exhausts, return the results we did get.
accumulate_members(0, _ValueGen, Acc) ->
    lists:reverse(Acc);
accumulate_members(BS, Gen, Acc) ->
    try
        accumulate_members(BS-1, Gen, [Gen() | Acc])
    catch throw:{stop, empty_keygen} ->
            ?DEBUG("ValGen exhausted~n", []),
            lists:reverse(Acc)
    end.

%% @private preload and update riak with an N-number of set keys,
%% named in range <<"1..Nset">>.
preload_sets(N, Pid, Bucket) ->
    SetKeys = [begin X1 = integer_to_binary(X),
                  Y = <<"set">>,
                  <<X1/binary,Y/binary>> end || X <- lists:seq(1, N)],
    [begin
         Set0 = riakc_set:new(),
         Set1 = riakc_set:add_element(<<"69">>, Set0),
         ok = riakc_pb_socket:update_type(Pid, Bucket, SetKey,
                                          riakc_set:to_op(Set1)),
         SetKey
     end || SetKey <- SetKeys].
