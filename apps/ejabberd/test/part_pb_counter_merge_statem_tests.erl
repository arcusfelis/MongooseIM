-module(part_pb_counter_merge_statem_tests).

-ifdef(TEST).
-ifdef(PROPER).
-include_lib("proper/include/proper.hrl").
-include_lib("eunit/include/eunit.hrl").

-behaviour(proper_statem).

%% Behaviour callbacks
-export([initial_state/0, command/1,
         precondition/2, postcondition/3, next_state/3]).

-export([merge/2]).


-record(state, {
    %% Control state.
    s,
    counters = [],
    total_value = 0,
    group_names = []}).
-define(M, part_pb_counter).

%% ------------------------------------------------------------------
%% Type Generators
%% ------------------------------------------------------------------

save_counter(ClientId, Counter, Counters) ->
    [{ClientId, Counter}|Counters].

next_client_id(Counters) ->
    Pos = length(Counters),
    <<Pos:32>>.
%   {Mega, Secs, Micro} = now(),
%   Total = Mega * 1000000 * 1000000 + Secs * 1000000 + Micro,
%   <<Total:64>>.

client_id_and_counter(Counters) ->
    ?LET({ClientId, Counter}, oneof(Counters), [ClientId, Counter]).

update_counter(ClientId, NewCounter, Counters) ->
    lists:keyreplace(ClientId, 1, Counters, {ClientId, NewCounter}).

group_name() ->
    binary(4).

group_name(#state{group_names = Names = [_|_]}) ->
    oneof([group_name()|Names]);
group_name(#state{group_names = []}) ->
    group_name().

merge(CountersToMerge, _CountersToReplace) ->
    ?M:merge([Counter || {_, Counter} <- CountersToMerge]).

%% `CountersToReplace' is a subset of `OldCounters' with the same order.
replace_counters(V, [{ClientId, _}|CountersToReplace], [{ClientId,_}|OldCounters]) ->
    [{ClientId, V}|replace_counters(V, CountersToReplace, OldCounters)];
replace_counters(V, CountersToReplace, [OldCounter|OldCounters]) ->
    %% Left the current old counter as it is.
    [OldCounter|replace_counters(V, CountersToReplace, OldCounters)];
replace_counters(_, [], []) ->
    [].

subset(Cs) ->
    Len = length(Cs),
    All = lists:seq(1, Len),
    ?LET(Positions, vector(Len, oneof(All)),
         [lists:nth(Pos, Cs) || Pos <- lists:usort(Positions)]).


%% ------------------------------------------------------------------
%% Simple PB-counter
%% ------------------------------------------------------------------

spbc_new() ->
    dict:new().

spbc_increment(ClientId, N, Dict) when N > 0 ->
    dict:update_counter({pos, ClientId}, N, Dict).

spbc_decrement(ClientId, N, Dict) when N > 0 ->
    dict:update_counter({neg, ClientId}, N, Dict).

spbc_merge([Dict|Dicts]) ->
    lists:foldl(fun spbc_merge/2, Dict, Dicts).

spbc_merge(D1, D2) ->
    dict:merge(fun(_Key, V1, V2) -> max(V1, V2) end, D1, D2).

spbc_value(SC) ->
    dict:fold(fun({pos, _}, Value, Acc) -> Acc + Value;
                 ({neg, _}, Value, Acc) -> Acc - Value
              end, 0, SC).

%% not io-list
spbc_format(SC) ->
    lists:usort(dict:to_list(SC)).


s_init() ->
    dict:new().

s_new(ClientId, SS) ->
    dict:store(ClientId, spbc_new(), SS).
    
s_merge_all(SS) ->
    SCS = [SC || {_ClientId, SC} <- dict:to_list(SS)],
    spbc_merge(SCS).

s_merge(CountersToMerge, CountersToReplace, SS) ->
    SCS = [dict:fetch(ClientId, SS) || {ClientId, _} <- CountersToMerge],
    SC = spbc_merge(SCS),
    ClientIdsToReplace = [ClientId || {ClientId, _} <- CountersToReplace],
    lists:foldl(fun(ClientId, SS1) -> dict:store(ClientId, SC, SS1) end,
                SS, ClientIdsToReplace).

s_increment(ClientId, N, SS) ->
    SC = dict:fetch(ClientId, SS),
    SC1 = spbc_increment(ClientId, N, SC),
    dict:store(ClientId, SC1, SS).

s_decrement(ClientId, N, SS) ->
    SC = dict:fetch(ClientId, SS),
    SC1 = spbc_decrement(ClientId, N, SC),
    dict:store(ClientId, SC1, SS).

s_value(ClientId, SS) ->
    SC = dict:fetch(ClientId, SS),
    spbc_value(SC).

s_format(SS) ->
    K2L = [{K, spbc_format(SC)}
           || {K, SC} <- lists:usort(dict:to_list(SS))],
    io_lib:format("~w", [K2L]).

s(#state{s=SS}) ->
    SS.

%% ------------------------------------------------------------------
%% Callbacks
%% ------------------------------------------------------------------

initial_state() -> 
    #state{s=s_init()}.

command(#state{counters = []}) ->
    {call, ?M, new, []};

command(#state{counters = Cs} = S) ->
    Inc = {call, ?M, increment, [group_name(S), pos_integer()|client_id_and_counter(Cs)]},
    Dec = {call, ?M, decrement, [group_name(S), pos_integer()|client_id_and_counter(Cs)]},
    Merge = {call, ?MODULE, merge, ?LET(Sub, subset(Cs), [subset(Sub), Sub])},
    New = {call, ?M, new, []},
    Freqs =
    [ {30, New}
    , {50, Inc}
    , {50, Dec}
    | [{50, Merge} || Cs =/= []] %% Merge, if `Cs' is non-empty list.
    ],
    frequency(Freqs).


next_state(S, V, {call, ?M, new, []}) ->
    ClientId = next_client_id(S#state.counters),
    assert_correct_update(S,
    S#state{s=s_new(ClientId, s(S)),
            counters = save_counter(ClientId, V, S#state.counters)});

next_state(S, V, {call, ?M, increment, [GroupName, N, ClientId, _]}) ->
    assert_known_client_id(ClientId, S),
    assert_correct_update(S,
    S#state{s=s_increment(ClientId, N, s(S)),
            counters = update_counter(ClientId, V, S#state.counters),
            total_value = S#state.total_value + N,
            group_names = ordsets:add_element(GroupName, S#state.group_names)});

next_state(S, V, {call, ?M, decrement, [GroupName, N, ClientId, _]}) ->
    assert_known_client_id(ClientId, S),
    assert_correct_update(S,
    S#state{s=s_decrement(ClientId, N, s(S)),
            counters = update_counter(ClientId, V, S#state.counters),
            total_value = S#state.total_value - N,
            group_names = ordsets:add_element(GroupName, S#state.group_names)});

next_state(S, V, {call, ?MODULE, merge, [CountersToMerge, CountersToReplace]}) ->
    assert_subset(CountersToMerge, S#state.counters),
    assert_subset(CountersToReplace, S#state.counters),
    assert_subset(CountersToReplace, CountersToMerge),
    assert_correct_update(S,
    S#state{s=s_merge(CountersToMerge, CountersToReplace, s(S)),
            counters = replace_counters(V, CountersToReplace, S#state.counters)}).


precondition(_, {call, ?M, new, []}) ->
    true;
precondition(S, {call, ?M, increment, [_, _, ClientId, _]}) ->
    is_known_client_id(ClientId, S);
precondition(S, {call, ?M, decrement, [_, _, ClientId, _]}) ->
    is_known_client_id(ClientId, S);
precondition(S, {call, ?MODULE, merge, [CountersToMerge, CountersToReplace]}) ->
    lists:all(fun(ClientId) -> is_known_client_id(ClientId, S) end,
              clients(CountersToMerge ++ CountersToReplace))
    andalso is_subset(CountersToMerge, S#state.counters)
    andalso is_subset(CountersToReplace, S#state.counters)
    andalso is_subset(CountersToReplace, CountersToMerge).


%% `postcondition/3' is called BEFORE `next_state/3'.
postcondition(S, {call, ?M, increment, [_, N, ClientId, _]}, R) ->
    assert_known_client_id(ClientId, S),
    SS = s_increment(ClientId, N, s(S)),
    assert_equal(s_value(ClientId, SS), ?M:value(R));
postcondition(S, {call, ?M, decrement, [_, N, ClientId, _]}, R) ->
    assert_known_client_id(ClientId, S),
    SS = s_decrement(ClientId, N, s(S)),
    assert_equal(s_value(ClientId, SS), ?M:value(R));
postcondition(S, {call, ?MODULE, merge, [CountersToMerge, [{ClientId, _}=C|_]]}, R) ->
    assert_known_client_id(ClientId, S),
    SS = s_merge(CountersToMerge, [C], s(S)),
    assert_equal(s_value(ClientId, SS), ?M:value(R));
postcondition(_S, _C, _R) ->
    true.

assert_equal(X, X) ->
    true;
assert_equal(L, R) ->
    io:format("~nASSERT: ~p and ~p are not equal.~n", [L, R]),
    false.


assert_known_client_id(ClientId, #state{counters=Counters}) ->
    [error({unknown_client, ClientId, clients(Counters)})
     || not lists:keymember(ClientId, 1, Counters)],
    ok.

is_known_client_id(ClientId, #state{counters=Counters}) ->
    lists:keymember(ClientId, 1, Counters).
    

assert_correct_update(OldS, NewS) ->
    assert_corrent_updated_clients(OldS, NewS),
    NewS.

assert_corrent_updated_clients(#state{counters=OldCounters}, #state{counters=NewCounters}) ->
    DeletedClients = clients(OldCounters) -- clients(NewCounters),
    [error({clients_cannot_be_deleted, DeletedClients})
     || _ <- DeletedClients],
    ok.

assert_subset(SubSet, Set) ->
    Diff = SubSet -- Set,
    case Diff of
        [] -> ok;
        [_|_] ->
            io:format(user, "~nSubSet ~p~nSet ~p~nDiff ~p~n", [SubSet, Set, Diff]),
            error({assert_subset, Diff})
    end.

is_subset(SubSet, Set) ->
    Diff = SubSet -- Set,
    Diff =:= [].


clients(Counters) ->
    [element(1, C) || C <- Counters].

counters(Counters) ->
    [element(2, C) || C <- Counters].

%% ------------------------------------------------------------------
%% Service Code
%% ------------------------------------------------------------------

prop_main() ->
    ?FORALL(Cmds, commands(?MODULE),
       ?TRAPEXIT(
            begin
            {History,State,Result} = run_commands(?MODULE, Cmds),
            ?WHENFAIL(display_error(History, State, Result),
                      aggregate(command_names(Cmds), Result =:= ok))
            end)).


display_error(History, State, Result) ->
%   io:format("History: ~p\nState: ~p\nResult: ~p\n",
%             [History, State, Result]),
    io:format("Simple counter: ~s~nCounters~n~s~n",
              [s_format(s(State)), format_counters(State#state.counters)]),
    io:format("Total: ~w~n",
              [State#state.total_value]),
    MS = s_merge_all(s(State)),
    MC = ?M:merge(counters(State#state.counters)),
    io:format("Merged simple counter value: ~w~n",
              [spbc_value(MS)]),
    io:format("Merged counter value: ~w~n",
              [?M:value(MC)]),
    io:format("Merged simple counter: ~w~n",
              [spbc_format(MS)]),
    io:format("Merged counter: ~s~n",
              [?M:format(MC)]),
    ok.


run_property_testing_test_() ->
    {timeout, 20, fun() ->
        EunitLeader = erlang:group_leader(),
        erlang:group_leader(whereis(user), self()),
        Res = proper:module(?MODULE, [{numtests, 500}]),
        erlang:group_leader(EunitLeader, self()),
        ?assertEqual([], Res)
    end}.


%% ------------------------------------------------------------------
%% Helpers
%% ------------------------------------------------------------------

format_counters(Cs) ->
    [io_lib:format("Client id ~w~n~s~n", [ClientId, ?M:format(C)])
     || {ClientId, C} <- Cs].

-endif.
-endif.
