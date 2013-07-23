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
    counters = [],
    total_value = 0,
    client_ids = [],
    group_names = []}).
-define(M, part_pb_counter).

%% ------------------------------------------------------------------
%% Type Generators
%% ------------------------------------------------------------------

save_counter(Counter, Counters) ->
    Pos = length(Counters),
    ClientId = <<Pos:32>>,
    [{ClientId, Counter}|Counters].

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
%% Callbacks
%% ------------------------------------------------------------------

initial_state() -> 
    #state{}.

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


next_state(S, V, {call, _, new, []}) ->
    S#state{counters = save_counter(V, S#state.counters)};

next_state(S, V, {call, _, F, [GroupName, N, ClientId, _]})
    when F == increment; F == decrement ->
    S#state{counters = update_counter(ClientId, V, S#state.counters),
            total_value = S#state.total_value
                        + case F of increment -> N; decrement -> -N end,
            group_names = ordsets:add_element(GroupName, S#state.group_names)};

next_state(S, V, {call, ?MODULE, merge, [_CountersToMerge, CountersToReplace]}) ->
    S#state{counters = replace_counters(V, CountersToReplace, S#state.counters)};

next_state(S, _V, _C) ->
    S.


precondition(_S, _C) ->
    true.

postcondition(_S, _C, _R) ->
    true.


%% ------------------------------------------------------------------
%% Service Code
%% ------------------------------------------------------------------

prop_main() ->
    ?FORALL(Cmds, commands(?MODULE),
       ?TRAPEXIT(
            begin
            {History,State,Result} = run_commands(?MODULE, Cmds),
            ?WHENFAIL(io:format("History: ~p\nState: ~p\nResult: ~p\n",
                               [History, State, Result]),
                      aggregate(command_names(Cmds), Result =:= ok))
            end)).



run_property_testing_test() ->
    EunitLeader = erlang:group_leader(),
    erlang:group_leader(whereis(user), self()),
    Res = proper:module(?MODULE, [{constraint_tries, 5000}]),
    erlang:group_leader(EunitLeader, self()),
    ?assertEqual([], Res). 

-endif.
-endif.


%% ------------------------------------------------------------------
%% Helpers
%% ------------------------------------------------------------------
