-module(part_pb_counter_statem_tests).

-ifdef(TEST).
-ifdef(PROPER).
-include_lib("proper/include/proper.hrl").
-include_lib("eunit/include/eunit.hrl").

-behaviour(proper_statem).

%% Behaviour callbacks
-export([initial_state/0, command/1,
         precondition/2, postcondition/3, next_state/3]).


-record(state, {
    counter,
    total_value = 0,
    client_ids = [],
    group_names = []}).
-define(M, part_pb_counter).

%% ------------------------------------------------------------------
%% Type Generators
%% ------------------------------------------------------------------

client_id() ->
    binary(4).

client_id(#state{client_ids = Ids = [_|_]}) ->
    oneof([client_id()|Ids]);
client_id(#state{client_ids = []}) ->
    client_id().

group_name() ->
    binary(4).

group_name(#state{group_names = Names = [_|_]}) ->
    oneof([group_name()|Names]);
group_name(#state{group_names = []}) ->
    group_name().

%% ------------------------------------------------------------------
%% Callbacks
%% ------------------------------------------------------------------

initial_state() -> 
    #state{}.

command(#state{counter = undefined}) ->
    {call, ?M, new, []};

command(#state{counter = C} = S) ->
    Inc = {call, ?M, increment, [group_name(S), pos_integer(), client_id(S), C]},
    Dec = {call, ?M, decrement, [group_name(S), pos_integer(), client_id(S), C]},
    Freqs =
    [ {50, Inc}
    , {50, Dec}
    ],
    frequency(Freqs).


next_state(S, V, {call, _, new, []}) ->
    S#state{counter = V};

next_state(S, V, {call, _, F, [GroupName, N, ClientId, _]})
    when F == increment; F == decrement ->
    S#state{counter = V,
            total_value = S#state.total_value
                        + case F of increment -> N; decrement -> -N end,
            client_ids = ordsets:add_element(ClientId, S#state.client_ids),
            group_names = ordsets:add_element(GroupName, S#state.group_names)};

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
    Res = proper:module(?MODULE, [{constraint_tries, 500}]),
    erlang:group_leader(EunitLeader, self()),
    ?assertEqual([], Res). 

-endif.
-endif.


%% ------------------------------------------------------------------
%% Helpers
%% ------------------------------------------------------------------
