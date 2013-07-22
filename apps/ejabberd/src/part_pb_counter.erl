-module(part_pb_counter).
-export([new/0,
         increment/4,
         decrement/4]).

-ifdef(TEST).
-ifdef(PROPER).
-include_lib("proper/include/proper.hrl").
-endif.

-include_lib("eunit/include/eunit.hrl").
-endif.


%% It is a part of var_integer.
-define(VAR_ZERO, 0).
%% It is a part of var_signed_integer.
-define(VAR_SIGNED_ZERO, 128).

new() ->
    <<?VAR_ZERO, ?VAR_ZERO>>. %% clients = [], groups = []

%% Increase a counter of the group `K' on `N'.
%%
%% `K' - group name (key, binary);
%% `N' - a difference (a positive integer);
%% `CID' - a client ID (a binary);
%% `C' - a counter.
increment(K, N, CID, C) ->
    update_value(K, N, CID, C).

decrement(K, N, CID, C) ->
    update_value(K, -N, CID, C).

update_value(K, N, CID, C) ->
    {Clients, T1} = decode_var_sized_binaries(C),
    {Groups, <<>>} = decode_var_sized_binaries(T1),
    GroupDict = decode_groups(Groups),
    {ShortCID, Clients2} = get_short_client_id(Clients, CID),
    GroupDict2 = with_group(update_group(ShortCID, N), K, GroupDict),
    iolist_to_binary(
        [encode_var_sized_binaries(Clients2),
         encode_var_sized_binaries(encode_groups(GroupDict2))]).

get_short_client_id(Clients, CID) ->
    get_short_client_id(Clients, CID, 0, []).

%% Clients are sorted.
get_short_client_id([CID|Clients], CID, N, Acc) ->
    {N, lists:reverse(Acc, [CID|Clients])};
get_short_client_id([C|Clients], CID, N, Acc) when C < CID ->
    get_short_client_id(Clients, CID, N+1, [C|Acc]);
get_short_client_id(Clients, CID, N, Acc) ->
    %% Create a new entry.
    {N, lists:reverse(Acc, [CID|Clients])}.

with_group(F, K, [{K, Group}|GroupDict]) ->
    [{K, F(Group)}|GroupDict];
with_group(F, K, [{GK, Group}|GroupDict]) when GK < K ->
    [{GK, Group}|with_group(F, K, GroupDict)];
with_group(F, K, GroupDict) ->
    [{K, F(new_group())}|GroupDict].
    
new_group() ->
    <<?VAR_SIGNED_ZERO>>. %% value = 0

update_group(ShortCID, N) when N > 0 ->
    fun(PBC) -> pb_increment(ShortCID, N, PBC) end;
update_group(ShortCID, N) when N < 0 ->
    fun(PBC) -> pb_decrement(ShortCID, -N, PBC) end.

pb_increment(ShortCID, N, PBC) when N > 0 ->
    %% Decode the current precalculated value of the counter.
    {_, TotalValue, T} = var_signed_integer:decode(PBC),
    iolist_to_binary([var_integer:encode(TotalValue+N),
                      pb_increment_cycle(ShortCID, N, T)]).

%% B is: `[list([IncValue, ShortCID]), optional([0, list([DecValue, ShortCID])])]'.
pb_increment_cycle(ShortCID, N, <<>>) ->
    %% Add a new entry at the end.
    [var_integer:encode(N), var_integer:encode(ShortCID)];
pb_increment_cycle(ShortCID, N, <<?VAR_ZERO, _/binary>> = B) ->
    %% Add a new entry at the end.
    [var_integer:encode(N), var_integer:encode(ShortCID), B];
pb_increment_cycle(ShortCID, N, B) ->
    {_, V, T1} = var_integer:decode(B),
    {_, CurShortCID, T2} = var_integer:decode(T1),
    case CurShortCID of
        ShortCID ->
            %% Replace with a new value.
            [var_integer:encode(V+N), var_integer:encode(ShortCID), T2];
        _ ->
            %% Go deeper.
            [var_integer:encode(V), var_integer:encode(CurShortCID),
             %% non-tail recursion.
             pb_increment_cycle(ShortCID, N, T2)]
    end.

pb_decrement(ShortCID, N, PBC) when N > 0 ->
    %% Decode the current precalculated value of the counter.
    {_, TotalValue, T} = var_signed_integer:decode(PBC),
    {IcrIOL, T1} = skip_incremented(T),
    iolist_to_binary([var_integer:encode(TotalValue-N),
                      IcrIOL, 0, pb_decrement_cycle(ShortCID, N, T1)]).

%% B is: `[list([IncValue, ShortCID]), optional([0, list([DecValue, ShortCID])])]'.
pb_decrement_cycle(ShortCID, N, <<>>) ->
    %% Add a new entry at the end.
    [var_integer:encode(N), var_integer:encode(ShortCID)];
pb_decrement_cycle(ShortCID, N, B) ->
    {_, V, T1} = var_integer:decode(B),
    {_, CurShortCID, T2} = var_integer:decode(T1),
    case CurShortCID of
        ShortCID ->
            %% Replace with a new value.
            [var_integer:encode(V+N), var_integer:encode(ShortCID), T2];
        _ ->
            %% Go deeper.
            [var_integer:encode(V), var_integer:encode(CurShortCID),
             %% non-tail recursion.
             pb_decrement_cycle(ShortCID, N, T2)]
    end.


skip_incremented(B) ->
    skip_incremented_cycle(B, []).

skip_incremented_cycle(<<>>, Acc) ->
    {lists:reverse(Acc), <<>>};
skip_incremented_cycle(<<?VAR_ZERO>>, Acc) ->
    {lists:reverse(Acc), <<>>};
skip_incremented_cycle(B, Acc) ->
    {_, V, T1} = var_integer:decode(B),
    {_, CurShortCID, T2} = var_integer:decode(T1),
    Acc2 = [[var_integer:encode(V), var_integer:encode(CurShortCID)]|Acc],
    %% tail recursion.
    skip_incremented_cycle(T2, Acc2).

    
decode_var_sized_binary(B) ->
    {_, N, T} = var_integer:decode(B),
    <<X:N/binary, T1/binary>> = T,
    {X, T1}.

decode_var_sized_binaries(B) ->
    {_, N, T} = var_integer:decode(B),
    decode_var_sized_binaries(T, N, []).

decode_var_sized_binaries(T, 0, Acc) ->
    {lists:reverse(Acc), T};
decode_var_sized_binaries(B, N, Acc) when N > 0 ->
    {X, T} = decode_var_sized_binary(B),
    decode_var_sized_binaries(T, N-1, [X|Acc]).

decode_groups(Groups) ->
    [decode_group(G) || G <- Groups].

decode_group(B) ->
    {K, T} = decode_var_sized_binary(B),
    {K, T}. %% Key and encoded single pb-counter.

encode_groups(GroupDict) ->
    [encode_group(K, V) || {K, V} <- GroupDict].

encode_group(K, V) ->
    iolist_to_binary([encode_var_sized_binary(K), V]).

encode_var_sized_binaries(Bs) ->
    [var_integer:encode(length(Bs)),
     [encode_var_sized_binary(B) || B <- Bs]].

encode_var_sized_binary(B) ->
    [var_integer:encode(byte_size(B)), B].



-ifdef(TEST).

encode_test_() ->
    [?_assertEqual(<<?VAR_ZERO>>, var_integer:encode(0))
    ,?_assertEqual(<<?VAR_SIGNED_ZERO>>, var_signed_integer:encode(0))
    ].


-ifdef(PROPER).

run_property_testing_test() ->
    EunitLeader = erlang:group_leader(),
    erlang:group_leader(whereis(user), self()),
    Res = proper:module(?MODULE, [{constraint_tries, 500}]),
    erlang:group_leader(EunitLeader, self()),
    ?assertEqual([], Res). 

-endif.
-endif.

