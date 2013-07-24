-module(part_pb_counter).
-export([new/0,
         value/1,
         merge/1,
         increment/4,
         decrement/4,
         format/1]).

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

merge([X]) ->
    X;
merge([_|_] = Cs) ->
    C = merge_groups([complete_decode_counter(C) || C <- Cs]),
    complete_encode_counter(C).

value(C) ->
    join_precalculated_values(C).

format(C) ->
    X = [{K, IncCID2Val, DecCID2Val}
         || {K, IncCID2Val, DecCID2Val} <- complete_decode_counter(C)],
    io_lib:format("~w", [X]).

join_precalculated_values(C) ->
    {_Clients, T1} = decode_var_sized_binaries(C),
    {Groups, <<>>} = decode_var_sized_binaries(T1),
    GroupDict = decode_groups(Groups),
    join_precalculated_group_values(GroupDict).

%% @doc Summerize total values.
%% Each group in the dict contains a precalculated total value and
%% PB-counter. This function does not decode the counter.
join_precalculated_group_values(GroupDict) ->
    join_precalculated_group_values(GroupDict, 0).

join_precalculated_group_values([{_Key, Data}|T], Sum) ->
    {_, TotalValue, _} = var_signed_integer:decode(Data),
    join_precalculated_group_values(T, Sum+TotalValue);
join_precalculated_group_values([], Sum) ->
    Sum.


merge_value(V1, V2) when V1 > 0, V2 > 0 ->
    max(V1, V2).

merge_groups([G1, G2|T]) ->
    merge_groups([merge_groups(G1, G2)|T]);
merge_groups([G]) ->
    G.

merge_groups([{K, IncCID2Val1, DecCID2Val1}|T1],
             [{K, IncCID2Val2, DecCID2Val2}|T2]) ->
    IncCID2Val = merge_values(IncCID2Val1, IncCID2Val2),
    DecCID2Val = merge_values(DecCID2Val1, DecCID2Val2),
    [{K, IncCID2Val, DecCID2Val}|merge_groups(T1, T2)];
merge_groups([{K1, _, _}=H1|T1],
             [{K2, _, _}=H2|T2]) when K1 < K2 ->
    [H1|merge_groups(T1, [H2|T2])];
merge_groups([H1|T1], [H2|T2]) ->
    [H2|merge_groups([H1|T1], T2)];
merge_groups([], L2) ->
    L2;
merge_groups(L1, []) ->
    L1.

merge_values([{CID, V1}|T1], [{CID, V2}|T2]) ->
    [{CID, merge_value(V1, V2)}|merge_values(T1, T2)];
merge_values([{CID1, _}=H1|T1], [{CID2, _}=H2|T2]) when CID1 < CID2 ->
    [H1|merge_values(T1, [H2|T2])];
merge_values([H1|T1], [H2|T2]) ->
    [H2|merge_values([H1|T1], T2)];
merge_values([], L2) ->
    L2;
merge_values(L1, []) ->
    L1.


complete_decode_counter(C) ->
    {Clients, T1} = decode_var_sized_binaries(C),
    {Groups, <<>>} = decode_var_sized_binaries(T1),
    complete_decode_groups(Groups, Clients).

complete_decode_groups(Groups, Clients) ->
    lists:usort([complete_decode_group(G, Clients) || G <- Groups]).

complete_decode_group(B, Clients) ->
    %% Key and encoded single pb-counter.
    {K, C} = decode_var_sized_binary(B),
    {_, _TotalValue, T} = var_signed_integer:decode(C),
    {IncShortCID2Val, DecShortCID2Val} =
    decode_incremented_decremented(T),
    {K,
     lists:usort(expand_short_client_ids(IncShortCID2Val, Clients)),
     lists:usort(expand_short_client_ids(DecShortCID2Val, Clients))}.


complete_encode_counter(Groups) ->
    {GroupDict2, Clients2} = complete_encode_groups(Groups, [], []),
    iolist_to_binary(
        [encode_var_sized_binaries(Clients2),
         encode_var_sized_binaries(encode_groups(GroupDict2))]).

complete_encode_groups([G|Groups], Clients, Acc) ->
    {GG, Clients2} = complete_encode_group(G, Clients),
    complete_encode_groups(Groups, Clients2, [GG|Acc]);
complete_encode_groups([], Clients, Acc) ->
    {lists:reverse(Acc), Clients}. %% Groups and Clients

complete_encode_group({K, IncCID2Val, DecCID2Val}, Clients) ->
    {IncShortCID2Val, Clients2} = apply_short_client_ids(IncCID2Val, Clients),
    {DecShortCID2Val, Clients3} = apply_short_client_ids(DecCID2Val, Clients2),
    Total = calc_total(IncShortCID2Val, DecShortCID2Val),
    Values = encode_incremented_decremented(IncShortCID2Val, DecShortCID2Val),
    EncodedPBC = [var_signed_integer:encode(Total), Values],
    {{K, EncodedPBC}, Clients3}.

apply_short_client_ids(CID2Val, Clients) ->
    apply_short_client_ids(CID2Val, Clients, []).

apply_short_client_ids([{CID, Val}|CID2Val], Clients, Acc) ->
    {ShortCID, Clients2} = get_short_client_id(Clients, CID),
    apply_short_client_ids(CID2Val, Clients2, [{ShortCID, Val}|Acc]);
apply_short_client_ids([], Clients, Acc) ->
    {lists:reverse(Acc), Clients}.


expand_short_client_ids(ShortCID2Val, Clients) ->
    [{expand_short_client_id(ShortCID, Clients), Val}
     || {ShortCID, Val} <- ShortCID2Val].

expand_short_client_id(ShortCID, Clients) ->
    lists:nth(ShortCID+1, Clients).


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

%% Clients are NOT sorted.
get_short_client_id([CID|Clients], CID, N, Acc) ->
    {N, lists:reverse(Acc, [CID|Clients])};
get_short_client_id([C|Clients], CID, N, Acc) ->
    get_short_client_id(Clients, CID, N+1, [C|Acc]);
get_short_client_id([], CID, N, Acc) ->
    %% Create a new entry.
    {N, lists:reverse(Acc, [CID])}.

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
    iolist_to_binary([var_signed_integer:encode(TotalValue+N),
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
    iolist_to_binary([var_signed_integer:encode(TotalValue-N),
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
skip_incremented_cycle(<<?VAR_ZERO, T/binary>>, Acc) ->
    {lists:reverse(Acc), T};
skip_incremented_cycle(B, Acc) ->
    {_, V, T1} = var_integer:decode(B),
    {_, CurShortCID, T2} = var_integer:decode(T1),
    Acc2 = [[var_integer:encode(V), var_integer:encode(CurShortCID)]|Acc],
    %% tail recursion.
    skip_incremented_cycle(T2, Acc2).


encode_incremented_decremented(IncShortCID2Val, DecShortCID2Val) ->
    case DecShortCID2Val of
        [] -> encode_values(IncShortCID2Val);
        [_|_] ->
            [encode_values(IncShortCID2Val), 0, encode_values(DecShortCID2Val)]
    end.

encode_values(ShortCID2Val) ->
    [[var_integer:encode(Val), var_integer:encode(ShortCID)]
     || {ShortCID, Val} <- ShortCID2Val].


decode_incremented_decremented(B) ->
    {IncShortCID2Val, T}    = decode_incremented(B),
    {DecShortCID2Val, <<>>} = decode_decremented(T),
    {IncShortCID2Val, DecShortCID2Val}.

decode_incremented(B) ->
    decode_incremented_cycle(B, []).

decode_incremented_cycle(<<>>, Acc) ->
    {lists:reverse(Acc), <<>>};
decode_incremented_cycle(<<?VAR_ZERO, T/binary>>, Acc) ->
    {lists:reverse(Acc), T};
decode_incremented_cycle(B, Acc) ->
    {_, V, T1} = var_integer:decode(B),
    {_, CurShortCID, T2} = var_integer:decode(T1),
    Acc2 = [{CurShortCID, V}|Acc],
    %% tail recursion.
    decode_incremented_cycle(T2, Acc2).

decode_decremented(B) ->
    decode_decremented_cycle(B, []).

decode_decremented_cycle(<<>>, Acc) ->
    {lists:reverse(Acc), <<>>};
decode_decremented_cycle(B, Acc) ->
    {_, V, T1} = var_integer:decode(B),
    {_, CurShortCID, T2} = var_integer:decode(T1),
    Acc2 = [{CurShortCID, V}|Acc],
    %% tail recursion.
    decode_decremented_cycle(T2, Acc2).


calc_total(IncShortCID2Val, DecShortCID2Val) ->
    calc_total(IncShortCID2Val) - calc_total(DecShortCID2Val).

calc_total(ShortCID2Val) ->
    calc_total_cycle(ShortCID2Val, 0).

calc_total_cycle([{_, V}|T], Sum) ->
    calc_total_cycle(T, V+Sum);
calc_total_cycle([], Sum) ->
    Sum.

    
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

counter() ->
    ?LET(GroupDict, list(group()), lists:ukeysort(1, GroupDict)).

group() ->
    ?SUCHTHAT({Key, IncCID2Val, DecCID2Val},
              {key(), pairs(), pairs()},
              IncCID2Val =/= [] orelse DecCID2Val =/= []).

pairs() ->
    ?LET(Pairs, list(pair()), lists:ukeysort(1, Pairs)).

pair() ->
    {client_id(), value()}.

client_id() ->
    binary().

value() ->
    pos_integer().

key() ->
     binary().

prop_inverse() ->
    ?FORALL(X, counter(),
            equals(X, complete_decode_counter(complete_encode_counter(X)))).
                      

-ifdef(PROPER).

run_property_testing_test_() ->
    {timeout, 20, fun() ->
        EunitLeader = erlang:group_leader(),
        erlang:group_leader(whereis(user), self()),
        Res = proper:module(?MODULE, [{constraint_tries, 500}]),
        erlang:group_leader(EunitLeader, self()),
        ?assertEqual([], Res)
    end}.

-endif.
-endif.

