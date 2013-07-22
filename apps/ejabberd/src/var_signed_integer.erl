-module(var_signed_integer).
-export([encode/1, decode/1]).
-compile([export_all]).
    
-ifdef(TEST).
-ifdef(PROPER).
-include_lib("proper/include/proper.hrl").
-endif.

-include_lib("eunit/include/eunit.hrl").
-endif.

%%% Variable size signed integer
%%%
%%% Like in EBML, but allows to compare encoded numbers.
%%% Like var_integer, but allows negative numbers.
 
%%% Width  Size  Representation
%%%   1    2^6   10xx xxxx
%%%   2    2^13  110x xxxx  xxxx xxxx
%%%   3    2^20  1110 xxxx  xxxx xxxx  xxxx xxxx
%%%   4    2^27  1111 0xxx  xxxx xxxx  xxxx xxxx  xxxx xxxx

%%% Number  Representation
%%%    0    1000 0000
%%%    1    1000 0001
%%%    2    1000 0010
%%%   -1    0111 1111
%%%   -2    0111 1110

%% @doc Read integer from binary.
%% Returns {offset::integer(), Value::integer(), Tail}.
-spec decode(binary()) -> {integer(), integer(), binary()}.
decode(<<2#00010:2, X:06, T/binary>>) -> {1, X, T};
decode(<<2#00001:2, X:06, T/binary>>) -> {1, neg(X, 6), T};
decode(<<2#00110:3, X:13, T/binary>>) -> {2, X, T};
decode(<<2#00001:3, X:13, T/binary>>) -> {2, neg(X, 13), T};
decode(<<2#01110:4, X:20, T/binary>>) -> {3, X, T};
decode(<<2#00001:4, X:20, T/binary>>) -> {3, neg(X, 20), T};
decode(<<2#11110:5, X:27, T/binary>>) -> {4, X, T};
decode(<<2#00001:5, X:27, T/binary>>) -> {4, neg(X, 27), T};

decode(<<2#11111:5, _/bitstring>> = B) -> decode_long_pos(B, 5, 34);
decode(<<2#00000:5, _/bitstring>> = B) -> decode_long_neg(B, 5, 34).


decode_long_pos(B, L, S) ->
    %% L = W - 1
    case B of
    <<_:L, 0:1, X:S, T/binary>> -> 
        {L, X, T};
    <<_:L, 1:1, _/bitstring>> ->
        decode_long_pos(B, L+1, S+7)
    end.

decode_long_neg(B, L, S) ->
    %% L = W - 1
    case B of
    <<_:L, 1:1, X:S, T/binary>> -> 
        {L, neg(X, S), T};
    <<_:L, 0:1, _/bitstring>> ->
        decode_long_neg(B, L+1, S+7)
    end.

neg(X, S) ->
    S1 = S + 1,
    <<X1:S1/signed>> = <<1:1, X:S>>,
    X1.

encode(X) when is_integer(X), X >= 0 ->
    W = width_of_integer(X),
    S = W*7 - 1,
    <<1:1, -2:W, X:S>>;
encode(X) when is_integer(X) ->
    W = width_of_integer(-X - 1),
    S = W*7 - 1,
    <<0:1, 1:W, X:S>>.


width_of_integer(0) ->
    1;
width_of_integer(X) when X > 0 ->
    width_of_integer(X bsr 6, 1).


width_of_integer(0, W) ->
    W;
width_of_integer(X, W) ->
    width_of_integer(X bsr 7, W+1).
    


-ifdef(TEST).
-define(M, ?MODULE).

encode_test_() ->
    E = fun ?M:encode/1,
    D = fun ?M:decode/1,
    [?_assertEqual({1,  31, <<>>}, D(E(31)))
    ,?_assertEqual({1, -33, <<>>}, D(E(-33)))
    ,?_assertEqual({2, 100, <<>>}, D(E(100)))
    ,?_assertEqual({2, 666, <<>>}, D(E(666)))
    ,?_assertEqual({4, 123456789, <<>>}, D(E(123456789)))
    ,?_assertEqual({9, 123456789123456789, <<>>}, D(E(123456789123456789)))
    ,?_assertEqual({1, 0, <<>>}, D(E(0)))
    ,?_assertEqual({1, 1, <<>>}, D(E(1)))
    ,?_assert(E(-1) > E(-65)) % 1 byte and 2 bytes length
    ].


-ifdef(PROPER).

run_property_testing_test() ->
    EunitLeader = erlang:group_leader(),
    erlang:group_leader(whereis(user), self()),
    Res = proper:module(?MODULE, [{constraint_tries, 500}]),
    erlang:group_leader(EunitLeader, self()),
    ?assertEqual([], Res). 

prop_inverse() ->
    ?FORALL(X, integer(), equals(element(2, ?M:decode(?M:encode(X))), X)).

prop_preserve_order() ->
    ?FORALL({X, Y}, {integer(), integer()},
            equals(X =< Y, ?M:encode(X) =< ?M:encode(Y))).

-endif.
-endif.

