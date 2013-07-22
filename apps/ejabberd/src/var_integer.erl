-module(var_integer).
-export([encode/1, decode/1]).
-compile([export_all]).
	
-ifdef(TEST).
-ifdef(PROPER).
-include_lib("proper/include/proper.hrl").
-endif.

-include_lib("eunit/include/eunit.hrl").
-endif.

%%% Variable size integer
%%%
%%% Like in EBML, but allows to compare encoded numbers.
 
%%% Width  Size  Representation
%%%   1    2^7   0xxx xxxx
%%%   2    2^14  10xx xxxx  xxxx xxxx
%%%   3    2^21  110x xxxx  xxxx xxxx  xxxx xxxx
%%%   4    2^28  1110 xxxx  xxxx xxxx  xxxx xxxx  xxxx xxxx

%% @doc Read integer from binary.
%% Returns {offset::integer(), Value::integer(), Tail}.
-spec decode(binary()) -> {integer(), integer(), binary()}.
%% -2:1 = 0:1, -2:2 = 2#10:2
decode(<<2#0000:1, X:07, T/binary>>) -> {1, X, T};
decode(<<2#0010:2, X:14, T/binary>>) -> {2, X, T};
decode(<<2#0110:3, X:21, T/binary>>) -> {3, X, T};
decode(<<2#1110:4, X:28, T/binary>>) -> {4, X, T};

decode(<<2#1111:4, _/bitstring>> = B) -> decode_long(B, 4, 35).


decode_long(B, L, S) ->
    %% L = W - 1
	case B of
	<<_:L, 0:1, X:S, T/binary>> -> 
		{L+1, X, T};
	<<_:L, 1:1, _/bitstring>> ->
		decode_long(B, L+1, S+7)
	end.



encode(X) when is_integer(X), X >= 0 -> 
	W = width_of_integer(X),
	S = W*7,
	<<-2:W, X:S>>.


width_of_integer(0) ->
	1;
width_of_integer(X) ->
	width_of_integer(X, 0).


width_of_integer(0, W) ->
	W;
width_of_integer(X, W) ->
	width_of_integer(X bsr 7, W+1).
	


-ifdef(TEST).
-define(M, ?MODULE).

encode_test_() ->
	E = fun ?M:encode/1,
	D = fun ?M:decode/1,
    [?_assertEqual(D(E(100)), {1, 100, <<>>})
	,?_assertEqual(D(E(666)), {2, 666, <<>>})
	,?_assertEqual(D(E(123456789)), {4, 123456789, <<>>})
	,?_assertEqual(D(E(123456789123456789)), {9, 123456789123456789, <<>>})
	,?_assertEqual(D(E(0)), {1, 0, <<>>})
	,?_assertEqual(D(E(1)), {1, 1, <<>>})
	].


-ifdef(PROPER).

run_property_testing_test() ->
    EunitLeader = erlang:group_leader(),
    erlang:group_leader(whereis(user), self()),
    Res = proper:module(?MODULE, [{constraint_tries, 500}]),
    erlang:group_leader(EunitLeader, self()),
    ?assertEqual([], Res). 

prop_inverse() ->
    ?FORALL(X, non_neg_integer(), equals(element(2, ?M:decode(?M:encode(X))), X)).

prop_preserve_order() ->
    ?FORALL({X, Y}, {non_neg_integer(), non_neg_integer()},
            equals(X =< Y, ?M:encode(X) =< ?M:encode(Y))).

-endif.
-endif.
