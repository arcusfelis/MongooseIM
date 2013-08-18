-module(mod_mam_statem4_tests).
-include_lib("ejabberd/include/jlib.hrl").

-ifdef(TEST).
-ifdef(PROPER).
-include_lib("proper/include/proper.hrl").
-include_lib("eunit/include/eunit.hrl").

-behaviour(proper_statem).

%% Behaviour callbacks
-export([initial_state/0, command/1,
         precondition/2, postcondition/3, next_state/3]).

-import(mod_mam_utils, [
    microseconds_to_datetime/1,
    encode_compact_uuid/2
]).

-record(state, {
    next_mess_id,
    next_now, % microseconds
    mess_ids
}).
-define(M, mod_mam_riak_arch).

%% ------------------------------------------------------------------
%% Type Generators
%% ------------------------------------------------------------------

packet() ->
    <<"hi">>.

alice() ->
    #jid{luser = <<"alice">>, lserver = <<"wonderland">>, lresource = <<>>,
          user = <<"alice">>,  server = <<"wonderland">>,  resource = <<>>}.

cat() ->
    #jid{luser = <<"cat">>, lserver = <<"wonderland">>, lresource = <<>>,
          user = <<"cat">>,  server = <<"wonderland">>,  resource = <<>>}.

maybe_cat() ->
    oneof([undefined, cat()]).

random_microseconds(S=#state{mess_ids=[]}) ->
    random_microseconds_v1(S);
random_microseconds(S=#state{mess_ids=[_|_]}) ->
    oneof([random_microseconds_v1(S), random_microseconds_v2(S)]).

%% @doc Random timestamp.
random_microseconds_v1(#state{next_now=Now}) ->
    integer(init_now(), Now).

%% @doc Timestamp of an existing message.
random_microseconds_v2(#state{mess_ids=MessIDs}) ->
    ?LET(MessID, oneof(MessIDs), mess_id_to_microseconds(MessID)).

maybe_start_and_end(S) ->
    oneof([
        {undefined, random_microseconds(S)},
        {random_microseconds(S), undefined},
        ?LET({X, Y}, {random_microseconds(S), random_microseconds(S)},
             {min(X, Y), max(X, Y)})
    ]).

init_now() ->
%   mod_mam_utils:datetime_to_microseconds({{2000,1,1}, {0,0,0}}).
    946684800000000.

microseconds_to_mess_id(Microseconds) when is_integer(Microseconds) ->
    Microseconds * 256.

mess_id_to_microseconds(MessID) when is_integer(MessID) ->
    MessID div 256.

next_now(S=#state{next_now=PrevNow}) ->
    set_next_now(PrevNow + random_microsecond_delay(), S).

%% @doc This function is called each time, when new time is needed.
set_next_now(NextNow, S=#state{}) when is_integer(NextNow) ->
    ?M:set_now(NextNow),
    S#state{
        next_now=NextNow,
        next_mess_id=microseconds_to_mess_id(NextNow)}.

random_microsecond_delay() ->
    %% One hour is the maximim delay.
    random:uniform(3600000000).

page_size() -> integer(0, 5).

offset() -> integer(0, 10).

mess_id(#state{next_mess_id=MessID, mess_ids=MessIDs}) ->
    oneof([MessID|MessIDs]).

rsm(MessID) ->
    oneof([
        undefined,
        #rsm_in{index=offset()},
        #rsm_in{direction = before},
        #rsm_in{direction = oneof([before, aft]), id = MessID}
    ]).

%% ------------------------------------------------------------------
%% Callbacks
%% ------------------------------------------------------------------

initial_state() -> 
    set_next_now(init_now(), #state{mess_ids=[]}).

command(S) ->
    oneof([
    {call, ?M, archive_message,
     [S#state.next_mess_id, incoming, alice(), cat(), cat(), packet()]},
    {call, ?M, archive_message,
     [S#state.next_mess_id, outgoing, alice(), cat(), alice(), packet()]},
    ?LET({Start, End}, maybe_start_and_end(S),
        {call, ?M, lookup_messages,
         [alice(), rsm(mess_id(S)), Start, End,
          S#state.next_now, maybe_cat(), page_size(), true, 256]}),
    ?LET({Start, End}, maybe_start_and_end(S),
        {call, ?M, purge_multiple_messages,
         [alice(), Start, End, S#state.next_now, maybe_cat()]})
    ]).

next_state(S, _V, {call, ?M, archive_message, [MessID, _, _, _, _, _]}) ->
    next_now(S#state{mess_ids=[MessID|S#state.mess_ids]});
next_state(S, _V, {call, ?M, lookup_messages, _}) ->
    next_now(S);
next_state(S, _V, {call, ?M, purge_multiple_messages, [_, Start, End, _, _]}) ->
    next_now(delete_messages(Start, End, S)).

precondition(_S, _C) ->
    true.

postcondition(S,
    {call, ?M, lookup_messages,
     [_, RSM, undefined, undefined, Start, End, PageSize, _, _]},
    {ok, {ResultTotalCount, _, Rows}})
    when is_list(Rows), is_integer(ResultTotalCount), ResultTotalCount >= 0 ->
    ML = cut(Start, End, S#state.mess_ids),
    PL = trim(PageSize, paginate(RSM, ML)),
    ExpectedPageSize = length(PL),
    ExpectedTotalCount = length(ML),
    ResultPageSize = length(Rows),
    case ResultTotalCount =:= ExpectedTotalCount andalso
         ResultPageSize   =:= ExpectedPageSize of
        true -> true;
        false ->
        ?debugVal(ExpectedTotalCount),
        ?debugVal(ResultTotalCount),
        ?debugVal(ExpectedPageSize),
        ?debugVal(ResultPageSize),
        ?debugVal(RSM),
        false
    end;
postcondition(_S, _C, _R) ->
    true.

%% ------------------------------------------------------------------
%% Model helpers
%% ------------------------------------------------------------------

delete_messages(Start, End, S) ->
    ML = cut(Start, End, S#state.mess_ids),
    S#state{mess_ids=ML}.

paginate(undefined, ML) ->
    ML;
paginate(#rsm_in{index=Offset}, ML) when is_integer(Offset) ->
    safe_nthtail(Offset, ML);
paginate(#rsm_in{direction = before, id = undefined}, ML) ->
    ML;
paginate(#rsm_in{direction = before, id = BeforeMessID}, ML) ->
    [MessID || MessID <- ML, MessID < BeforeMessID];
paginate(#rsm_in{direction = aft, id = AfterMessID}, ML) ->
    [MessID || MessID <- ML, MessID > AfterMessID].


%% @doc This is variant of `lists:nthtail/2', that returns `[]',
%% when `N' is greater then length of the list.
safe_nthtail(N, [_|T]) when N > 0 ->
    safe_nthtail(N-1, T);
safe_nthtail(_, L) when is_list(L) ->
    L.

%% @doc Returns first `N' elements.
trim(N, [H|T]) when N > 0 ->
    [H|trim(N-1, T)];
trim(_, _) ->
    [].

%% @doc Get messages between `Start' and `End'.
cut(Start, End, MessIDs) ->
    cut_top(End, cut_bottom(Start, MessIDs)).

cut_top(undefined, MessIDs) ->
    MessIDs;
cut_top(End, MessIDs) ->
    MaxMessID = microseconds_to_max_mess_id(End),
    [MessID || MessID <- MessIDs, MessID =< MessIDs].

cut_bottom(undefined, MessIDs) ->
    MessIDs;
cut_bottom(Start, MessIDs) ->
    MinMessID = microseconds_to_max_mess_id(Start),
    [MessID || MessID <- MessIDs, MessID >= MinMessID].

microseconds_to_min_mess_id(Microseconds) ->
    encode_compact_uuid(Microseconds, 0).

microseconds_to_max_mess_id(Microseconds) ->
    encode_compact_uuid(Microseconds, 255).

%% ------------------------------------------------------------------
%% Service Code
%% ------------------------------------------------------------------

prop_main() ->
    ?FORALL(Cmds, commands(?MODULE),
       ?TRAPEXIT(
            begin
            ?M:reset_mock(),
            {History,State,Result} = run_commands(?MODULE, Cmds),
            ?WHENFAIL(begin
                io:format("History: ~p\nState: ~p\nResult: ~p\n",
                          [History, State, Result])
                end,
              aggregate(command_names(Cmds), Result =:= ok))
            end)).
       

run_property_testing_test_() ->
    {setup,
     fun() -> ?M:load_mock(0) end,
     fun(_) -> ?M:unload_mock() end,
     {timeout, 60,
         fun() ->
            EunitLeader = erlang:group_leader(),
            erlang:group_leader(whereis(user), self()),
            Res = proper:module(?MODULE,
                [{numtests, 300}, {max_size, 50}, long_result]),
            erlang:group_leader(EunitLeader, self()),
            analyse_result(Res),
            ?assertEqual([], Res)
         end}}.

analyse_result([{{?MODULE,prop_main,0}, [Cmds|_]}|T]) ->
    analyse_bad_commands(Cmds),
    analyse_result(T);
analyse_result([_|T]) ->
    analyse_result(T);
analyse_result([]) ->
    [].

analyse_bad_commands(Cmds) ->
    ?M:reset_mock(),
    {History,State,Result} = run_commands(?MODULE, Cmds),
    io:format(user, "~n~p~n", [Cmds]),
    io:format(user, "~n~sok.~2n", [pretty_print_result(Cmds)]),
    ok.

pretty_print_result([{set, _,
    {call, _, archive_message,
     [MessID, Dir, LocJID, RemJID, SrcJID, _]}}|T]) ->
    [pretty_print_archive_message(MessID, Dir, LocJID, RemJID, SrcJID)
    |pretty_print_result(T)];
pretty_print_result([{set, _,
    {call, _, lookup_messages,
     [LocJID, RSM, Start, End, Now, _, PageSize, _, _]}}|T]) ->
    [pretty_print_lookup_messages(LocJID, RSM, Start, End, Now, PageSize)
    |pretty_print_result(T)];

pretty_print_result([{set, _,
    {call, ?M, purge_multiple_messages,
     [LocJID, Start, End, Now, WithJID]}}|T]) ->
    [pretty_print_purge_messages(LocJID, Start, End, Now, WithJID)
    |pretty_print_result(T)];
pretty_print_result([_|T]) ->
    ["% skipped\n"|pretty_print_result(T)];
pretty_print_result([]) ->
    [].

pretty_print_archive_message(MessID, Dir, LocJID, RemJID, SrcJID) ->
    {Now, _} = mod_mam_utils:decode_compact_uuid(MessID),
    DateTime = microseconds_to_datetime(Now),
    io_lib:format(
        "set_now(datetime_to_microseconds(~p)),~n"
        "archive_message(id(), ~p, ~s, ~s, ~s, packet()),~n",
        [DateTime, Dir,
         pretty_print_jid(LocJID),
         pretty_print_jid(RemJID),
         pretty_print_jid(SrcJID)]).

pretty_print_lookup_messages(LocJID, RSM, Start, End, Now, PageSize) ->
    io_lib:format(
        "set_now(~s),~n"
        "lookup_messages(~s, ~s, ~s, ~s, "
        "get_now(), undefined, ~p, true, 256),~n",
        [pretty_print_microseconds(Now),
         pretty_print_jid(LocJID),
         pretty_print_rsm(RSM),
         pretty_print_maybe_microseconds(Start),
         pretty_print_maybe_microseconds(End),
         PageSize]).

pretty_print_purge_messages(LocJID, Start, End, Now, WithJID) ->
    io_lib:format(
        "set_now(~s),~n"
        "purge_multiple_messages(~s, ~s, ~s, get_now(), ~s),~n",
        [pretty_print_microseconds(Now),
         pretty_print_jid(LocJID),
         pretty_print_maybe_microseconds(Start),
         pretty_print_maybe_microseconds(End),
         maybe_pretty_print_jid(WithJID)]).

maybe_pretty_print_jid(undefined) -> "undefined";
maybe_pretty_print_jid(JID) -> pretty_print_jid(JID).

pretty_print_jid(#jid{luser = <<"alice">>}) -> "alice()";
pretty_print_jid(#jid{luser = <<"cat">>, lresource = <<"1">>})   -> "cat1()";
pretty_print_jid(#jid{luser = <<"cat">>, lresource = <<>>})      -> "cat()".

pretty_print_rsm(#rsm_in{index=Offset}) when is_integer(Offset) ->
    io_lib:format("#rsm_in{index=~p}", [Offset]);
pretty_print_rsm(#rsm_in{direction=Dir, id=undefined}) ->
    io_lib:format("#rsm_in{direction=~p}", [Dir]);
pretty_print_rsm(#rsm_in{direction=Dir, id=MessID}) ->
    {Microseconds, _} = mod_mam_utils:decode_compact_uuid(MessID),
    DateTime = microseconds_to_datetime(Microseconds),
    io_lib:format("#rsm_in{direction=~p, id=datetime_to_mess_id(~p)}", [Dir, DateTime]);
pretty_print_rsm(undefined) -> "undefined";
pretty_print_rsm(RSM) ->
    io_lib:format("~p", [RSM]).

pretty_print_microseconds(Microseconds) ->
    DateTime = microseconds_to_datetime(Microseconds),
    io_lib:format("datetime_to_microseconds(~p)", [DateTime]).

pretty_print_maybe_microseconds(undefined) ->
    "undefined";
pretty_print_maybe_microseconds(Microseconds) ->
    pretty_print_microseconds(Microseconds).

-endif.
-endif.


%% ------------------------------------------------------------------
%% Helpers
%% ------------------------------------------------------------------
