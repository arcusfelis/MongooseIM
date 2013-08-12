-module(mod_mam_statem_tests).
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
    microseconds_to_datetime/1
]).

-record(state, {
    next_mess_id,
    next_now, % microseconds
    messages,
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

hatter() ->
    #jid{luser = <<"hatter">>, lserver = <<"wonderland">>, lresource = <<>>,
          user = <<"hatter">>,  server = <<"wonderland">>,  resource = <<>>}.

duchess() ->
    #jid{luser = <<"duchess">>, lserver = <<"wonderland">>, lresource = <<>>,
          user = <<"duchess">>,  server = <<"wonderland">>,  resource = <<>>}.

mouse() ->
    #jid{luser = <<"dormouse">>, lserver = <<"wonderland">>, lresource = <<>>,
          user = <<"dormouse">>,  server = <<"wonderland">>,  resource = <<>>}.

user_jid() ->
%   oneof([alice(), cat(), hatter(), duchess(), mouse()]).
    oneof([alice(), cat()]).

maybe_user_jid() ->
    oneof([undefined, user_jid()]).

jid_pair() ->
    ?SUCHTHAT({JID1, JID2}, {user_jid(), user_jid()}, JID1 =/= JID2).


init_now() ->
%   mod_mam_utils:datetime_to_microseconds({{2000,1,1}, {0,0,0}}).
    946684800000000.

microseconds_to_mess_id(Microseconds) when is_integer(Microseconds) ->
    Microseconds * 256.

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

page_size() -> integer(0, 10).

offset() -> integer(0, 30).

mess_id(#state{next_mess_id=MessID, mess_ids=MessIDs}) ->
    oneof([MessID|MessIDs]).

rsm(MessID) ->
    oneof([
        none,
        #rsm_in{index=offset()},
        #rsm_in{direction = before},
        #rsm_in{
            direction = oneof([before, aft]),
            id = {call, mod_mam_utils, mess_id_to_external_binary, [MessID]}}
    ]).

%% ------------------------------------------------------------------
%% Callbacks
%% ------------------------------------------------------------------

initial_state() -> 
    set_next_now(init_now(), #state{
        mess_ids=[],
        messages=dict:new()}).

command(S) ->
    oneof([
     ?LET({LocJID, RemJID}, jid_pair(),
          {call, ?M, archive_message,
           [S#state.next_mess_id, incoming, LocJID, RemJID, RemJID, packet()]}),
     ?LET({LocJID, RemJID}, jid_pair(),
          {call, ?M, archive_message,
           [S#state.next_mess_id, outgoing, LocJID, RemJID, LocJID, packet()]}),
    {call, ?M, lookup_messages,
     [user_jid(), rsm(mess_id(S)), undefined, undefined,
      S#state.next_now, maybe_user_jid(), page_size(), true, 256]}
    ]).

next_state(S, _V, {call, ?M, archive_message,
                   [MessID, _, LocJID, RemJID, _, _]}) ->
    next_now(S#state{
        messages=save_message(MessID, LocJID, RemJID, S#state.messages),
        mess_ids=[MessID|S#state.mess_ids]});
next_state(S, _V, {call, ?M, lookup_messages, _}) ->
    next_now(S#state{});
next_state(S, _V, _) ->
    S.

precondition(_S, _C) ->
    true.

postcondition(S,
    {call, ?M, lookup_messages,
     [LocJID, RSM, undefined, undefined, _, RemJID, PageSize, _, _]},
    {ok, {ResultTotalCount, _, Rows}})
    when is_list(Rows), is_integer(ResultTotalCount), ResultTotalCount >= 0 ->
    ML = find_messages(LocJID, RemJID, S#state.messages),
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

save_message(MessID, LocJID, RemJID, MD) ->
    dict:append(LocJID, MessID,
        dict:append({LocJID, RemJID}, MessID, MD)).

find_messages(LocJID, undefined, MD) ->
    case dict:find(LocJID, MD) of
        {ok, ML} -> ML;
        error -> []
    end;
find_messages(LocJID, RemJID, MD) ->
    case dict:find({LocJID, RemJID}, MD) of
        {ok, ML} -> ML;
        error -> []
    end.

paginate(none, ML) ->
    ML;
paginate(#rsm_in{index=Offset}, ML) when is_integer(Offset) ->
    save_nthtail(Offset, ML);
paginate(#rsm_in{direction = before, id = undefined}, ML) ->
    ML;
paginate(#rsm_in{direction = before, id = BExtMessID}, ML) ->
    BeforeMessID = mod_mam_utils:external_binary_to_mess_id(BExtMessID),
    [MessID || MessID <- ML, MessID < BeforeMessID];
paginate(#rsm_in{direction = aft, id = BExtMessID}, ML) ->
    AfterMessID = mod_mam_utils:external_binary_to_mess_id(BExtMessID),
    [MessID || MessID <- ML, MessID > AfterMessID].


%% @doc This is variant of `lists:nthtail/2', that returns `[]',
%% when `N' is greater then length of the list.
save_nthtail(N, [_|T]) when N > 0 ->
    save_nthtail(N-1, T);
save_nthtail(_, L) when is_list(L) ->
    L.

%% @doc Returns first `N' elements.
trim(N, [H|T]) when N > 0 ->
    [H|trim(N-1, T)];
trim(_, _) ->
    [].

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

analyse_result([{{?MODULE,prop_main,0}, [Shrank|_]}|T]) ->
    io:format(user, "~n~p~n", [Shrank]),
    io:format(user, "~n~sok.~2n", [pretty_print_result(Shrank)]),
    analyse_result(T);
analyse_result([_|T]) ->
    analyse_result(T);
analyse_result([]) ->
    [].

pretty_print_result([{set, _,
    {call, _, archive_message,
     [MessID, Dir, LocJID, RemJID, SrcJID, _]}}|T]) ->
    [pretty_print_archive_message(MessID, Dir, LocJID, RemJID, SrcJID)
    |pretty_print_result(T)];
pretty_print_result([{set, _,
    {call, _, lookup_messages,
     [LocJID, RSM, _, _, Now, _, PageSize, _, _]}}|T]) ->
    [pretty_print_lookup_messages(LocJID, RSM, Now, PageSize)
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

pretty_print_lookup_messages(LocJID, RSM, Now, PageSize) ->
    DateTime = microseconds_to_datetime(Now),
    io_lib:format(
        "set_now(datetime_to_microseconds(~p)),~n"
        "lookup_messages(~s, ~s, undefined, undefined, "
        "get_now(), undefined, ~p, true, 256),~n",
        [DateTime,
         pretty_print_jid(LocJID),
         pretty_print_rsm(RSM),
         PageSize]).

pretty_print_jid(#jid{luser = <<"alice">>}) -> "alice()";
pretty_print_jid(#jid{luser = <<"cat">>})   -> "cat()".

pretty_print_rsm(#rsm_in{index=Offset}) when is_integer(Offset) ->
    io_lib:format("#rsm_in{index=~p}", [Offset]);
pretty_print_rsm(_) -> "none".

pretty_print_microseconds(Microseconds) ->
    io_lib:format("~p", [Microseconds]).

-endif.
-endif.


%% ------------------------------------------------------------------
%% Helpers
%% ------------------------------------------------------------------
