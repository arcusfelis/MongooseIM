-module(mod_mam).
-behavior(gen_mod).
-export([start/2, stop/1]).
%% ejabberd handlers
-export([process_mam_iq/3,
         user_send_packet/3,
         remove_user/2,
         filter_packet/1]).

%% Client API
-export([delete_archive/2,
         archive_size/2]).

-import(mod_mam_utils,
        [maybe_microseconds/1,
         microseconds_to_now/1]).

%% UID
-import(mod_mam_utils,
        [generate_message_id/0,
         decode_compact_uuid/1]).

%% XML
-import(mod_mam_utils,
        [replace_archived_elem/3,
         get_one_of_path/2,
         is_complete_message/1,
         wrap_message/5,
         result_set/4,
         result_query/1,
         result_prefs/3,
         parse_prefs/1]).

%% Other
-import(mod_mam_utils,
        [maybe_integer/2,
         is_function_exist/3,
         mess_id_to_external_binary/1]).

%% Ejabberd
-import(mod_mam_utils,
        [send_message/3,
         is_jid_in_user_roster/2]).

-include_lib("ejabberd/include/ejabberd.hrl").
-include_lib("ejabberd/include/jlib.hrl").
-include_lib("exml/include/exml.hrl").

%% ----------------------------------------------------------------------
%% Datetime types
%% Microseconds from 01.01.1970
-type unix_timestamp() :: non_neg_integer().

%% ----------------------------------------------------------------------
%% XMPP types
-type server_hostname() :: binary().
-type literal_username() :: binary().
-type literal_jid() :: binary().
-type elem() :: #xmlel{}.


%% ----------------------------------------------------------------------
%% Other types
-type archive_behaviour() :: atom(). % roster | always | never.

%% ----------------------------------------------------------------------
%% Constants

mam_ns_string() -> "urn:xmpp:mam:tmp".

mam_ns_binary() -> <<"urn:xmpp:mam:tmp">>.

default_result_limit() -> 50.

max_result_limit() -> 50.

%% ----------------------------------------------------------------------
%% API

delete_archive(Server, User) ->
    LUser = jlib:nodeprep(User),
    LServer = jlib:nameprep(Server),
    ?DEBUG("Remove user ~p from ~p.", [LUser, LServer]),
    remove_user_from_db(LServer, LUser),
    ok.

archive_size(Server, User) ->
    LUser = jlib:nodeprep(User),
    LServer = jlib:nameprep(Server),
    AM = archive_module(LServer),
    AM:archive_size(LServer, LUser).

%% ----------------------------------------------------------------------
%% gen_mod callbacks

start(Host, Opts) ->
    ?DEBUG("mod_mam starting", []),
    start_supervisor(Host),
    %% `parallel' is the only one recommended here.
    IQDisc = gen_mod:get_opt(iqdisc, Opts, parallel), %% Type
    mod_disco:register_feature(Host, mam_ns_binary()),
    gen_iq_handler:add_iq_handler(ejabberd_sm, Host, mam_ns_binary(),
                                  ?MODULE, process_mam_iq, IQDisc),
    ejabberd_hooks:add(user_send_packet, Host, ?MODULE, user_send_packet, 90),
    ejabberd_hooks:add(filter_packet, global, ?MODULE, filter_packet, 90),
    ejabberd_hooks:add(remove_user, Host, ?MODULE, remove_user, 50),
    ejabberd_users:start(Host),
    [start_module(Host, M) || M <- required_modules(Host)],
    ok.

stop(Host) ->
    ?DEBUG("mod_mam stopping", []),
    stop_supervisor(Host),
    ejabberd_hooks:delete(user_send_packet, Host, ?MODULE, user_send_packet, 90),
    ejabberd_hooks:delete(filter_packet, global, ?MODULE, filter_packet, 90),
    ejabberd_hooks:delete(remove_user, Host, ?MODULE, remove_user, 50),
    gen_iq_handler:remove_iq_handler(ejabberd_sm, Host, mam_ns_string()),
    mod_disco:unregister_feature(Host, mam_ns_binary()),
    [stop_module(Host, M) || M <- required_modules(Host)],
    ok.

%% ----------------------------------------------------------------------
%% OTP helpers


start_supervisor(_Host) ->
    CacheChildSpec =
    {mod_mam_cache,
     {mod_mam_cache, start_link, []},
     permanent,
     5000,
     worker,
     [mod_mam_cache]},
    supervisor:start_child(ejabberd_sup, CacheChildSpec).

stop_supervisor(_Host) ->
    ok.


start_module(Host, M) ->
    case is_function_exist(M, start, 1) of
        true  -> M:start(Host);
        false -> ok
    end,
    ok.

stop_module(Host, M) ->
    case is_function_exist(M, stop, 1) of
        true  -> M:stop(Host);
        false -> ok
    end,
    ok.

%% ----------------------------------------------------------------------
%% hooks and handlers

%% `To' is an account or server entity hosting the archive.
%% Servers that archive messages on behalf of local users SHOULD expose archives 
%% to the user on their bare JID (i.e. `From.luser'),
%% while a MUC service might allow MAM queries to be sent to the room's bare JID
%% (i.e `To.luser').
process_mam_iq(From=#jid{luser = LUser, lserver = LServer},
               _To,
               IQ=#iq{type = set,
                      sub_el = PrefsEl = #xmlel{name = <<"prefs">>}}) ->
    ?DEBUG("Handling mam prefs IQ~n    from ~p ~n    packet ~p.",
              [From, IQ]),
    {DefaultMode, AlwaysJIDs, NeverJIDs} = parse_prefs(PrefsEl),
    ?DEBUG("Parsed data~n\tDefaultMode ~p~n\tAlwaysJIDs ~p~n\tNeverJIDS ~p~n",
              [DefaultMode, AlwaysJIDs, NeverJIDs]),
    update_settings(LServer, LUser, DefaultMode, AlwaysJIDs, NeverJIDs),
    ResultPrefsEl = result_prefs(DefaultMode, AlwaysJIDs, NeverJIDs),
    IQ#iq{type = result, sub_el = [ResultPrefsEl]};

process_mam_iq(From=#jid{luser = LUser, lserver = LServer},
               _To,
               IQ=#iq{type = get,
                      sub_el = #xmlel{name = <<"prefs">>}}) ->
    ?DEBUG("Handling mam prefs IQ~n    from ~p ~n    packet ~p.",
              [From, IQ]),
    {DefaultMode, AlwaysJIDs, NeverJIDs} = get_prefs(LServer, LUser, always),
    ?DEBUG("Extracted data~n\tDefaultMode ~p~n\tAlwaysJIDs ~p~n\tNeverJIDS ~p~n",
              [DefaultMode, AlwaysJIDs, NeverJIDs]),
    ResultPrefsEl = result_prefs(DefaultMode, AlwaysJIDs, NeverJIDs),
    IQ#iq{type = result, sub_el = [ResultPrefsEl]};
    
process_mam_iq(From=#jid{luser = LUser, lserver = LServer},
               To,
               IQ=#iq{type = get,
                      sub_el = QueryEl = #xmlel{name = <<"query">>}}) ->
    ?DEBUG("Handling mam IQ~n    from ~p ~n    to ~p~n    packet ~p.",
              [From, To, IQ]),
    QueryID = xml:get_tag_attr_s(<<"queryid">>, QueryEl),

    wait_flushing(LServer),

    %% Filtering by date.
    %% Start :: integer() | undefined
    Start = maybe_microseconds(xml:get_path_s(QueryEl, [{elem, <<"start">>}, cdata])),
    End   = maybe_microseconds(xml:get_path_s(QueryEl, [{elem, <<"end">>}, cdata])),
    RSM   = jlib:rsm_decode(QueryEl),
    %% Filtering by contact.
    With  = xml:get_path_s(QueryEl, [{elem, <<"with">>}, cdata]),
    %% This element's name is "limit".
    %% But it must be "max" according XEP-0313.
    Limit = get_one_of_path(QueryEl, [
                    [{elem, <<"set">>}, {elem, <<"max">>}, cdata],
                    [{elem, <<"set">>}, {elem, <<"limit">>}, cdata]
                   ]),
    PageSize = min(max_result_limit(),
                   maybe_integer(Limit, default_result_limit())),
    LimitPassed = Limit =/= <<>>,
    case lookup_messages(From, RSM, Start, End, With, PageSize,
                         LimitPassed, max_result_limit()) of
    {error, 'policy-violation'} ->
        ?DEBUG("Policy violation by ~p.", [LUser]),
        ErrorEl = ?STANZA_ERRORT(<<"">>, <<"modify">>, <<"policy-violation">>,
                                 <<"en">>, <<"Too many results">>),          
        IQ#iq{type = error, sub_el = [ErrorEl]};
    {ok, {TotalCount, Offset, MessageRows}} ->
        {FirstId, LastId} =
            case MessageRows of
                []    -> {undefined, undefined};
                [_|_] -> {message_row_to_ext_id(hd(MessageRows)),
                          message_row_to_ext_id(lists:last(MessageRows))}
            end,
        [send_message(To, From, message_row_to_xml(M, QueryID))
         || M <- MessageRows],
        ResultSetEl = result_set(FirstId, LastId, Offset, TotalCount),
        ResultQueryEl = result_query(ResultSetEl),
        %% On receiving the query, the server pushes to the client a series of
        %% messages from the archive that match the client's given criteria,
        %% and finally returns the <iq/> result.
        IQ#iq{type = result, sub_el = [ResultQueryEl]}
    end.


%% @doc Handle an outgoing message.
%%
%% Note: for outgoing messages, the server MUST use the value of the 'to' 
%%       attribute as the target JID. 
user_send_packet(From, To, Packet) ->
    ?DEBUG("Send packet~n    from ~p ~n    to ~p~n    packet ~p.",
              [From, To, Packet]),
    handle_package(outgoing, false, From, To, From, Packet),
    ok.

%% @doc Handle an incoming message.
%%
%% Note: For incoming messages, the server MUST use the value of the
%%       'from' attribute as the target JID. 
%%
%% Return drop to drop the packet, or the original input to let it through.
%% From and To are jid records.
-spec filter_packet(Value) -> Value when
    Value :: {From, To, Packet} | drop,
    From :: jid(),
    To :: jid(),
    Packet :: elem().
filter_packet(drop) ->
    drop;
filter_packet({From, To=#jid{luser=LUser, lserver=LServer}, Packet}) ->
    ?DEBUG("Receive packet~n    from ~p ~n    to ~p~n    packet ~p.",
              [From, To, Packet]),
    Packet2 =
    case ejabberd_users:is_user_exists(LUser, LServer) of
    false -> Packet;
    true ->
        case handle_package(incoming, true, To, From, From, Packet) of
            undefined -> Packet;
            Id -> 
                ?DEBUG("Archived incoming ~p", [Id]),
                BareTo = jlib:jid_to_binary(jlib:jid_remove_resource(To)),
                replace_archived_elem(BareTo, Id, Packet)
        end
    end,
    {From, To, Packet2}.


%% @doc A ejabberd's callback with diferent order of arguments.
remove_user(User, Server) ->
    delete_archive(Server, User).

%% ----------------------------------------------------------------------
%% Internal functions

-spec handle_package(Dir, ReturnId, LocJID, RemJID, SrcJID, Packet) ->
    MaybeId when
    Dir :: incoming | outgoing,
    ReturnId :: boolean(),
    LocJID :: jid(),
    RemJID :: jid(),
    SrcJID :: jid(),
    Packet :: elem(),
    MaybeId :: binary() | undefined.

handle_package(Dir, ReturnId,
               LocJID=#jid{},
               RemJID=#jid{},
               SrcJID=#jid{}, Packet) ->
    IsComplete = is_complete_message(Packet),
    case IsComplete of
        true ->
        IsInteresting =
        case get_behaviour(always, LocJID, RemJID) of
            always -> true;
            never  -> false;
            roster -> is_jid_in_user_roster(LocJID, RemJID)
        end,
        case IsInteresting of
            true -> 
            Id = generate_message_id(),
            archive_message(Id, Dir, LocJID, RemJID, SrcJID, Packet),
            case ReturnId of
                true  -> mess_id_to_external_binary(Id);
                false -> undefined
            end;
            false -> undefined
        end;
        false -> undefined
    end.

required_modules(Host) ->
    [prefs_module(Host),
     archive_module(Host),
     writer_module(Host)].

prefs_module(Host) ->
    gen_mod:get_module_opt(Host, ?MODULE, prefs_module, mod_mam_odbc_prefs).

archive_module(Host) ->
    gen_mod:get_module_opt(Host, ?MODULE, archive_module, mod_mam_odbc_arch).

writer_module(Host) ->
    gen_mod:get_module_opt(Host, ?MODULE, writer_module, mod_mam_odbc_async_writer).


get_behaviour(DefaultBehaviour, LocJID=#jid{lserver=LServer}, RemJID=#jid{}) ->
    M = prefs_module(LServer),
    M:get_behaviour(DefaultBehaviour, LocJID, RemJID).

update_settings(LServer, LUser, DefaultMode, AlwaysJIDs, NeverJIDs) ->
    M = prefs_module(LServer),
    M:update_settings(LServer, LUser, DefaultMode, AlwaysJIDs, NeverJIDs).

%% @doc Load settings from the database.
-spec get_prefs(LServer, LUser, GlobalDefaultMode) -> Result when
    LServer     :: server_hostname(),
    LUser       :: literal_username(),
    DefaultMode :: archive_behaviour(),
    GlobalDefaultMode :: archive_behaviour(),
    Result      :: {DefaultMode, AlwaysJIDs, NeverJIDs},
    AlwaysJIDs  :: [literal_jid()],
    NeverJIDs   :: [literal_jid()].

get_prefs(LServer, LUser, GlobalDefaultMode) ->
    M = prefs_module(LServer),
    M:get_prefs(LServer, LUser, GlobalDefaultMode).


remove_user_from_db(LServer, LUser) ->
    wait_flushing(LServer),
    PM = prefs_module(LServer),
    AM = archive_module(LServer),
    PM:remove_user_from_db(LServer, LUser),
    AM:remove_user_from_db(LServer, LUser),
    mod_mam_cache:remove_user_from_db(LServer, LUser),
    mod_mam_cache:remove_user_from_cache(LServer, LUser),
    ok.


message_row_to_xml({BMessID,BSrcJID,BPacket}, QueryID) ->
    MessID = list_to_integer(binary_to_list(BMessID)),
    {Microseconds, _NodeId} = decode_compact_uuid(MessID),
    Packet = binary_to_term(BPacket),
    SrcJID = jlib:binary_to_jid(BSrcJID),
    DateTime = calendar:now_to_universal_time(microseconds_to_now(Microseconds)),
    BExtMessID = mess_id_to_external_binary(MessID),
    wrap_message(Packet, QueryID, BExtMessID, DateTime, SrcJID).

message_row_to_ext_id({BMessID,_,_}) ->
    MessID = list_to_integer(binary_to_list(BMessID)),
    mess_id_to_external_binary(MessID).


-spec lookup_messages(UserJID, RSM, Start, End, WithJID, PageSize,
                      LimitPassed, MaxResultLimit) ->
    {ok, {TotalCount, Offset, MessageRows}} | {error, 'policy-violation'}
    when
    UserJID :: #jid{},
    RSM     :: #rsm_in{} | none,
    Start   :: unix_timestamp() | undefined,
    End     :: unix_timestamp() | undefined,
    WithJID :: #jid{} | undefined,
    PageSize :: non_neg_integer(),
    LimitPassed :: boolean(),
    MaxResultLimit :: non_neg_integer(),
    TotalCount :: non_neg_integer(),
    Offset  :: non_neg_integer(),
    MessageRows :: list(tuple()).
lookup_messages(UserJID=#jid{lserver=LServer}, RSM, Start, End,
                WithJID, PageSize, LimitPassed, MaxResultLimit) ->
    AM = archive_module(LServer),
    AM:lookup_messages(UserJID, RSM, Start, End, WithJID, PageSize,
                       LimitPassed, MaxResultLimit).

archive_message(Id, Dir,
                LocJID=#jid{lserver=LServer}, RemJID, SrcJID, Packet) ->
    M = writer_module(LServer),
    M:archive_message(Id, Dir, LocJID, RemJID, SrcJID, Packet).

%% ----------------------------------------------------------------------
%% Helpers


%% TODO: While it is too easy to use a `timer:sleep/1' here, it will cause delays.
wait_flushing(LServer) ->
    M = writer_module(LServer),
    M:wait_flushing(LServer).

