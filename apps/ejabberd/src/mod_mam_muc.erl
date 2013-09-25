%%%-------------------------------------------------------------------
%%% @author Uvarov Michael <arcusfelis@gmail.com>
%%% @copyright (C) 2013, Uvarov Michael
%%% @doc XEP-0313: Message Archive Management
%%%
%%% The module uses several backend modules:
%%%
%%% <ul>
%%% <li>Preference manager ({@link mod_mam_muc_odbc_prefs});</li>
%%% <li>Writer ({@link mod_mam_muc_odbc_arch} or {@link mod_mam_muc_odbc_async_writer});</li>
%%% <li>Archive manager ({@link mod_mam_muc_odbc_arch});</li>
%%% <li>User's ID generator ({@link mod_mam_muc_user}).</li>
%%% </ul>
%%%
%%% Preferencies can be also stored in Mnesia ({@link mod_mam_mnesia_prefs}).
%%% This module handles MUC archives.
%%%
%%% This module should be started for each host.
%%% Message archivation is not shaped here (use standard support for this).
%%% MAM's IQs are shaped inside {@link shaper_srv}.
%%%
%%% Message identifiers (or UIDs in the spec) are generated based on:
%%%
%%% <ul>
%%% <li>date (using `now()');</li>
%%% <li>node number (using {@link ejabberd_node_id}).</li>
%%% </ul>
%%% @end
%%%-------------------------------------------------------------------
-module(mod_mam_muc).

%% ----------------------------------------------------------------------
%% Exports

%% Client API
-export([delete_archive/2,
         archive_size/2,
         archive_id/2]).

%% Utils
-export([create_dump_file/2,
         restore_dump_file/3,
         debug_info/1]).

%% gen_mod handlers
-export([start/2, stop/1]).

%% ejabberd room handlers
-export([filter_room_packet/4,
         room_process_mam_iq/3,
         forget_room/2]).

%% ----------------------------------------------------------------------
%% Imports

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

%% ejabberd
-import(mod_mam_utils,
        [send_message/3]).


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


%% ----------------------------------------------------------------------
%% Other types
-type archive_behaviour() :: atom(). % roster | always | never.
-type message_id() :: non_neg_integer().
-type archive_id() :: non_neg_integer().
-type action() :: atom().

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
    remove_archive(LServer, LUser),
    ok.

archive_size(Server, User) ->
    LUser = jlib:nodeprep(User),
    LServer = jlib:nameprep(Server),
    ArcID = archive_id(LServer, LUser),
    AM = archive_module(LServer),
    AM:archive_size(LServer, LUser, ArcID).

archive_id(LServer, LUser) ->
    UM = user_module(LServer),
    UM:archive_id(LServer, LUser).

%% ----------------------------------------------------------------------
%% Utils API

debug_info(LServer) ->
    AM = archive_module(LServer),
    WM = writer_module(LServer),
    PM = prefs_module(LServer),
    [{archive_module, AM},
     {writer_module, WM},
     {prefs_module, PM}].


new_iterator(ArcJID=#jid{lserver = LServer, luser = LUser}) ->
    Now = mod_mam_utils:now_to_microseconds(now()),
    ArcID = archive_id(LServer, LUser),
    new_iterator(ArcJID, ArcID, undefined,
        undefined, undefined, Now, undefined, 50).

new_iterator(ArcJID, ArcID, RSM, Start, End, Now, WithJID, PageSize) ->
    fun() ->
        {ok, {TotalCount, Offset, MessageRows}} =
        lookup_messages(ArcJID, ArcID, RSM, Start, End, Now,
                        WithJID, PageSize, true, PageSize),
        Data = [exml:to_iolist(message_row_to_dump_xml(M))
                || M <- MessageRows],
        Cont = case is_last_page(TotalCount, Offset, PageSize) of
            false ->
                fun() -> {error, eof} end;
            true ->
                new_iterator(
                    ArcJID, ArcID, after_rsm(MessageRows),
                    Start, End, Now, WithJID, PageSize)
            end,
        {ok, {Data, Cont}}
        end.


after_rsm(MessageRows) ->
    {MessID,_SrcJID,_Packet} = lists:last(MessageRows),
    #rsm_in{direction = aft, id = MessID}.

is_last_page(TotalCount, Offset, PageSize) ->
    Offset - PageSize >= TotalCount.

create_dump_file(ArcJID, OutFileName) ->
    mod_mam_dump:create_dump_file(new_iterator(ArcJID), OutFileName).

-spec restore_dump_file(LocJID, InFileName, Opts) -> ok when
    LocJID :: #jid{},
    InFileName :: file:filename(),
    Opts :: [Opt],
    Opt :: {rewrite_jids, RewriterF | Substitutions},
    RewriterF :: fun((BinJID) -> BinJID),
    Substitutions :: [{BinJID, BinJID}],
    BinJID :: binary().
restore_dump_file(LocJID=#jid{lserver=LServer, luser=LUser}, InFileName, Opts) ->
    ArcID = archive_id(LServer, LUser),
    WriterF = fun(MessID, FromJID, ToJID, MessElem) ->
            case LocJID of
                FromJID ->
                    {error, outgoing_messages_are_not_supported};
                ToJID ->
                    archive_message(MessID, ArcID, incoming, LocJID,
                                    FromJID, FromJID, MessElem);
                _ ->
                    {error, no_local_jid}
            end
        end,
    mod_mam_dump:restore_dump_file(WriterF, LocJID, InFileName, Opts).

%% ----------------------------------------------------------------------
%% gen_mod callbacks
%% Starting and stopping functions for MUC archives

start(ServerHost, Opts) ->
    %% MUC host.
    Host = gen_mod:get_opt_host(ServerHost, Opts, <<"conference.@HOST@">>),
    ?DEBUG("mod_mam_muc starting", []),
    [start_module(Host, M) || M <- required_modules(Host)],
    IQDisc = gen_mod:get_opt(iqdisc, Opts, parallel), %% Type
    mod_disco:register_feature(Host, mam_ns_binary()),
    gen_iq_handler:add_iq_handler(mod_muc_iq, Host, mam_ns_binary(),
                                  ?MODULE, room_process_mam_iq, IQDisc),
    ejabberd_hooks:add(filter_room_packet, Host, ?MODULE,
                       filter_room_packet, 90),
    ejabberd_hooks:add(forget_room, Host, ?MODULE, forget_room, 90),
    ok.

stop(ServerHost) ->
    %% MUC host.
    Host = gen_mod:get_module_opt_host(ServerHost, ?MODULE, <<"conference.@HOST@">>),
    ?DEBUG("mod_mam stopping", []),
    ejabberd_hooks:add(filter_room_packet, Host, ?MODULE,
                       filter_room_packet, 90),
    gen_iq_handler:remove_iq_handler(mod_muc_iq, Host, mam_ns_string()),
    mod_disco:unregister_feature(Host, mam_ns_binary()),
    [stop_module(Host, M) || M <- required_modules(Host)],
    ok.

%% ----------------------------------------------------------------------
%% Control modules

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

required_modules(Host) ->
    expand_modules(Host, base_modules(Host)).

expand_modules(Host, Mods) ->
    expand_modules(Host, Mods, []).

expand_modules(Host, [H|T], Acc) ->
    case is_function_exist(H, required_modules, 1) of
        true  ->
            %% Do not load the same module twice.
            ReqMods = skip_expanded_modules(H:required_modules(Host), Acc),
            expand_modules(Host, T, [H] ++ ReqMods ++ Acc);
        false ->
            expand_modules(Host, T, [H|Acc])
    end;
expand_modules(_, [], Acc) ->
    lists:reverse(Acc).

skip_expanded_modules(Mods, ExpandedMods) ->
    [M || M <- Mods, not lists:member(M, ExpandedMods)].

base_modules(Host) ->
    [prefs_module(Host),
     archive_module(Host),
     writer_module(Host),
     user_module(Host)].

prefs_module(Host) ->
    gen_mod:get_module_opt(Host, ?MODULE, prefs_module, mod_mam_odbc_prefs).

archive_module(Host) ->
    gen_mod:get_module_opt(Host, ?MODULE, archive_module, mod_mam_muc_odbc_arch).

writer_module(Host) ->
    gen_mod:get_module_opt(Host, ?MODULE, writer_module, mod_mam_muc_odbc_arch).

user_module(Host) ->
    gen_mod:get_module_opt(Host, ?MODULE, user_module, mod_mam_odbc_user).

%% ----------------------------------------------------------------------
%% hooks and handlers for MUC

%% @doc Handle public MUC-message.
-spec filter_room_packet(Packet, FromNick, FromJID, RoomJID) -> Packet when
    Packet :: term(),
    FromNick :: binary(),
    RoomJID :: #jid{},
    FromJID :: #jid{}.
filter_room_packet(Packet, FromNick,
                   FromJID=#jid{},
                   RoomJID=#jid{lserver = LServer, luser = LUser}) ->
    ?DEBUG("Incoming room packet.", []),
    IsComplete = is_complete_message(Packet),
    ArcID = archive_id(LServer, LUser),
    case IsComplete of
        true ->
        %% Occupant JID <room@service/nick>
        SrcJID = jlib:jid_replace_resource(RoomJID, FromNick),
        IsInteresting =
        case get_behaviour(always, ArcID, RoomJID, SrcJID) of
            always -> true;
            never  -> false;
            roster -> true
        end,
        case IsInteresting of
            true -> 
            MessID = generate_message_id(),
            archive_message(MessID, ArcID, incoming,
                            RoomJID, FromJID, SrcJID, Packet),
            BareRoomJID = jlib:jid_to_binary(RoomJID),
            replace_archived_elem(BareRoomJID,
                                  mess_id_to_external_binary(MessID),
                                  Packet);
            false -> Packet
        end;
        false -> Packet
    end.

%% `To' is an account or server entity hosting the archive.
%% Servers that archive messages on behalf of local users SHOULD expose archives 
%% to the user on their bare JID (i.e. `From.luser'),
%% while a MUC service might allow MAM queries to be sent to the room's bare JID
%% (i.e `To.luser').
-spec room_process_mam_iq(From, To, IQ) -> IQ | ignore when
    From :: jid(),
    To :: jid(),
    IQ :: #iq{}.
room_process_mam_iq(From=#jid{lserver=Host}, To, IQ) ->
    Action = iq_action(IQ),
    case is_action_allowed(Action, From, To) of
        true  -> 
            case shaper_srv:wait(Host, action_to_shaper_name(Action), From, 1) of
                ok ->
                    handle_mam_iq(Action, From, To, IQ);
                {error, max_delay_reached} ->
                    return_max_delay_reached_error_iq(IQ)
            end;
        false -> return_action_not_allowed_error_iq(IQ)
    end.

%% This hook is called from `mod_muc:forget_room(Host, Name)'.
forget_room(LServer, RoomName) ->
    delete_archive(LServer, RoomName).

%% ----------------------------------------------------------------------
%% Internal functions

is_action_allowed(Action, From, To=#jid{lserver=Host}) ->
    case acl:match_rule(Host, Action, From) of
        allow   -> true;
        deny    -> false;
        default -> is_action_allowed_by_default(Action, From, To)
    end.

-spec is_action_allowed_by_default(Action, From, To) -> boolean() when
    Action  :: action(),
    From    :: jid(),
    To      :: jid().
is_action_allowed_by_default(Action, From, To) ->
    is_room_action_allowed_by_default(Action, From, To).

is_room_action_allowed_by_default(Action, From, To) ->
    case action_type(Action) of
        set -> is_room_owner(From, To);
        get -> true
    end.

is_room_owner(From, To) ->
    case mod_mam_room:is_room_owner(To, From) of
        {error, _} -> false;
        {ok, IsOwner} -> IsOwner
    end.

action_type(mam_get_prefs)                  -> get;
action_type(mam_set_prefs)                  -> set;
action_type(mam_lookup_messages)            -> get;
action_type(mam_purge_single_message)       -> set;
action_type(mam_purge_multiple_messages)    -> set.

-spec action_to_shaper_name(action()) -> atom().
action_to_shaper_name(Action) -> list_to_atom(atom_to_list(Action) ++ "_shaper").

handle_mam_iq(Action, From, To, IQ) ->
    case Action of
    mam_get_prefs ->
        handle_get_prefs(To, IQ);
    mam_set_prefs ->
        handle_set_prefs(To, IQ);
    mam_lookup_messages ->
        handle_lookup_messages(From, To, IQ);
    mam_purge_single_message ->
        handle_purge_single_message(To, IQ);
    mam_purge_multiple_messages ->
        handle_purge_multiple_messages(To, IQ)
    end.

iq_action(#iq{type = Action, sub_el = SubEl = #xmlel{name = Category}}) ->
    case {Action, Category} of
        {set, <<"prefs">>} -> mam_set_prefs;
        {get, <<"prefs">>} -> mam_get_prefs;
        {get, <<"query">>} -> mam_lookup_messages;
        {set, <<"purge">>} ->
            case xml:get_tag_attr_s(<<"id">>, SubEl) of
                <<>> -> mam_purge_multiple_messages;
                _    -> mam_purge_single_message
            end
    end.

handle_set_prefs(#jid{luser = LUser, lserver = LServer},
                 IQ=#iq{sub_el = PrefsEl}) ->
    {DefaultMode, AlwaysJIDs, NeverJIDs} = parse_prefs(PrefsEl),
    ?DEBUG("Parsed data~n\tDefaultMode ~p~n\tAlwaysJIDs ~p~n\tNeverJIDS ~p~n",
              [DefaultMode, AlwaysJIDs, NeverJIDs]),
    ArcID = archive_id(LServer, LUser),
    set_prefs(LServer, LUser, ArcID, DefaultMode, AlwaysJIDs, NeverJIDs),
    ResultPrefsEl = result_prefs(DefaultMode, AlwaysJIDs, NeverJIDs),
    IQ#iq{type = result, sub_el = [ResultPrefsEl]}.

handle_get_prefs(#jid{luser = LUser, lserver = LServer}, IQ) ->
    ArcID = archive_id(LServer, LUser),
    {DefaultMode, AlwaysJIDs, NeverJIDs} =
        get_prefs(LServer, LUser, ArcID, always),
    ?DEBUG("Extracted data~n\tDefaultMode ~p~n\tAlwaysJIDs ~p~n\tNeverJIDS ~p~n",
              [DefaultMode, AlwaysJIDs, NeverJIDs]),
    ResultPrefsEl = result_prefs(DefaultMode, AlwaysJIDs, NeverJIDs),
    IQ#iq{type = result, sub_el = [ResultPrefsEl]}.
    
handle_lookup_messages(
        From,
        To=#jid{luser = LUser, lserver = LServer},
        IQ=#iq{sub_el = QueryEl}) ->
    Now = mod_mam_utils:now_to_microseconds(now()),
    ArcID = archive_id(LServer, LUser),
    QueryID = xml:get_tag_attr_s(<<"queryid">>, QueryEl),
    wait_flushing(LServer),
    %% Filtering by date.
    %% Start :: integer() | undefined
    Start = elem_to_start_microseconds(QueryEl),
    End   = elem_to_end_microseconds(QueryEl),
    %% Filtering by contact.
    With  = elem_to_with_jid(QueryEl),
    RSM   = fix_rsm(jlib:rsm_decode(QueryEl)),
    Limit = elem_to_limit(QueryEl),
    PageSize = min(max_result_limit(),
                   maybe_integer(Limit, default_result_limit())),
    LimitPassed = Limit =/= <<>>,
    case lookup_messages(To, ArcID, RSM, Start, End, Now, With, PageSize,
                         LimitPassed, max_result_limit()) of
    {error, 'policy-violation'} ->
        ?DEBUG("Policy violation by ~p.", [LUser]),
        ErrorEl = ?STANZA_ERRORT(<<"">>, <<"modify">>, <<"policy-violation">>,
                                 <<"en">>, <<"Too many results">>),          
        IQ#iq{type = error, sub_el = [ErrorEl]};
    {ok, {TotalCount, Offset, MessageRows}} ->
        {FirstMessID, LastMessID} =
            case MessageRows of
                []    -> {undefined, undefined};
                [_|_] -> {message_row_to_ext_id(hd(MessageRows)),
                          message_row_to_ext_id(lists:last(MessageRows))}
            end,
        [send_message(To, From, message_row_to_xml(M, QueryID))
         || M <- MessageRows],
        ResultSetEl = result_set(FirstMessID, LastMessID, Offset, TotalCount),
        ResultQueryEl = result_query(ResultSetEl),
        %% On receiving the query, the server pushes to the client a series of
        %% messages from the archive that match the client's given criteria,
        %% and finally returns the <iq/> result.
        IQ#iq{type = result, sub_el = [ResultQueryEl]}
    end.

%% Purging multiple messages.
handle_purge_multiple_messages(To=#jid{lserver = LServer, luser = LUser},
                               IQ=#iq{sub_el = PurgeEl}) ->
    Now = mod_mam_utils:now_to_microseconds(now()),
    wait_flushing(LServer),
    %% Filtering by date.
    %% Start :: integer() | undefined
    Start = elem_to_start_microseconds(PurgeEl),
    End   = elem_to_end_microseconds(PurgeEl),
    %% Filtering by contact.
    With  = elem_to_with_jid(PurgeEl),
    ArcID = archive_id(LServer, LUser),
    purge_multiple_messages(To, ArcID, Start, End, Now, With),
    return_purge_success(IQ).

handle_purge_single_message(To=#jid{lserver = LServer, luser = LUser},
                            IQ=#iq{sub_el = PurgeEl}) ->
    Now = mod_mam_utils:now_to_microseconds(now()),
    wait_flushing(LServer),
    BExtMessID = xml:get_tag_attr_s(<<"id">>, PurgeEl),
    MessID = mod_mam_utils:external_binary_to_mess_id(BExtMessID),
    ArcID = archive_id(LServer, LUser),
    PurgingResult = purge_single_message(To, ArcID, MessID, Now),
    return_purge_single_message_iq(IQ, PurgingResult).

get_behaviour(DefaultBehaviour, ArcID,
              LocJID=#jid{lserver=LServer}, RemJID=#jid{}) ->
    M = prefs_module(LServer),
    M:get_behaviour(DefaultBehaviour, ArcID, LocJID, RemJID).

set_prefs(LServer, LUser, ArcID, DefaultMode, AlwaysJIDs, NeverJIDs) ->
    M = prefs_module(LServer),
    M:set_prefs(LServer, LUser, ArcID, DefaultMode, AlwaysJIDs, NeverJIDs).

%% @doc Load settings from the database.
-spec get_prefs(LServer, LUser, ArcID, GlobalDefaultMode) -> Result when
    LServer     :: server_hostname(),
    LUser       :: literal_username(),
    ArcID       :: archive_id(),
    DefaultMode :: archive_behaviour(),
    GlobalDefaultMode :: archive_behaviour(),
    Result      :: {DefaultMode, AlwaysJIDs, NeverJIDs},
    AlwaysJIDs  :: [literal_jid()],
    NeverJIDs   :: [literal_jid()].
get_prefs(LServer, LUser, ArcID, GlobalDefaultMode) ->
    M = prefs_module(LServer),
    M:get_prefs(LServer, LUser, ArcID, GlobalDefaultMode).

remove_archive(LServer, LUser) ->
    ArcID = archive_id(LServer, LUser),
    wait_flushing(LServer),
    PM = prefs_module(LServer),
    AM = archive_module(LServer),
    UM = user_module(LServer),
    PM:remove_archive(LServer, LUser, ArcID),
    AM:remove_archive(LServer, LUser, ArcID),
    UM:remove_archive(LServer, LUser, ArcID),
    ok.

message_row_to_ext_id({MessID,_,_}) ->
    mess_id_to_external_binary(MessID).

message_row_to_xml({MessID,SrcJID,Packet}, QueryID) ->
    {Microseconds, _NodeMessID} = decode_compact_uuid(MessID),
    DateTime = calendar:now_to_universal_time(microseconds_to_now(Microseconds)),
    BExtMessID = mess_id_to_external_binary(MessID),
    wrap_message(Packet, QueryID, BExtMessID, DateTime, SrcJID).

message_row_to_dump_xml(M) ->
     xml:get_subtag(message_row_to_xml(M, undefined), <<"result">>).

-spec lookup_messages(ArcJID, ArcID, RSM, Start, End, Now, WithJID,
                      PageSize, LimitPassed, MaxResultLimit) ->
    {ok, {TotalCount, Offset, MessageRows}} | {error, 'policy-violation'}
    when
    ArcJID  :: #jid{},
    ArcID   :: archive_id(),
    RSM     :: #rsm_in{} | undefined,
    Start   :: unix_timestamp() | undefined,
    End     :: unix_timestamp() | undefined,
    Now     :: unix_timestamp(),
    WithJID :: #jid{} | undefined,
    PageSize :: non_neg_integer(),
    LimitPassed :: boolean(),
    MaxResultLimit :: non_neg_integer(),
    TotalCount :: non_neg_integer(),
    Offset  :: non_neg_integer(),
    MessageRows :: list(tuple()).
lookup_messages(ArcJID=#jid{lserver=LServer}, ArcID, RSM, Start, End, Now,
                WithJID, PageSize, LimitPassed, MaxResultLimit) ->
    AM = archive_module(LServer),
    AM:lookup_messages(ArcJID, ArcID, RSM, Start, End, Now, WithJID,
                       PageSize, LimitPassed, MaxResultLimit).

archive_message(MessID, ArcID, Dir,
                LocJID=#jid{lserver=LServer}, RemJID, SrcJID, Packet) ->
    M = writer_module(LServer),
    M:archive_message(MessID, ArcID, Dir, LocJID, RemJID, SrcJID, Packet).


-spec purge_single_message(ArcJID, ArcID, MessID, Now) ->
    ok | {error, 'not-found'} when
    ArcJID :: #jid{},
    ArcID  :: archive_id(),
    MessID :: message_id(),
    Now :: unix_timestamp().
purge_single_message(ArcJID=#jid{lserver=LServer}, ArcID, MessID, Now) ->
    AM = archive_module(LServer),
    AM:purge_single_message(ArcJID, ArcID, MessID, Now).

-spec purge_multiple_messages(ArcJID, ArcID, Start, End, Now, WithJID) -> ok
    when
    ArcJID  :: #jid{},
    ArcID   :: archive_id(),
    Start   :: unix_timestamp() | undefined,
    End     :: unix_timestamp() | undefined,
    Now     :: unix_timestamp(),
    WithJID :: #jid{} | undefined.
purge_multiple_messages(ArcJID=#jid{lserver=LServer},
    ArcID, Start, End, Now, WithJID) ->
    AM = archive_module(LServer),
    AM:purge_multiple_messages(ArcJID, ArcID, Start, End, Now, WithJID).

%% ----------------------------------------------------------------------
%% Helpers


wait_flushing(LServer) ->
    M = writer_module(LServer),
    M:wait_flushing(LServer).

maybe_jid(<<>>) ->
    undefined;
maybe_jid(JID) when is_binary(JID) ->
    jlib:binary_to_jid(JID).


%% @doc Convert id into internal format.
fix_rsm(none) ->
    undefined;
fix_rsm(RSM=#rsm_in{id = undefined}) ->
    RSM;
fix_rsm(RSM=#rsm_in{id = <<>>}) ->
    RSM#rsm_in{id = undefined};
fix_rsm(RSM=#rsm_in{id = BExtMessID}) when is_binary(BExtMessID) ->
    MessID = mod_mam_utils:external_binary_to_mess_id(BExtMessID),
    RSM#rsm_in{id = MessID}.

elem_to_start_microseconds(El) ->
    maybe_microseconds(xml:get_path_s(El, [{elem, <<"start">>}, cdata])).

elem_to_end_microseconds(El) ->
    maybe_microseconds(xml:get_path_s(El, [{elem, <<"start">>}, cdata])).

elem_to_with_jid(El) ->
    maybe_jid(xml:get_path_s(El, [{elem, <<"with">>}, cdata])).

%% This element's name is "limit".
%% But it must be "max" according XEP-0313.
elem_to_limit(QueryEl) ->
    get_one_of_path(QueryEl, [
        [{elem, <<"set">>}, {elem, <<"max">>}, cdata],
        [{elem, <<"set">>}, {elem, <<"limit">>}, cdata]
    ]).

return_purge_success(IQ) ->
    IQ#iq{type = result, sub_el = []}.

return_action_not_allowed_error_iq(IQ) ->
    ErrorEl = ?STANZA_ERRORT(<<"">>, <<"cancel">>, <<"not-allowed">>,
         <<"en">>, <<"The action is not allowed.">>),
    IQ#iq{type = error, sub_el = [ErrorEl]}.

return_purge_not_found_error_iq(IQ) ->
    %% Message not found.
    ErrorEl = ?STANZA_ERRORT(<<"">>, <<"cancel">>, <<"item-not-found">>,
         <<"en">>, <<"The provided UID did not match any message stored in archive.">>),
    IQ#iq{type = error, sub_el = [ErrorEl]}.

return_max_delay_reached_error_iq(IQ) ->
    %% Message not found.
    ErrorEl = ?ERRT_RESOURCE_CONSTRAINT(
        <<"en">>, <<"The action is cancelled because of flooding.">>),
    IQ#iq{type = error, sub_el = [ErrorEl]}.

return_purge_single_message_iq(IQ, ok) ->
    return_purge_success(IQ);
return_purge_single_message_iq(IQ, {error, 'not-found'}) ->
    return_purge_not_found_error_iq(IQ).
