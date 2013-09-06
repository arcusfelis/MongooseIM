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
%%% There are two backends: for MySQL and for Riak.
%%% Preferencies can be also stored in Mnesia ({@link mod_mam_mnesia_prefs}).
%%% This module handles both MUC and simple archives.
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
-module(mod_mam).
-behavior(gen_mod).

%% ----------------------------------------------------------------------
%% Exports

%% Client API
-export([delete_archive/2,
         archive_size/2,
         archive_id/2]).

%% gen_mod handlers
-export([start/2, stop/1]).

%% ejabberd handlers
-export([process_mam_iq/3,
         user_send_packet/3,
         remove_user/2,
         filter_packet/1]).

%% ejabberd room handlers
-export([filter_room_packet/4,
         room_process_mam_iq/3,
         forget_room/2]).

%% Utils
-export([create_dump_file/2,
         restore_dump_file/3,
         debug_info/1]).

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
-type message_id() :: non_neg_integer().
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
    AM = archive_module(LServer),
    AM:archive_size(LServer, LUser).

archive_id(LServer, LUser) ->
    UM = user_module(LServer),
    UM:archive_id(LServer, LUser).

debug_info(LServer) ->
    AM = archive_module(LServer),
    WM = writer_module(LServer),
    PM = prefs_module(LServer),
    [{archive_module, AM},
     {writer_module, WM},
     {prefs_module, PM}].

%% ----------------------------------------------------------------------
%% Utils API

create_dump_file(ArcJID, OutFileName) ->
    {ok, FD} = file:open(OutFileName, [write]),
    F = fun(Data, _) -> file:write(FD, Data) end,
    F(<<"<stream>">>, undefined),
    Now = mod_mam_utils:now_to_microseconds(now()),
    create_dump_cycle(
        F, undefined, ArcJID, undefined,
        undefined, undefined, Now, undefined, 50),
    F(<<"</stream>">>, undefined),
    file:close(FD).

create_dump_cycle(F, Acc, ArcJID, RSM, From, To, Now, WithJID, PageSize) ->
    {ok, {TotalCount, Offset, MessageRows}} =
    lookup_messages(ArcJID, RSM, From, To, Now,
                    WithJID, PageSize, true, PageSize),
    Data = [exml:to_iolist(message_row_to_dump_xml(M)) || M <- MessageRows],
    Acc1 = F(Data, Acc),
    case is_last_page(TotalCount, Offset, PageSize) of
    false ->
        Acc1;
    true ->
        create_dump_cycle(
            F, Acc1, ArcJID, after_rsm(MessageRows),
            From, To, Now, WithJID, PageSize)
    end.

message_row_to_dump_xml(M) ->
     xml:get_subtag(message_row_to_xml(M, undefined), <<"result">>).

after_rsm(MessageRows) ->
    {MessID,_SrcJID,_Packet} = lists:last(MessageRows),
    #rsm_in{direction = aft, id = MessID}.

is_last_page(TotalCount, Offset, PageSize) ->
    Offset - PageSize >= TotalCount.


-spec restore_dump_file(LocJID, InFileName, Opts) -> ok when
    LocJID :: #jid{},
    InFileName :: file:filename(),
    Opts :: [Opt],
    Opt :: {rewrite_jids, RewriterF | Substitutions},
    RewriterF :: fun((BinJID) -> BinJID),
    Substitutions :: [{BinJID, BinJID}],
    BinJID :: binary().
restore_dump_file(LocJID, InFileName, Opts) ->
    {ok, StreamAcc} = file:open(InFileName, [read, binary]),
    StreamF = fun read_data/1,
    InsF = fun insert_message_iter/2,
    {CallF1, CallAcc1} = apply_rewrite_jids(InsF, LocJID, Opts),
    prepare_res(parse_xml_stream(StreamF, StreamAcc, CallF1, CallAcc1)).

prepare_res({ok, _, _}) -> ok;
prepare_res({stop, _, _, _}) -> ok;
prepare_res({error, Reason, _, _}) -> {error, Reason}.

apply_rewrite_jids(CallF, InitCallAcc, Opts) ->
    case proplists:get_value(rewrite_jids, Opts) of
        undefined ->
            {CallF, InitCallAcc};
        RewriterOpts ->
            RewriterF = rewrite_opts_to_fun(RewriterOpts),
            {fun(ResElem=#xmlel{}, CallAcc) ->
                    CallF(rewrite_jids(ResElem, RewriterF), CallAcc);
                (Event, CallAcc) ->
                    ?DEBUG("Skipped ~p.", [Event]),
                    CallF(Event, CallAcc)
             end, InitCallAcc}
    end.

rewrite_opts_to_fun(RewriterF) when is_function(RewriterF) ->
    RewriterF;
rewrite_opts_to_fun(Substitutions) when is_list(Substitutions) ->
    substitute_fun(Substitutions).

substitute_fun(Substitutions) ->
    Dict = dict:from_list(Substitutions),
    fun(Orig) -> dict:fetch(Orig, Dict) end.

insert_message_iter(ResElem=#xmlel{}, LocJID) ->
    case insert_xml_message(ResElem, LocJID) of
        {error, Reason} ->
            {error, Reason, LocJID};
        ok ->
            {ok, LocJID}
    end;
insert_message_iter(_, LocJID) ->
    %% Skip `#xmlstreamelement{}' and `#xmlstreamend{}'
    {ok, LocJID}.

read_data(FD) ->
    case file:read(FD, 1024) of
    {ok, Data} ->
        {ok, Data, FD};
    eof ->
        file:close(FD),
        {stop, eof, FD}
    end.

-spec parse_xml_stream(StreamF, StreamAcc, CallF, CallAcc) -> Res when
    StreamF   :: fun((StreamAcc) -> StreamRes),
    CallF     :: fun((Elem, CallAcc) -> CallRes),
    CallRes   :: {ok, CallAcc} | {stop, Reason, CallAcc},
    StreamRes :: {ok, Data, StreamAcc} | {stop, Reason, StreamAcc},
    Res       :: {ok, StreamAcc, CallAcc}
               | {stop,  Reason, StreamAcc, CallAcc}
               | {error, Reason, StreamAcc, CallAcc},
    Data      :: binary(),
    Elem      :: term(),
    StreamAcc :: term(),
    CallAcc   :: term().

parse_xml_stream(StreamF, StreamAcc, CallF, CallAcc) ->
    {ok, Parser} = exml_stream:new_parser(),
    parse_xml_stream_cycle(StreamF, StreamAcc, CallF, CallAcc, Parser).

parse_xml_stream_cycle(StreamF, StreamAcc, CallF, CallAcc, Parser) ->
    case StreamF(StreamAcc) of
    {ok, Data, StreamAcc1} ->
        case exml_stream:parse(Parser, Data) of
        {ok, Parser1, Elems} ->
            ?DEBUG("Parsed ~p elements.", [length(Elems)]),
            case stopable_foldl(CallF, CallAcc, Elems) of
            {ok, CallAcc1} ->
            parse_xml_stream_cycle(
                StreamF, StreamAcc1, CallF, CallAcc1, Parser1);
            {stop, Reason, CallAcc1} ->
                {stop, Reason, StreamAcc1, CallAcc1};
            {error, Reason, CallAcc1} ->
                {error, Reason, StreamAcc1, CallAcc1}
            end;
        {error, Error} ->
            {error, Error, StreamAcc1, CallAcc}
        end;
    {stop, Reason, StreamAcc1} ->
        exml_stream:free_parser(Parser),
        {stop, Reason, StreamAcc1, CallAcc}
    end.

stopable_foldl(F, Acc, [H|T]) ->
    case F(H, Acc) of
        {ok, Acc1} -> stopable_foldl(F, Acc1, T);
        {stop, Reason, Acc1} -> {stop, Reason, Acc1};
        {error, Reason, Acc1} -> {error, Reason, Acc1}
    end;
stopable_foldl(_F, Acc, []) ->
    {ok, Acc}.

debug_rewriting(F) ->
    fun(From) ->
        To = F(From),
        ?DEBUG("Rewrote from ~p to ~p.", [From, To]),
        To
    end.

-spec rewrite_jids(ResElem, RewriterF) -> ResElem when
    ResElem :: #xmlel{},
    RewriterF :: fun((BinJID) -> BinJID).
rewrite_jids(ResElem, F) when is_function(F) ->
    F1 = debug_rewriting(F),
    WithMsgF = fun(MsgElem) ->
        update_tag_attr(F1, <<"to">>,
        update_tag_attr(F1, <<"from">>, MsgElem))
        end,
    WithDelayF = fun(DelayElem) ->
        update_tag_attr(F1, <<"from">>, DelayElem)
        end,
    WithFwdF = fun(FwdElem) ->
        update_sub_tag(WithDelayF, <<"delay">>,
        update_sub_tag(WithMsgF, <<"message">>, FwdElem))
        end,
    update_sub_tag(WithFwdF, <<"forwarded">>, ResElem).


-spec update_tag_attr(F, Name, Elem) -> Elem when
    F :: fun((Value) -> Value),
    Name :: binary(),
    Value :: binary(),
    Elem :: #xmlel{}.
update_tag_attr(F, Name, Elem)
    when is_function(F, 1), is_binary(Name) ->
    Value = xml:get_tag_attr_s(Name, Elem),
    xml:replace_tag_attr(Name, F(Value), Elem).

-spec update_sub_tag(F, Name, Elem) -> Elem when
    F :: fun((Elem) -> Elem),
    Name :: binary(),
    Elem :: #xmlel{}.
update_sub_tag(F, Name, Elem=#xmlel{children=Children})
    when is_function(F, 1), is_binary(Name) ->
    Elem#xmlel{children=update_tags(F, Name, Children)};
update_sub_tag(_, _, Elem) ->
    Elem.

update_tags(F, Name, [H=#xmlel{name=Name}|T]) ->
    [F(H)|update_tags(F, Name, T)];
update_tags(F, Name, [H|T]) ->
    [H|update_tags(F, Name, T)];
update_tags(_, _, []) ->
    [].

%% @doc Insert a message into archive.
%% `ResElem' is `<result><forwarded>...</forwarded></result>'.
%% This format is used inside dump files.
-spec insert_xml_message(ResElem, LocJID) -> ok | {error, Reason} when
    ResElem :: #xmlel{},
    LocJID :: #jid{},
    Reason :: term().
insert_xml_message(ResElem, LocJID) ->
    FwdElem = xml:get_subtag(ResElem, <<"forwarded">>),
    MessElem = xml:get_subtag(FwdElem, <<"message">>),
    BExtMessID = xml:get_tag_attr_s(<<"id">>, ResElem),
    MessID = mod_mam_utils:external_binary_to_mess_id(BExtMessID),
    FromJID = jlib:binary_to_jid(xml:get_tag_attr_s(<<"from">>, MessElem)),
    ToJID   = jlib:binary_to_jid(xml:get_tag_attr_s(<<"to">>, MessElem)),
    ?DEBUG("Restore message with id ~p.", [MessID]),
    case LocJID of
    FromJID ->
        archive_message(MessID, outgoing, LocJID, FromJID, FromJID, MessElem);
    ToJID ->
        archive_message(MessID, incoming, LocJID, FromJID, FromJID, MessElem);
    _ ->
        {error, no_local_jid}
    end.

%% ----------------------------------------------------------------------
%% gen_mod callbacks

start(Host, Opts) ->
    case gen_mod:get_opt(muc, Opts, false) of
        false -> start_for_users(Host, Opts);
        true  -> start_for_rooms(Host, rewrite_default_muc_modules(Host, Opts))
    end.

stop(Host) ->
    case gen_mod:get_module_opt(Host, muc, ?MODULE, false) of
        false -> stop_for_users(Host);
        true  -> stop_for_rooms(Host)
    end.

%% ----------------------------------------------------------------------
%% Starting and stopping functions for users' archives

start_for_users(Host, Opts) ->
    ?DEBUG("mod_mam starting", []),
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

stop_for_users(Host) ->
    ?DEBUG("mod_mam stopping", []),
    ejabberd_hooks:delete(user_send_packet, Host, ?MODULE, user_send_packet, 90),
    ejabberd_hooks:delete(filter_packet, global, ?MODULE, filter_packet, 90),
    ejabberd_hooks:delete(remove_user, Host, ?MODULE, remove_user, 50),
    gen_iq_handler:remove_iq_handler(ejabberd_sm, Host, mam_ns_string()),
    mod_disco:unregister_feature(Host, mam_ns_binary()),
    [stop_module(Host, M) || M <- required_modules(Host)],
    ok.

%% ----------------------------------------------------------------------
%% Starting and stopping functions for MUC archives

start_for_rooms(Host, Opts) ->
    ?DEBUG("mod_mam_muc starting", []),
    IQDisc = gen_mod:get_opt(iqdisc, Opts, parallel), %% Type
    mod_disco:register_feature(Host, mam_ns_binary()),
    gen_iq_handler:add_iq_handler(mod_muc_iq, Host, mam_ns_binary(),
                                  ?MODULE, room_process_mam_iq, IQDisc),
    ejabberd_hooks:add(filter_room_packet, Host, ?MODULE,
                       filter_room_packet, 90),
    ejabberd_hooks:add(forget_room, Host, ?MODULE, forget_room, 90),
    [start_module(Host, M) || M <- required_modules(Host)],
    ok.

stop_for_rooms(Host) ->
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
    gen_mod:get_module_opt(Host, ?MODULE, archive_module, mod_mam_odbc_arch).

writer_module(Host) ->
    gen_mod:get_module_opt(Host, ?MODULE, writer_module, mod_mam_odbc_arch).

user_module(Host) ->
    gen_mod:get_module_opt(Host, ?MODULE, user_module, mod_mam_odbc_user).

default_muc_modules() ->
    [{archive_module, mod_mam_muc_odbc_arch}
    ,{writer_module, mod_mam_muc_odbc_arch}].

rewrite_default_muc_modules(Host, Opts) ->
    rewrite_default_options(Host, default_muc_modules(), Opts).

rewrite_default_options(Host, [{K, V}|DefOpts], Opts) ->
    NewOpts = rewrite_default_option(Host, K, V, Opts),
    rewrite_default_options(Host, DefOpts, NewOpts);
rewrite_default_options(_Host, [], NewOpts) ->
    NewOpts.
    
rewrite_default_option(Host, K, NewV, Opts) ->
    case gen_mod:get_opt(K, Opts, default) of
        default ->
            gen_mod:set_module_opt(Host, ?MODULE, K, NewV),
            gen_mod:set_opt(K, Opts, NewV);
        _ ->
            Opts
    end.

%% ----------------------------------------------------------------------
%% hooks and handlers

%% `To' is an account or server entity hosting the archive.
%% Servers that archive messages on behalf of local users SHOULD expose archives 
%% to the user on their bare JID (i.e. `From.luser'),
%% while a MUC service might allow MAM queries to be sent to the room's bare JID
%% (i.e `To.luser').
-spec process_mam_iq(From, To, IQ) -> IQ | ignore when
    From :: jid(),
    To :: jid(),
    IQ :: #iq{}.
process_mam_iq(From, To, IQ) ->
    process_mam_iq(user, From, To, IQ).

process_mam_iq(ArcType, From=#jid{lserver=Host}, To, IQ) ->
    Action = iq_action(IQ),
    case is_action_allowed(ArcType, Action, From, To) of
        true  -> 
            case shaper_srv:wait(Host, action_to_shaper_name(Action), From, 1) of
                ok ->
                    handle_mam_iq(Action, From, To, IQ);
                {error, max_delay_reached} ->
                    return_max_delay_reached_error_iq(IQ)
            end;
        false -> return_action_not_allowed_error_iq(IQ)
    end.

is_action_allowed(ArcType, Action, From, To=#jid{lserver=Host}) ->
    case acl:match_rule(Host, Action, From) of
        allow   -> true;
        deny    -> false;
        default -> is_action_allowed_by_default(ArcType, Action, From, To)
    end.

-spec is_action_allowed_by_default(ArcType, Action, From, To) -> boolean() when
    ArcType :: user | room,
    Action  :: action(),
    From    :: jid(),
    To      :: jid().
is_action_allowed_by_default(user, _Action, From, To) ->
    compare_bare_jids(From, To);
is_action_allowed_by_default(room, Action, From, To) ->
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

compare_bare_jids(JID1, JID2) ->
    jlib:jid_remove_resource(JID1) =:=
    jlib:jid_remove_resource(JID2).

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
%% hooks and handlers for MUC

%% @doc Handle public MUC-message.
filter_room_packet(Packet, FromNick, FromJID,
                   RoomJID=#jid{lserver = LServer, luser = RoomName}) ->
    ?DEBUG("Incoming room packet.", []),
    IsComplete = is_complete_message(Packet),
    case IsComplete of
        true ->
        %% Occupant JID <room@service/nick>
        SrcJID = jlib:jid_replace_resource(RoomJID, FromNick),
        IsInteresting =
        case get_behaviour(always, RoomJID, SrcJID) of
            always -> true;
            never  -> false;
            roster -> true
        end,
        case IsInteresting of
            true -> 
            Id = generate_message_id(),
            archive_message(Id, incoming, RoomJID, FromJID, SrcJID, Packet),
            BareRoomJID = jlib:jid_to_binary(RoomJID),
            replace_archived_elem(BareRoomJID,
                                  mess_id_to_external_binary(Id),
                                  Packet);
            false -> Packet
        end;
        false -> Packet
    end.

%% `process_mam_iq/3' is simular.
-spec room_process_mam_iq(From, To, IQ) -> IQ | ignore when
    From :: jid(),
    To :: jid(),
    IQ :: #iq{}.
room_process_mam_iq(From, To, IQ) ->
    process_mam_iq(room, From, To, IQ).

%% This hook is called from `mod_muc:forget_room(Host, Name)'.
forget_room(LServer, RoomName) ->
    delete_archive(LServer, RoomName).

%% ----------------------------------------------------------------------
%% Internal functions

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
    set_prefs(LServer, LUser, DefaultMode, AlwaysJIDs, NeverJIDs),
    ResultPrefsEl = result_prefs(DefaultMode, AlwaysJIDs, NeverJIDs),
    IQ#iq{type = result, sub_el = [ResultPrefsEl]}.

handle_get_prefs(#jid{luser = LUser, lserver = LServer}, IQ) ->
    {DefaultMode, AlwaysJIDs, NeverJIDs} = get_prefs(LServer, LUser, always),
    ?DEBUG("Extracted data~n\tDefaultMode ~p~n\tAlwaysJIDs ~p~n\tNeverJIDS ~p~n",
              [DefaultMode, AlwaysJIDs, NeverJIDs]),
    ResultPrefsEl = result_prefs(DefaultMode, AlwaysJIDs, NeverJIDs),
    IQ#iq{type = result, sub_el = [ResultPrefsEl]}.
    
handle_lookup_messages(
        From,
        To=#jid{luser = LUser, lserver = LServer},
        IQ=#iq{sub_el = QueryEl}) ->
    Now = mod_mam_utils:now_to_microseconds(now()),
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
    case lookup_messages(To, RSM, Start, End, Now, With, PageSize,
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

%% Purging multiple messages.
handle_purge_multiple_messages(To=#jid{lserver = LServer},
                               IQ=#iq{sub_el = PurgeEl}) ->
    Now = mod_mam_utils:now_to_microseconds(now()),
    wait_flushing(LServer),
    %% Filtering by date.
    %% Start :: integer() | undefined
    Start = elem_to_start_microseconds(PurgeEl),
    End   = elem_to_end_microseconds(PurgeEl),
    %% Filtering by contact.
    With  = elem_to_with_jid(PurgeEl),
    purge_multiple_messages(To, Start, End, Now, With),
    return_purge_success(IQ).

handle_purge_single_message(To=#jid{lserver = LServer},
                            IQ=#iq{sub_el = PurgeEl}) ->
    Now = mod_mam_utils:now_to_microseconds(now()),
    wait_flushing(LServer),
    BExtMessID = xml:get_tag_attr_s(<<"id">>, PurgeEl),
    MessID = mod_mam_utils:external_binary_to_mess_id(BExtMessID),
    PurgingResult = purge_single_message(To, MessID, Now),
    return_purge_single_message_iq(IQ, PurgingResult).

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


get_behaviour(DefaultBehaviour, LocJID=#jid{lserver=LServer}, RemJID=#jid{}) ->
    M = prefs_module(LServer),
    M:get_behaviour(DefaultBehaviour, LocJID, RemJID).

set_prefs(LServer, LUser, DefaultMode, AlwaysJIDs, NeverJIDs) ->
    M = prefs_module(LServer),
    M:set_prefs(LServer, LUser, DefaultMode, AlwaysJIDs, NeverJIDs).

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

remove_archive(LServer, LUser) ->
    wait_flushing(LServer),
    PM = prefs_module(LServer),
    AM = archive_module(LServer),
    UM = user_module(LServer),
    PM:remove_archive(LServer, LUser),
    AM:remove_archive(LServer, LUser),
    UM:remove_archive(LServer, LUser),
    ok.

message_row_to_xml({MessID,SrcJID,Packet}, QueryID) ->
    {Microseconds, _NodeId} = decode_compact_uuid(MessID),
    DateTime = calendar:now_to_universal_time(microseconds_to_now(Microseconds)),
    BExtMessID = mess_id_to_external_binary(MessID),
    wrap_message(Packet, QueryID, BExtMessID, DateTime, SrcJID).

message_row_to_ext_id({MessID,_,_}) ->
    mess_id_to_external_binary(MessID).

-spec lookup_messages(ArcJID, RSM, Start, End, Now, WithJID, PageSize,
                      LimitPassed, MaxResultLimit) ->
    {ok, {TotalCount, Offset, MessageRows}} | {error, 'policy-violation'}
    when
    ArcJID :: #jid{},
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
lookup_messages(ArcJID=#jid{lserver=LServer}, RSM, Start, End, Now,
                WithJID, PageSize, LimitPassed, MaxResultLimit) ->
    AM = archive_module(LServer),
    AM:lookup_messages(ArcJID, RSM, Start, End, Now, WithJID, PageSize,
                       LimitPassed, MaxResultLimit).

archive_message(Id, Dir,
                LocJID=#jid{lserver=LServer}, RemJID, SrcJID, Packet) ->
    M = writer_module(LServer),
    M:archive_message(Id, Dir, LocJID, RemJID, SrcJID, Packet).


-spec purge_single_message(ArcJID, MessID, Now) ->
    ok | {error, 'not-found'} when
    ArcJID :: #jid{},
    MessID :: message_id(),
    Now :: unix_timestamp().
purge_single_message(ArcJID=#jid{lserver=LServer}, MessID, Now) ->
    AM = archive_module(LServer),
    AM:purge_single_message(ArcJID, MessID, Now).

-spec purge_multiple_messages(ArcJID, Start, End, Now, WithJID) -> ok when
    ArcJID :: #jid{},
    Start   :: unix_timestamp() | undefined,
    End     :: unix_timestamp() | undefined,
    Now     :: unix_timestamp(),
    WithJID :: #jid{} | undefined.
purge_multiple_messages(ArcJID=#jid{lserver=LServer},
    Start, End, Now, WithJID) ->
    AM = archive_module(LServer),
    AM:purge_multiple_messages(ArcJID, Start, End, Now, WithJID).

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

