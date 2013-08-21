-module(mod_mam_muc).
-behavior(gen_mod).
-export([start/2, stop/1]).
%% ejabberd handlers
-export([filter_room_packet/4,
         room_process_mam_iq/3,
         forget_room/2]).

%% Client API
-export([delete_archive/2,
         delete_archive_after_destruction/3,
         enable_logging/3,
         enable_querying/3,
         create_room_archive/2]).


-import(mod_mam_utils,
        [maybe_microseconds/1,
         microseconds_to_now/1]).

%% MessID
-import(mod_mam_utils,
        [generate_message_id/0,
         encode_compact_uuid/2,
         decode_compact_uuid/1,
         is_function_exist/3,
         mess_id_to_external_binary/1,
         external_binary_to_mess_id/1]).

%% XML
-import(mod_mam_utils,
        [replace_archived_elem/3,
         get_one_of_path/2,
         wrap_message/5,
         result_set/4,
         result_query/1]).

%% Other
-import(mod_mam_utils,
        [maybe_integer/2]).

%% Ejabberd
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
-type escaped_nick_name() :: binary(). 
-type escaped_room_id() :: binary(). 

%% ----------------------------------------------------------------------
%% Other types
-type filter() :: iolist().
-type escaped_message_id() :: binary().

%% ----------------------------------------------------------------------
%% Constants

mam_ns_string() -> "urn:xmpp:mam:tmp".

mam_ns_binary() -> <<"urn:xmpp:mam:tmp">>.

default_result_limit() -> 50.

max_result_limit() -> 50.

should_delete_archive_after_destruction_by_default(_LServer) -> true.

%% ----------------------------------------------------------------------
%% gen_mod callbacks

start(Host, Opts) ->
    ?DEBUG("mod_mam_muc starting", []),
    start_supervisor(Host),
    IQDisc = gen_mod:get_opt(iqdisc, Opts, parallel), %% Type
    mod_disco:register_feature(Host, mam_ns_binary()),
    gen_iq_handler:add_iq_handler(mod_muc_iq, Host, mam_ns_binary(),
                                  ?MODULE, room_process_mam_iq, IQDisc),
    ejabberd_hooks:add(filter_room_packet, Host, ?MODULE,
                       filter_room_packet, 90),
    ejabberd_hooks:add(forget_room, Host, ?MODULE, forget_room, 90),
    [start_module(Host, M) || M <- required_modules(Host)],
    ok.

stop(Host) ->
    ?DEBUG("mod_mam stopping", []),
    stop_supervisor(Host),
    ejabberd_hooks:add(filter_room_packet, Host, ?MODULE,
                       filter_room_packet, 90),
    gen_iq_handler:remove_iq_handler(mod_muc_iq, Host, mam_ns_string()),
    mod_disco:unregister_feature(Host, mam_ns_binary()),
    [stop_module(Host, M) || M <- required_modules(Host)],
    ok.

%% ----------------------------------------------------------------------
%% OTP helpers

start_supervisor(_Host) ->
    CacheChildSpec =
    {mod_mam_muc_cache,
     {mod_mam_muc_cache, start_link, []},
     permanent,
     5000,
     worker,
     [mod_mam_muc_cache]},
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
%% API

%% @doc Delete all messages from the room.
delete_archive(LServer, RoomName) ->
    RoomId = mod_mam_muc_cache:room_id(LServer, RoomName),
    SRoomId = integer_to_list(RoomId),
    %% TODO: use transaction
    {selected, _ColumnNames, _MessageRows} =
    ejabberd_odbc:sql_query(LServer,
    ["DELETE FROM mam_muc_message WHERE room_id = \"", SRoomId, "\""]),
    {selected, _ColumnNames, _MessageRows} =
    ejabberd_odbc:sql_query(LServer,
    ["DELETE FROM mam_muc_room WHERE id = \"", SRoomId, "\""]),
    true.

%% @doc All messages will be deleted after the room is destroyed.
%% If `DeleteIt' is true, than messages will be lost.
%% If `DeleteIt' is false, than messages will be stored.
%% If `DeleteIt' is undefined, then the default behaviour will be chosen.
delete_archive_after_destruction(LServer, RoomName, DeleteIt) ->
    set_bool(LServer, RoomName, "delete_archive_after_destruction", DeleteIt),
    ok.

%% @doc Enable logging for the room.
enable_logging(LServer, RoomName, Enabled) ->
    set_bool(LServer, RoomName, "enable_logging", Enabled),
    mod_mam_muc_cache:update_logging_enabled(LServer, RoomName, Enabled),
    ok.

%% @doc Enable access to the archive for the room.
enable_querying(LServer, RoomName, Enabled) ->
    set_bool(LServer, RoomName, "enable_querying", Enabled),
    mod_mam_muc_cache:update_querying_enabled(LServer, RoomName, Enabled),
    ok.

create_room_archive(LServer, RoomName) ->
    SRoomName = ejabberd_odbc:escape(RoomName),
    ejabberd_odbc:sql_query(
      LServer,
      ["INSERT INTO mam_muc_room(room_name) "
       "VALUES ('", SRoomName,"')"]),
    ok.


%% ----------------------------------------------------------------------
%% hooks and handlers

%% This hook is called from `mod_muc:forget_room(Host, Name)'.
forget_room(LServer, RoomName) ->
    ShouldDelete =
    case query_delete_archive_after_destruction(LServer, RoomName) of
        undefined ->
            should_delete_archive_after_destruction_by_default(LServer);
        Bool ->
            Bool
    end,
    case ShouldDelete of
        true -> delete_archive(LServer, RoomName);
        false -> false
    end.

%% @doc Handle public MUC-message.
filter_room_packet(Packet, FromNick, FromJID,
                   RoomJID=#jid{lserver = LServer, luser = RoomName}) ->
    ?DEBUG("Incoming room packet.", []),
    case mod_mam_muc_cache:is_logging_enabled(LServer, RoomName) of
    false -> Packet;
    true ->
    Id = generate_message_id(),
    SrcJID = jlib:jid_replace_resource(RoomJID, FromNick),
    archive_message(Id, incoming, RoomJID, FromJID, SrcJID, Packet),
    BareRoomJID = jlib:jid_to_binary(RoomJID),
    replace_archived_elem(BareRoomJID, mess_id_to_external_binary(Id), Packet)
    end.

%% `process_mam_iq/3' is simular.
-spec room_process_mam_iq(From, To, IQ) -> IQ | ignore | error when
    From :: jid(),
    To :: jid(),
    IQ :: #iq{}.
room_process_mam_iq(From=#jid{luser = LUser},
                    To=#jid{lserver = LServer, luser = RoomName},
                    IQ=#iq{type = get,
                           sub_el = QueryEl = #xmlel{name = <<"query">>}}) ->
    case mod_mam_muc_cache:is_querying_enabled(LServer, RoomName) of
    false -> error;
    true ->
    ?DEBUG("Handle IQ query.", []),
    QueryID = xml:get_tag_attr_s(<<"queryid">>, QueryEl),
    Now   = mod_mam_utils:now_to_microseconds(now()),
    %% Filtering by date.
    %% Start :: integer() | undefined
    Start = elem_to_start_microseconds(QueryEl),
    End   = elem_to_end_microseconds(QueryEl),
    %% Filtering by contact.
    With  = elem_to_with_jid(QueryEl),
    RSM   = fix_rsm(jlib:rsm_decode(QueryEl)),
    %% This element's name is "limit".
    %% But it must be "max" according XEP-0313.
    Limit = get_one_of_path(QueryEl, [
                    [{elem, <<"set">>}, {elem, <<"max">>}, cdata],
                    [{elem, <<"set">>}, {elem, <<"limit">>}, cdata]
                   ]),
    PageSize = min(max_result_limit(),
                   maybe_integer(Limit, default_result_limit())),
    LimitPassed = Limit =/= <<>>,
    wait_flushing(LServer),
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
    end
    end;
room_process_mam_iq(_, _, _) ->
    ?DEBUG("Bad IQ.", []),
    error.

%% ----------------------------------------------------------------------
%% Internal functions

message_row_to_xml({MessID,SrcJID,Packet}, QueryID) ->
    {Microseconds, _NodeId} = decode_compact_uuid(MessID),
    DateTime = calendar:now_to_universal_time(microseconds_to_now(Microseconds)),
    BExtMessID = mess_id_to_external_binary(MessID),
    wrap_message(Packet, QueryID, BExtMessID, DateTime, SrcJID).

message_row_to_ext_id({MessID,_,_}) ->
    mess_id_to_external_binary(MessID).

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

maybe_jid(<<>>) ->
    undefined;
maybe_jid(JID) when is_binary(JID) ->
    jlib:binary_to_jid(JID).

%% ----------------------------------------------------------------------
%% Callbacks

required_modules(Host) ->
    [archive_module(Host),
     writer_module(Host)].

archive_module(Host) ->
    gen_mod:get_module_opt(Host, ?MODULE, archive_module, mod_mam_muc_odbc_arch).

writer_module(Host) ->
    gen_mod:get_module_opt(Host, ?MODULE, writer_module, mod_mam_muc_odbc_async_writer).


-spec lookup_messages(RoomJID, RSM, Start, End, Now, WithJID, PageSize,
                      LimitPassed, MaxResultLimit) ->
    {ok, {TotalCount, Offset, MessageRows}} | {error, 'policy-violation'}
    when
    RoomJID :: #jid{},
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
lookup_messages(RoomJID=#jid{lserver=LServer}, RSM, Start, End, Now,
                WithJID, PageSize, LimitPassed, MaxResultLimit) ->
    AM = archive_module(LServer),
    AM:lookup_messages(RoomJID, RSM, Start, End, Now, WithJID, PageSize,
                       LimitPassed, MaxResultLimit).

archive_message(Id, Dir,
                LocJID=#jid{lserver=LServer}, RemJID, SrcJID, Packet) ->
    M = writer_module(LServer),
    M:archive_message(Id, Dir, LocJID, RemJID, SrcJID, Packet).

wait_flushing(LServer) ->
    M = writer_module(LServer),
    M:wait_flushing(LServer).

%% ----------------------------------------------------------------------
%% SQL Internal functions

sql_bool(true)      -> "\"1\"";
sql_bool(false)     -> "\"0\"";
sql_bool(undefined) -> "null".

set_bool(LServer, RoomName, FieldName, FieldValue) ->
    RoomId = mod_mam_muc_cache:room_id(LServer, RoomName),
    SRoomId = integer_to_list(RoomId),
    ejabberd_odbc:sql_query(
      LServer,
      ["UPDATE mam_muc_room "
       "SET ", FieldName, " = ", sql_bool(FieldValue), " "
       "WHERE id = \"", SRoomId, "\""]).

query_delete_archive_after_destruction(LServer, RoomName) ->
    select_bool(LServer, RoomName, "delete_archive_after_destruction").

select_bool(LServer, RoomName, Field) ->
    SRoomName = ejabberd_odbc:escape(RoomName),
    Result =
    ejabberd_odbc:sql_query(
      LServer,
      ["SELECT " ++ Field ++ " "
       "FROM mam_muc_room "
       "WHERE room_name='", SRoomName, "' "
       "LIMIT 1"]),

    case Result of
        {selected, [Field], [{Res}]} ->
            case Res of
                null    -> undefined;
                <<"1">> -> true;
                <<"0">> -> false
            end;
        {selected, [Field], []} ->
            %% The room is not found
            create_room_archive(LServer, RoomName),
            undefined
    end.


%% ----------------------------------------------------------------------
%% Helpers


