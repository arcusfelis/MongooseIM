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
    ok.

stop(Host) ->
    ?DEBUG("mod_mam stopping", []),
    stop_supervisor(Host),
    ejabberd_hooks:add(filter_room_packet, Host, ?MODULE,
                       filter_room_packet, 90),
    gen_iq_handler:remove_iq_handler(mod_muc_iq, Host, mam_ns_string()),
    mod_disco:unregister_feature(Host, mam_ns_binary()),
    ok.

%% ----------------------------------------------------------------------
%% OTP helpers

start_supervisor(Host) ->
    WriterProc = mod_mam_muc_async_writer:srv_name(Host),
    WriterChildSpec =
    {WriterProc,
     {mod_mam_muc_async_writer, start_link, [WriterProc, Host]},
     permanent,
     5000,
     worker,
     [mod_mam_muc_async_writer]},
    CacheChildSpec =
    {mod_mam_muc_cache,
     {mod_mam_muc_cache, start_link, []},
     permanent,
     5000,
     worker,
     [mod_mam_muc_cache]},
    supervisor:start_child(ejabberd_sup, WriterChildSpec),
    supervisor:start_child(ejabberd_sup, CacheChildSpec).

stop_supervisor(Host) ->
    WriterProc = mod_mam_muc_async_writer:srv_name(Host),
    stop_child(WriterProc).

stop_child(Proc) ->
    supervisor:terminate_child(ejabberd_sup, Proc),
    supervisor:delete_child(ejabberd_sup, Proc).


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
filter_room_packet(Packet, FromNick, _FromJID,
                   To=#jid{lserver = LServer, luser = RoomName}) ->
    ?DEBUG("Incoming room packet.", []),
    case mod_mam_muc_cache:is_logging_enabled(LServer, RoomName) of
    false -> Packet;
    true ->
    Id = generate_message_id(),
    mod_mam_muc_async_writer:archive_message(LServer, RoomName, Id, FromNick, Packet),
    BareTo = jlib:jid_to_binary(To),
    replace_archived_elem(BareTo, mess_id_to_external_binary(Id), Packet)
    end.

%% `process_mam_iq/3' is simular.
-spec room_process_mam_iq(From, To, IQ) -> IQ | ignore | error when
    From :: jid(),
    To :: jid(),
    IQ :: #iq{}.
room_process_mam_iq(From=#jid{lserver = LServer},
                    To=#jid{luser = RoomName},
                    IQ=#iq{type = get,
                           sub_el = QueryEl = #xmlel{name = <<"query">>}}) ->
    case mod_mam_muc_cache:is_querying_enabled(LServer, RoomName) of
    false -> error;
    true ->
    ?DEBUG("Handle IQ query.", []),
    QueryID = xml:get_tag_attr_s(<<"queryid">>, QueryEl),
    %% Filtering by date.
    %% Start :: integer() | undefined
    Start = maybe_microseconds(xml:get_path_s(QueryEl, [{elem, <<"start">>}, cdata])),
    End   = maybe_microseconds(xml:get_path_s(QueryEl, [{elem, <<"end">>}, cdata])),
    RSM   = jlib:rsm_decode(QueryEl),
    %% Filtering by contact.
    With  = xml:get_path_s(QueryEl, [{elem, <<"with">>}, cdata]),
    SWithNick =
    case With of
        <<>> -> undefined;
        _    -> 
            #jid{lresource = WithLResource} = jlib:binary_to_jid(With),
            ejabberd_odbc:escape(WithLResource)
    end,
    %% This element's name is "limit".
    %% But it must be "max" according XEP-0313.
    Limit = get_one_of_path(QueryEl, [
                    [{elem, <<"set">>}, {elem, <<"max">>}, cdata],
                    [{elem, <<"set">>}, {elem, <<"limit">>}, cdata]
                   ]),
    PageSize = min(max_result_limit(),
                   maybe_integer(Limit, default_result_limit())),

    RoomId = mod_mam_muc_cache:room_id(LServer, RoomName),
    SRoomId = integer_to_list(RoomId),
    Filter = prepare_filter(SRoomId, Start, End, SWithNick),
    wait_flushing(LServer),
    TotalCount = calc_count(LServer, Filter),
    Offset     = calc_offset(LServer, Filter, PageSize, TotalCount, RSM),
    ?DEBUG("RSM info: ~nTotal count: ~p~nOffset: ~p~n",
              [TotalCount, Offset]),
    %% If a query returns a number of stanzas greater than this limit and the
    %% client did not specify a limit using RSM then the server should return
    %% a policy-violation error to the client. 
    case TotalCount - Offset > max_result_limit() andalso Limit =:= <<>> of
        true ->
        ErrorEl = ?STANZA_ERRORT(<<"">>, <<"modify">>, <<"policy-violation">>,
                                 <<"en">>, <<"Too many results">>),          
        IQ#iq{type = error, sub_el = [ErrorEl]};

        false ->
        MessageRows = extract_messages(LServer, Filter, Offset, PageSize),
        {FirstId, LastId} =
            case MessageRows of
                []    -> {undefined, undefined};
                [_|_] -> {message_row_to_ext_id(hd(MessageRows)),
                          message_row_to_ext_id(lists:last(MessageRows))}
            end,
        [send_message(To, From, message_row_to_xml(M, To, QueryID))
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

message_row_to_xml({BMessID,BNick,BPacket}, RoomJID, QueryID) ->
    MessID = list_to_integer(binary_to_list(BMessID)),
    {Microseconds, _NodeId} = decode_compact_uuid(MessID),
    Packet = binary_to_term(BPacket),
    DateTime = calendar:now_to_universal_time(microseconds_to_now(Microseconds)),
    FromJID = jlib:jid_replace_resource(RoomJID, BNick),
    BExtMessID = mess_id_to_external_binary(MessID),
    wrap_message(Packet, QueryID, BExtMessID, DateTime, FromJID).

message_row_to_ext_id({BMessID,_,_}) ->
    MessID = list_to_integer(binary_to_list(BMessID)),
    mess_id_to_external_binary(MessID).


%% ----------------------------------------------------------------------
%% SQL Internal functions

%% #rsm_in{
%%    max = non_neg_integer() | undefined,
%%    direction = before | aft | undefined,
%%    id = binary() | undefined,
%%    index = non_neg_integer() | undefined}
-spec calc_offset(LServer, Filter, PageSize, TotalCount, RSM) -> Offset
    when
    LServer  :: server_hostname(),
    Filter   :: filter(),
    PageSize :: non_neg_integer(),
    TotalCount :: non_neg_integer(),
    RSM      :: #rsm_in{},
    Offset   :: non_neg_integer().
calc_offset(_LS, _F, _PS, _TC, #rsm_in{direction = undefined, index = Index})
    when is_integer(Index) ->
    Index;
%% Requesting the Last Page in a Result Set
calc_offset(_LS, _F, PS, TC, #rsm_in{direction = before, id = <<>>}) ->
    max(0, TC - PS);
calc_offset(LServer, Filter, PageSize, _TC, #rsm_in{direction = before, id = ID})
    when is_binary(ID) ->
    SID = ext_to_sec_mess_id_bin(ID),
    max(0, calc_before(LServer, Filter, SID) - PageSize);
calc_offset(LServer, Filter, _PS, _TC, #rsm_in{direction = aft, id = ID})
    when is_binary(ID), byte_size(ID) > 0 ->
    SID = ext_to_sec_mess_id_bin(ID),
    calc_index(LServer, Filter, SID);
calc_offset(_LS, _F, _PS, _TC, _RSM) ->
    0.

ext_to_sec_mess_id_bin(BExtMessID) ->
    integer_to_list(external_binary_to_mess_id(BExtMessID)).

%% Zero-based index of the row with MessID in the result test.
%% If the element does not exists, the ID of the next element will
%% be returned instead.
%% "SELECT COUNT(*) as "index" FROM mam_muc_message WHERE id <= '",  MessID
-spec calc_index(LServer, Filter, SMessID) -> Count
    when
    LServer  :: server_hostname(),
    Filter   :: filter(),
    SMessID     :: escaped_message_id(),
    Count    :: non_neg_integer().
calc_index(LServer, Filter, SMessID) ->
    {selected, _ColumnNames, [{BIndex}]} =
    ejabberd_odbc:sql_query(
      LServer,
      ["SELECT COUNT(*) FROM mam_muc_message ", Filter, " AND id <= '", SMessID, "'"]),
    list_to_integer(binary_to_list(BIndex)).

%% @doc Count of elements in RSet before the passed element.
%% The element with the passed MessID can be already deleted.
%% "SELECT COUNT(*) as "count" FROM mam_muc_message WHERE id < '",  MessID
-spec calc_before(LServer, Filter, SMessID) -> Count
    when
    LServer  :: server_hostname(),
    Filter   :: filter(),
    SMessID     :: escaped_message_id(),
    Count    :: non_neg_integer().
calc_before(LServer, Filter, SMessID) ->
    {selected, _ColumnNames, [{BIndex}]} =
    ejabberd_odbc:sql_query(
      LServer,
      ["SELECT COUNT(*) FROM mam_muc_message ", Filter, " AND id < '", SMessID, "'"]),
    list_to_integer(binary_to_list(BIndex)).


%% @doc Get the total result set size.
%% "SELECT COUNT(*) as "count" FROM mam_muc_message WHERE "
-spec calc_count(LServer, Filter) -> Count
    when
    LServer  :: server_hostname(),
    Filter   :: filter(),
    Count    :: non_neg_integer().
calc_count(LServer, Filter) ->
    {selected, _ColumnNames, [{BCount}]} =
    ejabberd_odbc:sql_query(
      LServer,
      ["SELECT COUNT(*) FROM mam_muc_message ", Filter]),
    list_to_integer(binary_to_list(BCount)).

-spec prepare_filter(SRoomId, IStart, IEnd, SWithNick) -> filter()
    when
    SRoomId   :: escaped_room_id(),
    IStart    :: unix_timestamp() | undefined,
    IEnd      :: unix_timestamp() | undefined,
    SWithNick :: escaped_nick_name().
prepare_filter(SRoomId, IStart, IEnd, SWithNick) ->
   ["WHERE room_id='", SRoomId, "'",
     case IStart of
        undefined -> "";
        _         -> [" AND id >= ",
                      integer_to_list(encode_compact_uuid(IStart, 0))]
     end,
     case IEnd of
        undefined -> "";
        _         -> [" AND id <= ",
                      integer_to_list(encode_compact_uuid(IEnd, 255))]
     end,
     case SWithNick of
        undefined -> "";
        _         -> [" AND nick_name = '", SWithNick, "'"]
     end].

%% Columns are `["id","nick_name","message"]'.
-spec extract_messages(LServer, Filter, IOffset, IMax) ->
    [Record] when
    LServer :: server_hostname(),
    Filter  :: filter(),
    IOffset :: non_neg_integer(),
    IMax    :: pos_integer(),
    Record :: tuple().
extract_messages(_LServer, _Filter, _IOffset, 0) ->
    [];
extract_messages(LServer, Filter, IOffset, IMax) ->
    {selected, _ColumnNames, MessageRows} =
    ejabberd_odbc:sql_query(
      LServer,
      ["SELECT id, nick_name, message "
       "FROM mam_muc_message ",
        Filter,
       " ORDER BY id"
       " LIMIT ",
         case IOffset of
             0 -> "";
             _ -> [integer_to_list(IOffset), ", "]
         end,
         integer_to_list(IMax)]),
    ?DEBUG("extract_messages query returns ~p", [MessageRows]),
    MessageRows.

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

wait_flushing(_LServer) ->
    timer:sleep(1000).

