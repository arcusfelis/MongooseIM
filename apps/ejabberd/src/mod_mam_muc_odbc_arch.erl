%%%-------------------------------------------------------------------
%%% @author Uvarov Michael <arcusfelis@gmail.com>
%%% @copyright (C) 2013, Uvarov Michael
%%% @doc A backend for storing messages from MUC rooms using ODBC.
%%% @end
%%%-------------------------------------------------------------------
-module(mod_mam_muc_odbc_arch).
-export([archive_size/2,
         lookup_messages/9,
         remove_user_from_db/2,
         purge_single_message/3,
         purge_multiple_messages/5]).

%% UID
-import(mod_mam_utils,
        [encode_compact_uuid/2]).

-include_lib("ejabberd/include/ejabberd.hrl").
-include_lib("ejabberd/include/jlib.hrl").
-include_lib("exml/include/exml.hrl").

-type filter() :: iolist().
-type message_id() :: non_neg_integer().
-type escaped_message_id() :: binary().
-type escaped_jid() :: binary().
-type server_hostname() :: binary().
-type unix_timestamp() :: non_neg_integer().

archive_size(LServer, LRoom) ->
    RoomID = mod_mam_muc_cache:room_id(LServer, LRoom),
    {selected, _ColumnNames, [{BSize}]} =
    ejabberd_odbc:sql_query(
      LServer,
      ["SELECT COUNT(*) "
       "FROM mam_muc_message "
       "WHERE room_id = '", escape_room_id(RoomID), "'"]),
    list_to_integer(binary_to_list(BSize)).


-spec lookup_messages(RoomJID, RSM, Start, End, Now, WithJID, PageSize,
                      LimitPassed, MaxResultLimit) ->
    {ok, {TotalCount, Offset, MessageRows}} | {error, 'policy-violation'}
			     when
    RoomJID :: #jid{},
    RSM     :: #rsm_in{} | undefined,
    Start   :: unix_timestamp() | undefined,
    End     :: unix_timestamp() | undefined,
    Now     :: unix_timestamp(),
    PageSize :: non_neg_integer(),
    WithJID :: #jid{} | undefined,
    LimitPassed :: boolean(),
    MaxResultLimit :: non_neg_integer(),
    TotalCount :: non_neg_integer(),
    Offset  :: non_neg_integer(),
    MessageRows :: list(tuple()).
lookup_messages(RoomJID=#jid{lserver=LServer},
                RSM, Start, End, _Now, WithJID,
                PageSize, LimitPassed, MaxResultLimit) ->
    Filter = prepare_filter(RoomJID, Start, End, WithJID),
    TotalCount = calc_count(LServer, Filter),
    Offset     = calc_offset(LServer, Filter, PageSize, TotalCount, RSM),
    %% If a query returns a number of stanzas greater than this limit and the
    %% client did not specify a limit using RSM then the server should return
    %% a policy-violation error to the client. 
    case TotalCount - Offset > MaxResultLimit andalso not LimitPassed of
        true ->
            {error, 'policy-violation'};

        false ->
            MessageRows = extract_messages(LServer, Filter, Offset, PageSize),
            {ok, {TotalCount, Offset, rows_to_uniform_format(MessageRows, RoomJID)}}
    end.


rows_to_uniform_format(MessageRows, RoomJID) ->
    [row_to_uniform_format(Row, RoomJID) || Row <- MessageRows].

row_to_uniform_format({BMessID,BNick,BPacket}, RoomJID) ->
    MessID = list_to_integer(binary_to_list(BMessID)),
    SrcJID = jlib:jid_replace_resource(RoomJID, BNick),
    Packet = binary_to_term(BPacket),
    {MessID, SrcJID, Packet}.


remove_user_from_db(LServer, LRoom) ->
    RoomID = mod_mam_muc_cache:room_id(LServer, LRoom),
    {updated, _} =
    ejabberd_odbc:sql_query(
      LServer,
      ["DELETE FROM mam_muc_message "
       "WHERE room_id = '", escape_room_id(RoomID), "'"]),
    ok.

-spec purge_single_message(RoomJID, MessID, Now) ->
    ok | {error, 'not-allowed' | 'not-found'} when
    RoomJID :: #jid{},
    MessID :: message_id(),
    Now :: unix_timestamp().
purge_single_message(#jid{lserver = LServer, luser = LRoom}, MessID, _Now) ->
    RoomID = mod_mam_muc_cache:room_id(LServer, LRoom),
    Result =
    ejabberd_odbc:sql_query(
      LServer,
      ["DELETE FROM mam_muc_message "
       "WHERE room_id = '", escape_room_id(RoomID), "' "
       "AND id = '", escape_message_id(MessID), "'"]),
    case Result of
        {updated, 0} -> {error, 'not-found'};
        {updated, 1} -> ok
    end.

-spec purge_multiple_messages(RoomJID, Start, End, Now, WithJID) ->
    ok | {error, 'not-allowed'} when
    RoomJID :: #jid{},
    Start   :: unix_timestamp() | undefined,
    End     :: unix_timestamp() | undefined,
    Now     :: unix_timestamp(),
    WithJID :: #jid{} | undefined.
purge_multiple_messages(RoomJID = #jid{lserver=LServer}, Start, End, _Now, WithJID) ->
    Filter = prepare_filter(RoomJID, Start, End, WithJID),
    {updated, _} =
    ejabberd_odbc:sql_query(
      LServer,
      ["DELETE FROM mam_muc_message ", Filter]),
    ok.

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


%% Zero-based index of the row with UID in the result test.
%% If the element does not exists, the ID of the next element will
%% be returned instead.
%% "SELECT COUNT(*) as "index" FROM mam_muc_message WHERE id <= '",  UID
-spec calc_index(LServer, Filter, SUID) -> Count
    when
    LServer  :: server_hostname(),
    Filter   :: filter(),
    SUID     :: escaped_message_id(),
    Count    :: non_neg_integer().
calc_index(LServer, Filter, SUID) ->
    {selected, _ColumnNames, [{BIndex}]} =
    ejabberd_odbc:sql_query(
      LServer,
      ["SELECT COUNT(*) FROM mam_muc_message ", Filter, " AND id <= '", SUID, "'"]),
    list_to_integer(binary_to_list(BIndex)).

%% @doc Count of elements in RSet before the passed element.
%% The element with the passed UID can be already deleted.
%% @end
%% "SELECT COUNT(*) as "count" FROM mam_muc_message WHERE id < '",  UID
-spec calc_before(LServer, Filter, SUID) -> Count
    when
    LServer  :: server_hostname(),
    Filter   :: filter(),
    SUID     :: escaped_message_id(),
    Count    :: non_neg_integer().
calc_before(LServer, Filter, SUID) ->
    {selected, _ColumnNames, [{BIndex}]} =
    ejabberd_odbc:sql_query(
      LServer,
      ["SELECT COUNT(*) FROM mam_muc_message ", Filter, " AND id < '", SUID, "'"]),
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


%% prepare_filter/4
-spec prepare_filter(RoomJID, Start, End, WithJID) -> filter()
    when
    RoomJID :: #jid{},
    Start   :: unix_timestamp() | undefined,
    End     :: unix_timestamp() | undefined,
    WithJID :: #jid{} | undefined.
prepare_filter(#jid{lserver=LServer, luser=RoomName}, Start, End, WithJID) ->
    RoomID = mod_mam_muc_cache:room_id(LServer, RoomName),
    SWithNick = maybe_jid_to_escaped_resource(WithJID),
    prepare_filter_1(RoomID, Start, End, SWithNick).

-spec prepare_filter_1(RoomID, IStart, IEnd, SWithNick) -> filter()
    when
    RoomID  :: non_neg_integer(),
    IStart  :: unix_timestamp() | undefined,
    IEnd    :: unix_timestamp() | undefined,
    SWithNick :: escaped_jid() | undefined.
prepare_filter_1(RoomID, IStart, IEnd, SWithNick) ->
   ["WHERE room_id='", escape_room_id(RoomID), "'",
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
    RSM      :: #rsm_in{} | undefined,
    Offset   :: non_neg_integer().
calc_offset(_LS, _F, _PS, _TC, #rsm_in{direction = undefined, index = Index})
    when is_integer(Index) ->
    Index;
%% Requesting the Last Page in a Result Set
calc_offset(_LS, _F, PS, TC, #rsm_in{direction = before, id = undefined}) ->
    max(0, TC - PS);
calc_offset(LServer, F, PS, _TC, #rsm_in{direction = before, id = ID})
    when is_integer(ID) ->
    SID = escape_message_id(ID),
    max(0, calc_before(LServer, F, SID) - PS);
calc_offset(LServer, F, _PS, _TC, #rsm_in{direction = aft, id = ID})
    when is_integer(ID) ->
    SID = escape_message_id(ID),
    calc_index(LServer, F, SID);
calc_offset(_LS, _F, _PS, _TC, _RSM) ->
    0.

escape_message_id(MessID) when is_integer(MessID) ->
    integer_to_list(MessID).

escape_room_id(RoomID) when is_integer(RoomID) ->
    integer_to_list(RoomID).

maybe_jid_to_escaped_resource(undefined) ->
    undefined;
maybe_jid_to_escaped_resource(#jid{lresource = <<>>}) ->
    undefined;
maybe_jid_to_escaped_resource(#jid{lresource = WithLResource}) ->
    ejabberd_odbc:escape(WithLResource).
