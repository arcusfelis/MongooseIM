-module(mod_mam_odbc_arch).
-export([archive_size/2,
         lookup_messages/8,
         remove_user_from_db/2]).

%% UID
-import(mod_mam_utils,
        [encode_compact_uuid/2,
         external_binary_to_mess_id/1]).

-include_lib("ejabberd/include/ejabberd.hrl").
-include_lib("ejabberd/include/jlib.hrl").
-include_lib("exml/include/exml.hrl").

-type filter() :: iolist().
-type escaped_message_id() :: binary().
-type escaped_jid() :: binary().
-type escaped_resource() :: binary().
-type server_hostname() :: binary().
-type unix_timestamp() :: non_neg_integer().

archive_size(LServer, LUser) ->
    UserID = mod_mam_cache:user_id(LServer, LUser),
    {selected, _ColumnNames, [{BSize}]} =
    ejabberd_odbc:sql_query(
      LServer,
      ["SELECT COUNT(*) "
       "FROM mam_message "
       "WHERE user_id = '", integer_to_list(UserID), "'"]),
    list_to_integer(binary_to_list(BSize)).


-spec lookup_messages(UserJID, RSM, Start, End, WithJID, PageSize,
                      LimitPassed, MaxResultLimit) ->
    {ok, {TotalCount, Offset, MessageRows}} | {error, 'policy-violation'}
			     when
    UserJID :: #jid{},
    RSM     :: #rsm_in{} | none,
    Start   :: unix_timestamp() | undefined,
    End     :: unix_timestamp() | undefined,
    PageSize :: non_neg_integer(),
    WithJID :: #jid{} | undefined,
    LimitPassed :: boolean(),
    MaxResultLimit :: non_neg_integer(),
    TotalCount :: non_neg_integer(),
    Offset  :: non_neg_integer(),
    MessageRows :: list(tuple()).
lookup_messages(UserJID=#jid{lserver=LServer},
                RSM, Start, End, WithJID,
                PageSize, LimitPassed, MaxResultLimit) ->
    Filter = prepare_filter(UserJID, Start, End, WithJID),
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
            {ok, {TotalCount, Offset, rows_to_uniform_format(MessageRows)}}
    end.


rows_to_uniform_format(MessageRows) ->
    [row_to_uniform_format(Row) || Row <- MessageRows].

row_to_uniform_format({BMessID,BSrcJID,BPacket}) ->
    MessID = list_to_integer(binary_to_list(BMessID)),
    SrcJID = jlib:binary_to_jid(BSrcJID),
    Packet = binary_to_term(BPacket),
    {MessID, SrcJID, Packet}.


remove_user_from_db(LServer, LUser) ->
    UserID = mod_mam_cache:user_id(LServer, LUser),
    {updated, _} =
    ejabberd_odbc:sql_query(
      LServer,
      ["DELETE FROM mam_message "
       "WHERE user_id = '", integer_to_list(UserID), "'"]),
    ok.


%% Each record is a tuple of form 
%% `{<<"13663125233">>,<<"bob@localhost">>,<<"res1">>,<<binary>>}'.
%% Columns are `["id","from_jid","message"]'.
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
      ["SELECT id, from_jid, message "
       "FROM mam_message ",
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
%% "SELECT COUNT(*) as "index" FROM mam_message WHERE id <= '",  UID
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
      ["SELECT COUNT(*) FROM mam_message ", Filter, " AND id <= '", SUID, "'"]),
    list_to_integer(binary_to_list(BIndex)).

%% @doc Count of elements in RSet before the passed element.
%% The element with the passed UID can be already deleted.
%% "SELECT COUNT(*) as "count" FROM mam_message WHERE id < '",  UID
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
      ["SELECT COUNT(*) FROM mam_message ", Filter, " AND id < '", SUID, "'"]),
    list_to_integer(binary_to_list(BIndex)).


%% @doc Get the total result set size.
%% "SELECT COUNT(*) as "count" FROM mam_message WHERE "
-spec calc_count(LServer, Filter) -> Count
    when
    LServer  :: server_hostname(),
    Filter   :: filter(),
    Count    :: non_neg_integer().
calc_count(LServer, Filter) ->
    {selected, _ColumnNames, [{BCount}]} =
    ejabberd_odbc:sql_query(
      LServer,
      ["SELECT COUNT(*) FROM mam_message ", Filter]),
    list_to_integer(binary_to_list(BCount)).


%% prepare_filter/4
-spec prepare_filter(UserJID, Start, End, WithJID) -> filter()
    when
    UserJID :: #jid{},
    Start   :: unix_timestamp() | undefined,
    End     :: unix_timestamp() | undefined,
    WithJID :: #jid{} | undefined.
prepare_filter(#jid{lserver=LServer, luser=LUser}, Start, End, WithJID) ->
    UserID = mod_mam_cache:user_id(LServer, LUser),
    {SWithJID, SWithResource} =
    case WithJID of
        undefined -> {undefined, undefined};
        #jid{lresource = <<>>} ->
            {secure_escaped_jid(WithJID), undefined};
        #jid{lresource = WithLResource} ->
            WithBareJID = jlib:jid_remove_resource(WithJID),
            {secure_escaped_jid(WithBareJID),
             ejabberd_odbc:escape(WithLResource)}
    end,
    prepare_filter(UserID, Start, End, SWithJID, SWithResource).

%% prepare_filter/5
-spec prepare_filter(UserID, IStart, IEnd, SWithJID, SWithResource) -> filter()
    when
    UserID  :: non_neg_integer(),
    IStart  :: unix_timestamp() | undefined,
    IEnd    :: unix_timestamp() | undefined,
    SWithJID :: escaped_jid() | undefined,
    SWithResource :: escaped_resource() | undefined.
prepare_filter(UserID, IStart, IEnd, SWithJID, SWithResource) ->
   ["WHERE user_id='", integer_to_list(UserID), "'",
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
     case SWithJID of
        undefined -> "";
        _         -> [" AND remote_bare_jid = '", SWithJID, "'"]
     end,
     case SWithResource of
        undefined -> "";
        _         -> [" AND remote_resource = '", SWithResource, "'"]
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
    RSM      :: #rsm_in{} | none,
    Offset   :: non_neg_integer().
calc_offset(_LS, _F, _PS, _TC, #rsm_in{direction = undefined, index = Index})
    when is_integer(Index) ->
    Index;
%% Requesting the Last Page in a Result Set
calc_offset(_LS, _F, PS, TC, #rsm_in{direction = before, id = <<>>}) ->
    max(0, TC - PS);
calc_offset(LServer, F, PS, _TC, #rsm_in{direction = before, id = ID})
    when is_binary(ID) ->
    SID = ext_to_sec_mess_id_bin(ID),
    max(0, calc_before(LServer, F, SID) - PS);
calc_offset(LServer, F, _PS, _TC, #rsm_in{direction = aft, id = ID})
    when is_binary(ID), byte_size(ID) > 0 ->
    SID = ext_to_sec_mess_id_bin(ID),
    calc_index(LServer, F, SID);
calc_offset(_LS, _F, _PS, _TC, _RSM) ->
    0.

ext_to_sec_mess_id_bin(BExtMessID) ->
    integer_to_list(external_binary_to_mess_id(BExtMessID)).


secure_escaped_jid(JID) ->
    ejabberd_odbc:escape(jlib:binary_to_jid(JID)).
