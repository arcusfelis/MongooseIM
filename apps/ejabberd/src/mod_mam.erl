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
         encode_compact_uuid/2,
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
         is_function_exist/3]).

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
-type escaped_jid() :: binary().
-type literal_jid() :: binary().
-type escaped_resource() :: binary().
-type elem() :: #xmlel{}.


%% ----------------------------------------------------------------------
%% Other types
-type filter() :: iolist().
-type escaped_message_id() :: binary().
-type archive_behaviour() :: atom(). % roster | always | never.

%% ----------------------------------------------------------------------
%% Constants

mam_ns_string() -> "urn:xmpp:mam:tmp".

mam_ns_binary() -> <<"urn:xmpp:mam:tmp">>.

default_result_limit() -> 50.

max_result_limit() -> 50.

%% ----------------------------------------------------------------------
%% API

delete_archive(User, Server) ->
    LUser = jlib:nodeprep(User),
    LServer = jlib:nameprep(Server),
    ?DEBUG("Remove user ~p from ~p.", [LUser, LServer]),
    remove_user_from_db(LServer, LUser),
    ok.

archive_size(User, Server) ->
    LUser = jlib:nodeprep(User),
    LServer = jlib:nameprep(Server),
    UserID = mod_mam_cache:user_id(LServer, LUser),
    calc_archive_size(LServer, UserID).

%% ----------------------------------------------------------------------
%% gen_mod callbacks

start(Host, Opts) ->
    ?DEBUG("mod_mam starting", []),
    start_supervisor(Host),
    %% Only parallel is recommended hare.
    IQDisc = gen_mod:get_opt(iqdisc, Opts, parallel), %% Type
    mod_disco:register_feature(Host, mam_ns_binary()),
    gen_iq_handler:add_iq_handler(ejabberd_sm, Host, mam_ns_binary(),
                                  ?MODULE, process_mam_iq, IQDisc),
    ejabberd_hooks:add(user_send_packet, Host, ?MODULE, user_send_packet, 90),
    ejabberd_hooks:add(filter_packet, global, ?MODULE, filter_packet, 90),
    ejabberd_hooks:add(remove_user, Host, ?MODULE, remove_user, 50),
    ejabberd_hooks:add(remove_user, Host, mod_mam_cache, remove_user, 50),
    ejabberd_users:start(Host),
    PF = prefs_module(Host),
    case is_function_exist(PF, start, 1) of
        true  -> PF:start(Host);
        false -> ok
    end,
    ok.

stop(Host) ->
    ?DEBUG("mod_mam stopping", []),
    stop_supervisor(Host),
    ejabberd_hooks:delete(user_send_packet, Host, ?MODULE, user_send_packet, 90),
    ejabberd_hooks:delete(filter_packet, global, ?MODULE, filter_packet, 90),
    ejabberd_hooks:delete(remove_user, Host, ?MODULE, remove_user, 50),
    gen_iq_handler:remove_iq_handler(ejabberd_sm, Host, mam_ns_string()),
    mod_disco:unregister_feature(Host, mam_ns_binary()),
    PF = prefs_module(Host),
    case is_function_exist(PF, stop, 1) of
        true  -> PF:stop(Host);
        false -> ok
    end,
    ok.

%% ----------------------------------------------------------------------
%% OTP helpers


start_supervisor(Host) ->
    WriterProc = mod_mam_async_writer:srv_name(Host),
    WriterChildSpec =
    {WriterProc,
     {mod_mam_async_writer, start_link, [WriterProc, Host]},
     permanent,
     5000,
     worker,
     [mod_mam_async_writer]},
    CacheChildSpec =
    {mod_mam_cache,
     {mod_mam_cache, start_link, []},
     permanent,
     5000,
     worker,
     [mod_mam_cache]},
    supervisor:start_child(ejabberd_sup, CacheChildSpec),
    supervisor:start_child(ejabberd_sup, WriterChildSpec).

stop_supervisor(Host) ->
    Proc = mod_mam_async_writer:srv_name(Host),
    supervisor:terminate_child(ejabberd_sup, Proc),
    supervisor:delete_child(ejabberd_sup, Proc).


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
    Filter = prepare_filter(From, Start, End, With),
    TotalCount = calc_count(LServer, Filter),
    Offset     = calc_offset(LServer, Filter, PageSize, TotalCount, RSM),
    ?DEBUG("RSM info: ~nTotal count: ~p~nOffset: ~p~n",
              [TotalCount, Offset]),

    %% If a query returns a number of stanzas greater than this limit and the
    %% client did not specify a limit using RSM then the server should return
    %% a policy-violation error to the client. 
    case TotalCount - Offset > max_result_limit() andalso Limit =:= <<>> of
        true ->
        ?DEBUG("Policy violation by ~p.", [LUser]),
        ErrorEl = ?STANZA_ERRORT(<<"">>, <<"modify">>, <<"policy-violation">>,
                                 <<"en">>, <<"Too many results">>),          
        IQ#iq{type = error, sub_el = [ErrorEl]};

        false ->
        MessageRows = extract_messages(LServer, Filter, Offset, PageSize),
        {FirstId, LastId} =
            case MessageRows of
                []    -> {undefined, undefined};
                [_|_] -> {message_row_to_id(hd(MessageRows)),
                          message_row_to_id(lists:last(MessageRows))}
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
                ?DEBUG("Archived ~p", [Id]),
                BareTo = jlib:jid_to_binary(jlib:jid_remove_resource(To)),
                replace_archived_elem(BareTo, Id, Packet)
        end
    end,
    {From, To, Packet2}.


remove_user(User, Server) ->
    delete_archive(User, Server).

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
            mod_mam_async_writer:archive_message(Id, Dir, LocJID, RemJID, SrcJID, Packet),
            case ReturnId of
                true  -> integer_to_binary(Id);
                false -> undefined
            end;
            false -> undefined
        end;
        false -> undefined
    end.

prefs_module(Host) ->
    gen_mod:get_module_opt(Host, ?MODULE, prefs_module, mod_mam_odbc_prefs).

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
    M = prefs_module(LServer),
    M:remove_user_from_db(LServer, LUser),
    SUser = ejabberd_odbc:escape(LUser),
    ejabberd_odbc:sql_query(
      LServer,
      ["DELETE "
       "FROM mam_user "
       "WHERE user_name='", SUser, "'"]),
    ok.


calc_archive_size(LServer, UserID) ->
    {selected, _ColumnNames, [{BSize}]} =
    ejabberd_odbc:sql_query(
      LServer,
      ["SELECT COUNT(*) "
       "FROM mam_message "
       "WHERE user_id = '", integer_to_list(UserID), "'"]),
    list_to_integer(binary_to_list(BSize)).

message_row_to_xml({BUID,BSrcJID,BPacket}, QueryID) ->
    UID = list_to_integer(binary_to_list(BUID)),
    {Microseconds, _NodeId} = decode_compact_uuid(UID),
    Packet = binary_to_term(BPacket),
    SrcJID = jlib:binary_to_jid(BSrcJID),
    DateTime = calendar:now_to_universal_time(microseconds_to_now(Microseconds)),
    wrap_message(Packet, QueryID, BUID, DateTime, SrcJID).

message_row_to_id({BUID,_,_}) ->
    BUID.

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
    SID = ejabberd_odbc:escape(ID),
    max(0, calc_before(LServer, Filter, SID) - PageSize);
calc_offset(LServer, Filter, _PS, _TC, #rsm_in{direction = aft, id = ID})
    when is_binary(ID), byte_size(ID) > 0 ->
    SID = ejabberd_odbc:escape(ID),
    calc_index(LServer, Filter, SID);
calc_offset(_LS, _F, _PS, _TC, _RSM) ->
    0.

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


prepare_filter(#jid{lserver=LServer, luser=LUser}, Start, End, With) ->
    UserID = mod_mam_cache:user_id(LServer, LUser),
    {SWithJID, SWithResource} =
    case With of
        <<>> -> {undefined, undefined};
        _    ->
            WithJID = #jid{lresource = WithLResource} = jlib:binary_to_jid(With),
            WithBareJID = jlib:jid_remove_resource(WithJID),
            {ejabberd_odbc:escape(jlib:jid_to_binary(WithBareJID)),
             case WithLResource of <<>> -> undefined;
                  _ -> ejabberd_odbc:escape(WithLResource) end}
    end,
    prepare_filter(UserID, Start, End, SWithJID, SWithResource).

-spec prepare_filter(UserID, IStart, IEnd, SWithJID, SWithResource) -> filter()
    when
    UserID  :: non_neg_integer(),
    IStart  :: unix_timestamp() | undefined,
    IEnd    :: unix_timestamp() | undefined,
    SWithJID :: escaped_jid(),
    SWithResource :: escaped_resource().
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


%% ----------------------------------------------------------------------
%% Helpers


%% TODO: it is too simple, can add a separate process per node,
%% that will wait that all nodes flush.
%% Also, this process can monitor DB and protect against overloads.
wait_flushing(_LServer) ->
    timer:sleep(500).

