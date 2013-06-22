-module(mod_mam).
-behavior(gen_mod).
-export([start/2, stop/1]).
%% ejabberd handlers
-export([process_mam_iq/3,
         on_send_packet/3,
         on_remove_user/2,
         filter_packet/1]).

-include_lib("ejabberd/include/ejabberd.hrl").
-include_lib("ejabberd/include/jlib.hrl").
-include_lib("exml/include/exml.hrl").

%% ----------------------------------------------------------------------
%% Datetime types
-type iso8601_datetime_binary() :: binary().
%% Seconds from 01.01.1970
-type unix_timestamp() :: non_neg_integer().

%% ----------------------------------------------------------------------
%% XMPP types
-type server_hostname() :: binary().
-type literal_username() :: binary().
-type escaped_username() :: binary().
-type escaped_jid() :: binary().
-type literal_jid() :: binary().
-type escaped_resource() :: binary().
-type elem() :: #xmlel{}.


%% ----------------------------------------------------------------------
%% Other types
-type filter() :: iolist().
-type escaped_message_id() :: binary().
-type archive_behaviour_bin() :: binary(). % <<"roster">> | <<"always">> | <<"never">>.

%% ----------------------------------------------------------------------
%% Constants

mam_ns_string() -> "urn:xmpp:mam:tmp".

mam_ns_binary() -> <<"urn:xmpp:mam:tmp">>.

rsm_ns_binary() -> <<"http://jabber.org/protocol/rsm">>.

default_result_limit() -> 50.

max_result_limit() -> 50.

encode_direction(incoming) -> "I";
encode_direction(outgoing) -> "O".

encode_behaviour(<<"roster">>) -> "R";
encode_behaviour(<<"always">>) -> "A";
encode_behaviour(<<"never">>)  -> "N".

decode_behaviour(<<"R">>) -> <<"roster">>;
decode_behaviour(<<"A">>) -> <<"always">>;
decode_behaviour(<<"N">>) -> <<"never">>.

%% ----------------------------------------------------------------------
%% gen_mod callbacks

start(Host, Opts) ->
    ?DEBUG("mod_mam starting", []),
    IQDisc = gen_mod:get_opt(iqdisc, Opts, one_queue), %% Type
    mod_disco:register_feature(Host, mam_ns_binary()),
    gen_iq_handler:add_iq_handler(ejabberd_sm, Host, mam_ns_binary(),
                                  ?MODULE, process_mam_iq, IQDisc),
    ejabberd_hooks:add(user_send_packet, Host, ?MODULE, on_send_packet, 90),
    ejabberd_hooks:add(filter_packet, global, ?MODULE, filter_packet, 90),
    ejabberd_hooks:add(remove_user, Host, ?MODULE, on_remove_user, 50),
    ok.

stop(Host) ->
    ?DEBUG("mod_mam stopping", []),
    ejabberd_hooks:delete(user_send_packet, Host, ?MODULE, on_send_packet, 90),
    ejabberd_hooks:delete(user_receive_packet, Host, ?MODULE, on_receive_packet, 90),
    ejabberd_hooks:delete(filter_packet, global, ?MODULE, filter_packet, 90),
    ejabberd_hooks:delete(remove_user, Host, ?MODULE, on_remove_user, 50),
    gen_iq_handler:remove_iq_handler(ejabberd_sm, Host, mam_ns_string()),
    mod_disco:unregister_feature(Host, mam_ns_binary()),
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
    {DefaultMode, AlwaysJIDs, NewerJIDs} = parse_prefs(PrefsEl),
    ?DEBUG("Parsed data~n\tDefaultMode ~p~n\tAlwaysJIDs ~p~n\tNewerJIDS ~p~n",
              [DefaultMode, AlwaysJIDs, NewerJIDs]),
    update_settings(LServer, LUser, DefaultMode, AlwaysJIDs, NewerJIDs),
    ResultPrefsEl = result_prefs(DefaultMode, AlwaysJIDs, NewerJIDs),
    IQ#iq{type = result, sub_el = [ResultPrefsEl]};

process_mam_iq(From=#jid{luser = LUser, lserver = LServer},
               _To,
               IQ=#iq{type = get,
                      sub_el = PrefsEl = #xmlel{name = <<"prefs">>}}) ->
    ?DEBUG("Handling mam prefs IQ~n    from ~p ~n    packet ~p.",
              [From, IQ]),
    {DefaultMode, AlwaysJIDs, NewerJIDs} = get_prefs(LServer, LUser, <<"always">>),
    ?DEBUG("Extracted data~n\tDefaultMode ~p~n\tAlwaysJIDs ~p~n\tNewerJIDS ~p~n",
              [DefaultMode, AlwaysJIDs, NewerJIDs]),
    ResultPrefsEl = result_prefs(DefaultMode, AlwaysJIDs, NewerJIDs),
    IQ#iq{type = result, sub_el = [ResultPrefsEl]};
    
process_mam_iq(From=#jid{luser = LUser, lserver = LServer},
               To,
               IQ=#iq{type = get,
                      sub_el = QueryEl = #xmlel{name = <<"query">>}}) ->
    ?DEBUG("Handling mam IQ~n    from ~p ~n    to ~p~n    packet ~p.",
              [From, To, IQ]),
    QueryID = xml:get_tag_attr_s(<<"queryid">>, QueryEl),
    %% Filtering by date.
    %% Start :: integer() | undefined
    Start = maybe_unix_timestamp(xml:get_path_s(QueryEl, [{elem, <<"start">>}, cdata])),
    End   = maybe_unix_timestamp(xml:get_path_s(QueryEl, [{elem, <<"end">>}, cdata])),
    RSM   = jlib:rsm_decode(QueryEl),
    %% Filtering by contact.
    With  = xml:get_path_s(QueryEl, [{elem, <<"with">>}, cdata]),
    {WithSJID, WithSResource} =
    case With of
        <<>> -> {undefined, undefined};
        _    ->
            WithJID = #jid{lresource = WithLResource} = jlib:binary_to_jid(With),
            WithBareJID = jlib:jid_remove_resource(WithJID),
            {ejabberd_odbc:escape(jlib:jid_to_binary(WithBareJID)),
             case WithLResource of <<>> -> undefined;
                  _ -> ejabberd_odbc:escape(WithLResource) end}
    end,
    %% This element's name is "limit".
    %% But it must be "max" according XEP-0313.
    Limit = get_one_of_path_bin(QueryEl, [
                    [{elem, <<"set">>}, {elem, <<"max">>}, cdata],
                    [{elem, <<"set">>}, {elem, <<"limit">>}, cdata]
                   ]),
    PageSize = min(max_result_limit(),
                   maybe_integer(Limit, default_result_limit())),
    ?DEBUG("Parsed data~n\tStart ~p~n\tEnd ~p~n\tQueryId ~p~n\t"
               "Limit ~p~n\tPageSize ~p~n\t"
               "WithSJID ~p~n\tWithSResource ~p~n\tRSM: ~p~n",
          [Start, End, QueryID, Limit, PageSize, WithSJID, WithSResource, RSM]),
    SUser = ejabberd_odbc:escape(LUser),
    Filter = prepare_filter(SUser, Start, End, WithSJID, WithSResource),
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
on_send_packet(From, To, Packet) ->

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
filter_packet({From, To, Packet}) ->
    ?DEBUG("Receive packet~n    from ~p ~n    to ~p~n    packet ~p.",
              [From, To, Packet]),
    Packet2 =
    case handle_package(incoming, true, To, From, From, Packet) of
        undefined -> Packet;
        Id -> 
            BareTo = jlib:jid_to_binary(jlib:jid_remove_resource(To)),
            Archived = #xmlel{
                name = <<"archived">>,
                attrs=[{<<"by">>, BareTo}, {<<"id">>, Id}]},
            xml:append_subtags(Packet, [Archived]) 
    end,
    {From, To, Packet2}.

on_remove_user(User, Server) ->
    LUser = jlib:nodeprep(User),
    LServer = jlib:nameprep(Server),
    SUser = ejabberd_odbc:escape(LUser),
    remove_user(LServer, SUser),
    ?DEBUG("Remove user ~p from ~p.", [LUser, LServer]),
    ok.

%% ----------------------------------------------------------------------
%% Helpers

-spec handle_package(Dir, ReturnId, LocalJID, RemoteJID, FromJID, Packet) ->
    MaybeId when
    Dir :: incoming | outgoing,
    ReturnId :: boolean(),
    LocalJID :: jid(),
    RemoteJID :: jid(),
    FromJID :: jid(),
    Packet :: elem(),
    MaybeId :: binary() | undefined.

handle_package(Dir, ReturnId,
               _LocalJID=#jid{luser = LUser, lserver = LServer},
               RemoteJID=#jid{lresource = RLResource},
               FromJID=#jid{}, Packet) ->
    IsComplete = is_complete_message(Packet),
    case IsComplete of
        true ->
        SUser = ejabberd_odbc:escape(LUser),
        %% Convert `#jid{}' to prepared `{S,U,R}'
        LRJID = jlib:jid_tolower(RemoteJID),
        BareLRJID = jlib:jid_remove_resource(LRJID),
        SRJID = ejabberd_odbc:escape(jlib:jid_to_binary(LRJID)),
        BareSRJID = ejabberd_odbc:escape(jlib:jid_to_binary(BareLRJID)),
        IsInteresting =
        case behaviour(LServer, SUser, SRJID, BareSRJID) of
            always -> true;
            never  -> false;
            roster -> is_jid_in_user_roster(LServer, LUser, BareSRJID)
        end,
        case IsInteresting of
            true -> 
            SRResource = ejabberd_odbc:escape(RLResource),
            SData = ejabberd_odbc:escape(term_to_binary(Packet)),
            SDir = encode_direction(Dir),
            FromLJID = jlib:jid_tolower(FromJID),
            FromSJID = ejabberd_odbc:escape(jlib:jid_to_binary(FromLJID)),
            case ReturnId of
                true ->
                %% Archive and return id
                AtomicF = fun() ->
                archive_message(LServer, SUser, BareSRJID, SRResource,
                                SDir, FromSJID, SData),
                last_insert_id(LServer)
                end,
                {atomic, Id} = ejabberd_odbc:sql_transaction(LServer, AtomicF),
                Id;
                false ->
                archive_message(LServer, SUser, BareSRJID, SRResource,
                                SDir, FromSJID, SData),
                undefined
            end;
            false -> undefined
        end;
        false -> undefined
    end.

%% @doc Check, that the stanza is a message with body.
%% Servers SHOULD NOT archive messages that do not have a <body/> child tag.
-spec is_complete_message(Packet::#xmlel{}) -> boolean().
is_complete_message(Packet=#xmlel{name = <<"message">>}) ->
    case xml:get_tag_attr_s(<<"type">>, Packet) of
    Type when Type == <<"">>;
              Type == <<"normal">>;
              Type == <<"chat">>;
              Type == <<"groupchat">> ->
        case xml:get_subtag(Packet, <<"body">>) of
            false -> false;
            _     -> true
        end;
    %% Skip <<"error">> type
    _ -> false
    end;
is_complete_message(_) -> false.


%% @doc Form `<forwarded/>' element, according to the XEP.
-spec wrap_message(Packet::elem(), QueryID::binary(),
                   MessageUID::term(), DateTime::calendar:datetime(), FromJID::jid()) ->
        Wrapper::elem().
wrap_message(Packet, QueryID, MessageUID, DateTime, FromJID) ->
    #xmlel{
        name = <<"message">>,
        attrs = [],
        children = [result(QueryID, MessageUID,
                           [forwarded(Packet, DateTime, FromJID)])]}.

-spec forwarded(elem(), calendar:datetime(), jid()) -> elem().
forwarded(Packet, DateTime, FromJID) ->
    #xmlel{
        name = <<"forwarded">>,
        attrs = [{<<"xmlns">>, <<"urn:xmpp:forward:0">>}],
        children = [delay(DateTime, FromJID), Packet]}.

-spec delay(calendar:datetime(), jid()) -> elem().
delay(DateTime, FromJID) ->
    jlib:timestamp_to_xml(DateTime, utc, FromJID, <<>>).


%% @doc This element will be added in each forwarded message.
result(QueryID, MessageUID, Children) when is_list(Children) ->
    #xmlel{
        name = <<"result">>,
        attrs = [{<<"xmlns">>, mam_ns_binary()},
                 {<<"queryid">>, QueryID},
                 {<<"id">>, MessageUID}],
        children = Children}.


%% @doc This element will be added into "iq/query".
-spec result_set(FirstId, LastId, FirstIndexI, CountI) -> elem() when
    FirstId :: binary() | undefined,
    LastId  :: binary() | undefined,
    FirstIndexI :: non_neg_integer() | undefined,
    CountI      :: non_neg_integer().
result_set(FirstId, LastId, FirstIndexI, CountI) ->
    %% <result xmlns='urn:xmpp:mam:tmp' queryid='f27' id='28482-98726-73623' />
    FirstEl = [#xmlel{name = <<"first">>,
                      attrs = [{<<"index">>, integer_to_binary(FirstIndexI)}],
                      children = [#xmlcdata{content = FirstId}]
                     }
               || FirstId =/= undefined],
    LastEl = [#xmlel{name = <<"last">>,
                     children = [#xmlcdata{content = LastId}]
                    }
               || LastId =/= undefined],
    CountEl = #xmlel{
            name = <<"count">>,
            children = [#xmlcdata{content = integer_to_binary(CountI)}]},
     #xmlel{
        name = <<"set">>,
        attrs = [{<<"xmlns">>, rsm_ns_binary()}],
        children = FirstEl ++ LastEl ++ [CountEl]}.

result_query(SetEl) ->
     #xmlel{
        name = <<"query">>,
        attrs = [{<<"xmlns">>, mam_ns_binary()}],
        children = [SetEl]}.

-spec result_prefs(DefaultMode, AlwaysJIDs, NewerJIDs) -> ResultPrefsEl when
    DefaultMode :: binary(),
    AlwaysJIDs  :: [binary()],
    NewerJIDs   :: [binary()],
    ResultPrefsEl :: elem().
result_prefs(DefaultMode, AlwaysJIDs, NewerJIDs) ->
    AlwaysEl = #xmlel{name = <<"always">>,
                      children = encode_jids(AlwaysJIDs)},
    NewerEl  = #xmlel{name = <<"never">>,
                      children = encode_jids(NewerJIDs)},
    #xmlel{
       name = <<"prefs">>,
       attrs = [{<<"xmlns">>,mam_ns_binary()}, {<<"default">>, DefaultMode}],
       children = [AlwaysEl, NewerEl]
    }.

encode_jids(JIDs) ->
    [#xmlel{name = <<"jid">>, children = [#xmlcdata{content = JID}]}
     || JID <- JIDs].


-spec parse_prefs(PrefsEl) -> {DefaultMode, AlwaysJIDs, NewerJIDs} when
    PrefsEl :: elem(),
    DefaultMode :: binary(),
    AlwaysJIDs  :: [binary()],
    NewerJIDs   :: [binary()].
parse_prefs(El=#xmlel{name = <<"prefs">>, attrs = Attrs}) ->
    {value, Default} = xml:get_attr(<<"default">>, Attrs),
    AlwaysJIDs = parse_jid_list(El, <<"always">>),
    NewerJIDs  = parse_jid_list(El, <<"never">>),
    {Default, AlwaysJIDs, NewerJIDs}.

parse_jid_list(El, Name) ->
    case xml:get_subtag(El, Name) of
        false -> [];
        #xmlel{children = JIDEls} ->
            [xml:get_tag_cdata(JIDEl) || JIDEl <- JIDEls]
    end.

send_message(From, To, Mess) ->
    ejabberd_sm:route(From, To, Mess).


is_jid_in_user_roster(LServer, LUser, JID) ->
    {Subscription, _Groups} =
    ejabberd_hooks:run_fold(
        roster_get_jid_info, LServer,
        {none, []}, [LUser, LServer, JID]),
    Subscription == from orelse Subscription == both.


behaviour(LServer, SUser, SJID, BareSJID) ->
    case query_behaviour(LServer, SUser, SJID, BareSJID) of
        {selected, ["behaviour"], [{Behavour}]} ->
            case Behavour of
                "A" -> always;
                "N" -> never;
                "R" -> roster
            end;
        _ -> always %% default for everybody
    end.

query_behaviour(LServer, SUser, SJID, BareSJID) ->
    Result =
    ejabberd_odbc:sql_query(
      LServer,
      ["SELECT behaviour "
       "FROM mam_config "
       "WHERE local_username='", SUser, "' "
         "AND (remote_jid='' OR remote_jid='", SJID, "'",
               case BareSJID of
                    SJID -> "";
                    _    -> [" OR remote_jid='", BareSJID, "'"]
               end,
         ") "
       "ORDER BY remote_jid DESC "
       "LIMIT 1"]),
    ?DEBUG("query_behaviour query returns ~p", [Result]),
    Result.

update_settings(LServer, LUser, DefaultMode, AlwaysJIDs, NewerJIDs) ->
    SUser = ejabberd_odbc:escape(LUser),
    DelQuery = ["DELETE FROM mam_config WHERE local_username = '", SUser, "'"],
    InsQuery = ["INSERT INTO mam_config(local_username, behaviour, remote_jid) "
       "VALUES ", encode_first_config_row(SUser, encode_behaviour(DefaultMode), ""),
       [encode_config_row(SUser, "A", ejabberd_odbc:escape(JID))
        || JID <- AlwaysJIDs],
       [encode_config_row(SUser, "N", ejabberd_odbc:escape(JID))
        || JID <- NewerJIDs]],
    %% Run as a transaction
    {atomic, [DelResult, InsResult]} =
        sql_transaction_map(LServer, [DelQuery, InsQuery]),
    ?DEBUG("update_settings query returns ~p and ~p", [DelResult, InsResult]),
    ok.

encode_first_config_row(SUser, SBehavour, SJID) ->
    ["('", SUser, "', '", SBehavour, "', '", SJID, "')"].

encode_config_row(SUser, SBehavour, SJID) ->
    [", ('", SUser, "', '", SBehavour, "', '", SJID, "')"].

sql_transaction_map(LServer, Queries) ->
    AtomicF = fun() ->
        [ejabberd_odbc:sql_query(LServer, Query) || Query <- Queries]
    end,
    ejabberd_odbc:sql_transaction(LServer, AtomicF).

%% @doc Load settings from the database.
-spec get_prefs(LServer, LUser, GlobalDefaultMode) -> Result when
    LServer     :: server_hostname(),
    LUser       :: literal_username(),
    DefaultMode :: archive_behaviour_bin(),
    GlobalDefaultMode :: archive_behaviour_bin(),
    Result      :: {DefaultMode, AlwaysJIDs, NewerJIDs},
    AlwaysJIDs  :: [literal_jid()],
    NewerJIDs   :: [literal_jid()].
get_prefs(LServer, LUser, GlobalDefaultMode) ->
    SUser = ejabberd_odbc:escape(LUser),
    {selected, _ColumnNames, Rows} =
    ejabberd_odbc:sql_query(
      LServer,
      ["SELECT remote_jid, behaviour "
       "FROM mam_config "
       "WHERE local_username='", SUser, "'"]),
    decode_prefs_rows(Rows, GlobalDefaultMode, [], []).

decode_prefs_rows([{<<>>, Behavour}|Rows], _DefaultMode, AlwaysJIDs, NewerJIDs) ->
    decode_prefs_rows(Rows, decode_behaviour(Behavour), AlwaysJIDs, NewerJIDs);
decode_prefs_rows([{JID, <<"A">>}|Rows], DefaultMode, AlwaysJIDs, NewerJIDs) ->
    decode_prefs_rows(Rows, DefaultMode, [JID|AlwaysJIDs], NewerJIDs);
decode_prefs_rows([{JID, <<"N">>}|Rows], DefaultMode, AlwaysJIDs, NewerJIDs) ->
    decode_prefs_rows(Rows, DefaultMode, AlwaysJIDs, [JID|NewerJIDs]);
decode_prefs_rows([], DefaultMode, AlwaysJIDs, NewerJIDs) ->
    {DefaultMode, AlwaysJIDs, NewerJIDs}.


archive_message(LServer, SUser, BareSJID, SResource, Direction, FromSJID, SData) ->
    Result =
    ejabberd_odbc:sql_query(
      LServer,
      ["INSERT INTO mam_message(local_username, remote_bare_jid, "
                                "remote_resource, message, direction, "
                                "from_jid, added_at) "
       "VALUES ('", SUser,"', '", BareSJID, "', '", SResource, "',"
               "'", SData, "', '", Direction, "', '", FromSJID, "', ",
                integer_to_list(current_unix_timestamp()), ")"]),
    ?DEBUG("archive_message query returns ~p", [Result]),
    ok.


last_insert_id(LServer) ->
    {selected, [_], [{Id}]} =
    ejabberd_odbc:sql_query(LServer, "SELECT LAST_INSERT_ID()"),
    Id.


remove_user(LServer, SUser) ->
    Result1 =
    ejabberd_odbc:sql_query(
      LServer,
      ["DELETE "
       "FROM mam_config "
       "WHERE local_username='", SUser, "'"]),
    Result2 =
    ejabberd_odbc:sql_query(
      LServer,
      ["DELETE "
       "FROM mam_message "
       "WHERE local_username='", SUser, "'"]),
    ?DEBUG("remove_user query returns ~p and ~p", [Result1, Result2]),
    ok.

message_row_to_xml({BUID,BSeconds,BFromJID,BPacket}, QueryID) ->
    Packet = binary_to_term(BPacket),
    FromJID = jlib:binary_to_jid(BFromJID),
    Seconds  = list_to_integer(binary_to_list(BSeconds)),
    DateTime = calendar:now_to_universal_time(seconds_to_now(Seconds)),
    wrap_message(Packet, QueryID, BUID, DateTime, FromJID).

message_row_to_id({BUID,_,_,_}) ->
    BUID.

%% Each record is a tuple of form 
%% `{<<"3">>,<<"1366312523">>,<<"bob@localhost">>,<<"res1">>,<<binary>>}'.
%% Columns are `["id","added_at","from_jid","message"]'.
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
      ["SELECT id, added_at, from_jid, message "
       "FROM mam_message ",
        Filter,
       " ORDER BY added_at"
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


-spec prepare_filter(SUser, IStart, IEnd, WithSJID, WithSResource) -> filter()
    when
    SUser   :: escaped_username(),
    IStart  :: unix_timestamp() | undefined,
    IEnd    :: unix_timestamp() | undefined,
    WithSJID :: escaped_jid(),
    WithSResource :: escaped_resource().
prepare_filter(SUser, IStart, IEnd, WithSJID, WithSResource) ->
   ["WHERE local_username='", SUser, "'",
     case IStart of
        undefined -> "";
        _         -> [" AND added_at >= ", integer_to_list(IStart)]
     end,
     case IEnd of
        undefined -> "";
        _         -> [" AND added_at <= ", integer_to_list(IEnd)]
     end,
     case WithSJID of
        undefined -> "";
        _         -> [" AND remote_bare_jid = '", WithSJID, "'"]
     end,
     case WithSResource of
        undefined -> "";
        _         -> [" AND remote_resource = '", WithSResource, "'"]
     end].


%% "maybe" means, that the function may return 'undefined'.
-spec maybe_unix_timestamp(iso8601_datetime_binary()) -> unix_timestamp();
                          (<<>>) -> undefined.
maybe_unix_timestamp(<<>>) -> undefined;
maybe_unix_timestamp(ISODateTime) -> 
    case iso8601_datetime_binary_to_timestamp(ISODateTime) of
        undefined -> undefined;
        Stamp -> now_to_seconds(Stamp)
    end.

-spec current_unix_timestamp() -> unix_timestamp().
current_unix_timestamp() ->
    now_to_seconds(os:timestamp()).

-spec now_to_seconds(erlang:timestamp()) -> unix_timestamp().
now_to_seconds({Mega, Secs, _}) ->
    1000000 * Mega + Secs.

-spec seconds_to_now(unix_timestamp()) -> erlang:timestamp().
seconds_to_now(Seconds) when is_integer(Seconds) ->
    {Seconds div 1000000, Seconds rem 1000000, 0}.

%% @doc Returns time in `now()' format.
-spec iso8601_datetime_binary_to_timestamp(iso8601_datetime_binary()) ->
    erlang:timestamp().
iso8601_datetime_binary_to_timestamp(DateTime) when is_binary(DateTime) ->
    jlib:datetime_string_to_timestamp(binary_to_list(DateTime)).


-spec maybe_integer(binary()) -> integer() | undefined.
maybe_integer(Bin) -> maybe_integer(Bin, undefined).

maybe_integer(<<>>, Def) -> Def;
maybe_integer(Bin, _Def) when is_binary(Bin) ->
    list_to_integer(binary_to_list(Bin)).


get_one_of_path_bin(Elem, List) ->
    get_one_of_path(Elem, List, <<>>).

get_one_of_path(Elem, [H|T], Def) ->
    case xml:get_path_s(Elem, H) of
        Def -> get_one_of_path(Elem, T, Def);
        Val  -> Val
    end;
get_one_of_path(_Elem, [], Def) ->
    Def.

