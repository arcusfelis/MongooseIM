%% @doc Stores cache using ETS-table.
-module(mod_mam_muc_cache).
-export([start_link/0,
         is_logging_enabled/2,
         is_querying_enabled/2,
         update_logging_enabled/3,
         update_querying_enabled/3,
         room_id/2]).

%% gen_server callbacks
-export([init/1, handle_call/3, handle_cast/2, handle_info/2,
         terminate/2, code_change/3]).


-include("ejabberd.hrl").
-include("jlib.hrl").

-record(state, {}).

%% @private
srv_name() ->
    mod_mam_cache.

tbl_name_room_id() ->
    mod_mam_cache_table_room_id.

tbl_name_logging() ->
    mod_mam_cache_table_logging_enabled.

tbl_name_querying() ->
    mod_mam_cache_table_querying_enabled.

%%====================================================================
%% API
%%====================================================================

start_link() ->
    gen_server:start_link({local, srv_name()}, ?MODULE, [], []).

is_logging_enabled(LServer, RoomName) ->
    case lookup_logging_enabled(LServer, RoomName) of
        not_found ->
            case query_enable_logging(LServer, RoomName) of
                undefined -> is_logging_enabled_by_default(LServer);
                IsEnabled -> IsEnabled
            end;
        IsEnabled ->
            IsEnabled
    end.

is_querying_enabled(LServer, RoomName) ->
    case lookup_querying_enabled(LServer, RoomName) of
        not_found ->
            case query_enable_querying(LServer, RoomName) of
                undefined -> is_querying_enabled_by_default(LServer);
                IsEnabled -> IsEnabled
            end;
        IsEnabled ->
            IsEnabled
    end.

room_id(LServer, RoomName) ->
    case lookup_room_id(LServer, RoomName) of
        not_found ->
            RoomId = query_room_id(LServer, RoomName),
            update_room_id(LServer, RoomName, RoomId),
            RoomId;
        RoomId ->
            RoomId
    end.

is_logging_enabled_by_default(LServer) ->
    true.

is_querying_enabled_by_default(LServer) ->
    true.

%% @doc Put new value into the cache.
update_querying_enabled(LServer, RoomName, Enabled) ->
    gen_server:call(srv_name(), {update_querying_enabled, LServer, RoomName, Enabled}).

%% @doc Put new value into the cache.
update_logging_enabled(LServer, RoomName, Enabled) ->
    gen_server:call(srv_name(), {update_logging_enabled, LServer, RoomName, Enabled}).

%%====================================================================
%% Internal functions
%%====================================================================

%% @private
update_room_id(LServer, RoomName, RoomId) ->
    gen_server:call(srv_name(), {update_room_id, LServer, RoomName, RoomId}).

lookup_querying_enabled(LServer, RoomName) ->
    try
        ets:lookup_element(tbl_name_querying(), {RoomName, LServer}, 2)
    catch error:badarg ->
        not_found
    end.

lookup_logging_enabled(LServer, RoomName) ->
    try
        ets:lookup_element(tbl_name_logging(), {RoomName, LServer}, 2)
    catch error:badarg ->
        not_found
    end.

lookup_room_id(LServer, RoomName) ->
    try
        ets:lookup_element(tbl_name_room_id(), {RoomName, LServer}, 2)
    catch error:badarg ->
        not_found
    end.

query_room_id(LServer, RoomName) ->
    SRoomName = ejabberd_odbc:escape(RoomName),
    Result =
    ejabberd_odbc:sql_query(
      LServer,
      ["SELECT id "
       "FROM mam_muc_room "
       "WHERE room_name='", SRoomName, "' "
       "LIMIT 1"]),

    case Result of
        {selected, ["id"], [{IdBin}]} ->
            binary_to_integer(IdBin);
        {selected, ["id"], []} ->
            %% The room is not found
            create_room_archive(LServer, RoomName),
            query_room_id(LServer, RoomName)
    end.


query_enable_logging(LServer, RoomName) ->
    select_bool(LServer, RoomName, "enable_logging").

query_enable_querying(LServer, RoomName) ->
    select_bool(LServer, RoomName, "enable_querying").

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

create_room_archive(LServer, RoomName) ->
    SRoomName = ejabberd_odbc:escape(RoomName),
    ejabberd_odbc:sql_query(
      LServer,
      ["INSERT INTO mam_muc_room(room_name) "
       "VALUES ('", SRoomName,"')"]),
    ok.

    

%%====================================================================
%% gen_server callbacks
%%====================================================================

%%--------------------------------------------------------------------
%% Function: init(Args) -> {ok, State} |
%%                         {ok, State, Timeout} |
%%                         ignore               |
%%                         {stop, Reason}
%% Description: Initiates the server
%%--------------------------------------------------------------------
init([]) ->
    TOpts = [named_table, protected,
             {write_concurrency, false},
             {read_concurrency, true}],
    [ets:new(N, TOpts)
     || N <- [tbl_name_room_id(), tbl_name_logging(), tbl_name_querying()]],
    {ok, #state{}}.

%%--------------------------------------------------------------------
%% Function: %% handle_call(Request, From, State) -> {reply, Reply, State} |
%%                                      {reply, Reply, State, Timeout} |
%%                                      {noreply, State} |
%%                                      {noreply, State, Timeout} |
%%                                      {stop, Reason, Reply, State} |
%%                                      {stop, Reason, State}
%% Description: Handling call messages
%%--------------------------------------------------------------------
handle_call({update_querying_enabled, LServer, RoomName, Enabled}, _From, State) ->
    ets:insert(tbl_name_room_id(), {{LServer, RoomName}, Enabled}),
    {reply, ok, State};
handle_call({update_logging_enabled, LServer, RoomName, Enabled}, _From, State) ->
    ets:insert(tbl_name_room_id(), {{LServer, RoomName}, Enabled}),
    {reply, ok, State};
handle_call({update_room_id, LServer, RoomName, RoomId}, _From, State) ->
    ets:insert(tbl_name_room_id(), {{LServer, RoomName}, RoomId}),
    {reply, ok, State}.


%%--------------------------------------------------------------------
%% Function: handle_cast(Msg, State) -> {noreply, State} |
%%                                      {noreply, State, Timeout} |
%%                                      {stop, Reason, State}
%% Description: Handling cast messages
%%--------------------------------------------------------------------

handle_cast(Msg, State) ->
    ?WARNING_MSG("Strange message ~p.", [Msg]),
    {noreply, State}.

%%--------------------------------------------------------------------
%% Function: handle_info(Info, State) -> {noreply, State} |
%%                                       {noreply, State, Timeout} |
%%                                       {stop, Reason, State}
%% Description: Handling all non call/cast messages
%%--------------------------------------------------------------------

handle_info(_Msg, State) ->
    {noreply, State}.

%%--------------------------------------------------------------------
%% Function: terminate(Reason, State) -> void()
%% Description: This function is called by a gen_server when it is about to
%% terminate. It should be the opposite of Module:init/1 and do any necessary
%% cleaning up. When it returns, the gen_server terminates with Reason.
%% The return value is ignored.
%%--------------------------------------------------------------------
terminate(_Reason, _State) ->
    ok.

%%--------------------------------------------------------------------
%% Func: code_change(OldVsn, State, Extra) -> {ok, NewState}
%% Description: Convert process state when code is changed
%%--------------------------------------------------------------------
code_change(_OldVsn, State, _Extra) ->
    {ok, State}.

