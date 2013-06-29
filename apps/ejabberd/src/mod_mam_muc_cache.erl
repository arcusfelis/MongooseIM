%% @doc Stores cache using ETS-table.
-module(mod_mam_muc_cache).
-export([start_link/0,
         is_logging_enabled/2,
         room_id/2]).

%% gen_server callbacks
-export([init/1, handle_call/3, handle_cast/2, handle_info/2,
         terminate/2, code_change/3]).


-include("ejabberd.hrl").
-include("jlib.hrl").

-record(state, {}).

%% @private
srv_name() ->
    mod_mam_iq_cache.

tbl_name() ->
    mod_mam_iq_cache_table.

%%====================================================================
%% API
%%====================================================================

start_link() ->
    gen_server:start_link({local, srv_name()}, ?MODULE, [], []).

is_logging_enabled(LServer, RoomName) ->
    case query_enable_logging(LServer, RoomName) of
        undefined -> is_logging_enabled_by_default(LServer);
        IsEnabled -> IsEnabled
    end.

is_logging_enabled_by_default(LServer) ->
    true.

room_id(LServer, RoomName) ->
    query_room_id(LServer, RoomName).

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
    SRoomName = ejabberd_odbc:escape(RoomName),
    Result =
    ejabberd_odbc:sql_query(
      LServer,
      ["SELECT enable_logging "
       "FROM mam_muc_room "
       "WHERE room_name='", SRoomName, "' "
       "LIMIT 1"]),

    case Result of
        {selected, ["enable_logging"], [{Res}]} ->
            case Res of
                null    -> undefined;
                <<"1">> -> true;
                <<"0">> -> false
            end;
        {selected, ["enable_logging"], []} ->
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
    ets:new(tbl_name(), [named_table, protected,
                         {write_concurrency, false},
                         {read_concurrency, true}]),
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
handle_call(_Request, _From, State) ->
    Reply = ok,
    {reply, Reply, State}.

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

