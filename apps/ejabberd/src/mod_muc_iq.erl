%% @doc Stores a table of custom IQ-hanlers for mod_muc_room.
-module(mod_muc_iq).
-export([start_link/0,
         process_iq/4,
         register_iq_handler/4,
         register_iq_handler/5,
         unregister_iq_handler/2]).

%% gen_server callbacks
-export([init/1, handle_call/3, handle_cast/2, handle_info/2,
         terminate/2, code_change/3]).


-include("ejabberd.hrl").
-include("jlib.hrl").

-record(state, {}).

%% @private
srv_name() ->
    mod_muc_iq.

%%====================================================================
%% API
%%====================================================================

start_link() ->
    gen_server:start_link({local, srv_name()}, ?MODULE, [], []).
    

%% @doc Handle custom IQ.
%% Called from mod_muc_room.
process_iq(Host, From, RoomJID, IQ = #iq{xmlns = XMLNS}) ->
    case ets:lookup(muc_iqtable, {XMLNS, Host}) of
        [{_, Module, Function}] ->
            Module:Function(From, RoomJID, IQ);
        [{_, Module, Function, Opts}] ->
            gen_iq_handler:stop_iq_handler(Module, Function, Opts);
        [] -> error
    end.


register_iq_handler(Host, XMLNS, Module, Fun) ->
    gen_server:cast(srv_name(),
                    {register_iq_handler, Host, XMLNS, Module, Fun}).

register_iq_handler(Host, XMLNS, Module, Fun, Opts) ->
    gen_server:cast(srv_name(),
                    {register_iq_handler, Host, XMLNS, Module, Fun, Opts}).

unregister_iq_handler(Host, XMLNS) ->
    gen_server:cast(srv_name(),
                    {unregister_iq_handler, Host, XMLNS}).

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
    ets:new(muc_iqtable, [named_table]),
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

handle_cast({register_iq_handler, Host, XMLNS, Module, Function}, State) ->
    ets:insert(muc_iqtable, {{XMLNS, Host}, Module, Function}),
    {noreply, State};
handle_cast({register_iq_handler, Host, XMLNS, Module, Function, Opts}, State) ->
    ets:insert(muc_iqtable, {{XMLNS, Host}, Module, Function, Opts}),
    {noreply, State};
handle_cast({unregister_iq_handler, Host, XMLNS}, State) ->
    ets:delete(muc_iqtable, {XMLNS, Host}),
    {noreply, State};
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

