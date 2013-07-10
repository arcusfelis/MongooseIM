%% @doc Stores cache using ETS-table.
-module(mod_mam_async_writer).
-export([start_link/2,
         archive_message/8]).

%% gen_server callbacks
-export([init/1, handle_call/3, handle_cast/2, handle_info/2,
         terminate/2, code_change/3]).


-include("ejabberd.hrl").
-include("jlib.hrl").

-record(state, {
    flush_interval=500,
    max_queue_size=10,
    host,
    acc=[],
    flush_interval_tref}).

%%====================================================================
%% API
%%====================================================================

start_link(ProcName, Host) ->
    gen_server:start_link({local, ProcName}, ?MODULE, [Host], []).

archive_message(WriterPid, Id, SUser, BareSJID, SResource, Direction, FromSJID, SData) ->
    Msg = {archive_message, Id, SUser, BareSJID, SResource, Direction, FromSJID, SData},
    gen_server:cast(WriterPid, Msg).

%%====================================================================
%% Internal functions
%%====================================================================

run_flush(State=#state{host=Host, flush_interval_tref=TRef, acc=Acc}) ->
    erlang:cancel_timer(TRef),
    ?DEBUG("Flushed ~p entries.", [length(Acc)]),
    Result =
    ejabberd_odbc:sql_query(
      Host,
      ["INSERT INTO mam_message(id, local_username, remote_bare_jid, "
                                "remote_resource, message, direction, "
                                "from_jid) "
       "VALUES ", join(Acc)]),
    ?DEBUG("archive_message query returns ~p", [Result]),
    State#state{acc=[], flush_interval_tref=undefined}.

join([H|T]) ->
    [H, [", " ++ X || X <- T]].

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
init([Host]) ->
    {ok, #state{host=Host}}.

%%--------------------------------------------------------------------
%% Function: %% handle_call(Request, From, State) -> {reply, Reply, State} |
%%                                      {reply, Reply, State, Timeout} |
%%                                      {noreply, State} |
%%                                      {noreply, State, Timeout} |
%%                                      {stop, Reason, Reply, State} |
%%                                      {stop, Reason, State}
%% Description: Handling call messages
%%--------------------------------------------------------------------
handle_call(_, _From, State) ->
    {reply, ok, State}.


%%--------------------------------------------------------------------
%% Function: handle_cast(Msg, State) -> {noreply, State} |
%%                                      {noreply, State, Timeout} |
%%                                      {stop, Reason, State}
%% Description: Handling cast messages
%%--------------------------------------------------------------------

handle_cast({archive_message, Id, SUser, BareSJID, SResource, Direction, FromSJID, SData},
            State=#state{acc=Acc, flush_interval_tref=TRef, flush_interval=Int,
                         max_queue_size=Max}) ->
    ?DEBUG("Schedule to write ~p.", [Id]),
    Values = ["(", integer_to_list(Id), ", '", SUser,"', '", BareSJID, "', "
               "'", SResource, "', '", SData, "', '", Direction, "', "
               "'", FromSJID, "')"],
    TRef2 = case {Acc, TRef} of
            {[], undefined} -> erlang:send_after(Int, self(), flush);
            {_, _} -> TRef
            end,
    State2 = State#state{acc=[Values|Acc], flush_interval_tref=TRef2},
    case length(Acc) + 1 >= Max of
        true -> {noreply, run_flush(State2)};
        false -> {noreply, State2}
    end;
handle_cast(Msg, State) ->
    ?WARNING_MSG("Strange message ~p.", [Msg]),
    {noreply, State}.


%%--------------------------------------------------------------------
%% Function: handle_info(Info, State) -> {noreply, State} |
%%                                       {noreply, State, Timeout} |
%%                                       {stop, Reason, State}
%% Description: Handling all non call/cast messages
%%--------------------------------------------------------------------

handle_info(flush, State) ->
    {noreply, run_flush(State)}.

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

