%%%----------------------------------------------------------------------
%%% MongooseIM
%%%
%%% This program is free software; you can redistribute it and/or
%%% modify it under the terms of the GNU General Public License as
%%% published by the Free Software Foundation; either version 2 of the
%%% License, or (at your option) any later version.
%%%
%%% This program is distributed in the hope that it will be useful,
%%% but WITHOUT ANY WARRANTY; without even the implied warranty of
%%% MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.  See the GNU
%%% General Public License for more details.
%%%
%%% You should have received a copy of the GNU General Public License
%%% along with this program; if not, write to the Free Software
%%% Foundation, Inc., 59 Temple Place, Suite 330, Boston, MA
%%% 02111-1307 USA
%%%
%%%----------------------------------------------------------------------

-module(mod_offline_kafka_writer).

-export([start/2,
         stop/1,
         produce/6]).

%% Internal exports
-export([start_link/3]).

%% gen_server callbacks
-export([init/1, handle_call/3, handle_cast/2, handle_info/2,
         terminate/2, code_change/3]).

-include("ejabberd.hrl").
-include("jlib.hrl").
-include("mod_offline.hrl").

-record(state, {
    flush_interval=500,
    max_packet_size=30,
    host,
    conn,
    acc=[],
    flush_interval_tref}).

start(Host, Opts) ->
    start_workers(Host),
    ok.

stop(Host) ->
    stop_workers(Host),
    ok.

%% Supervision
%% ------------------------------------------------------------------

pool_size(_Host) ->
    4.

start_workers(Host) ->
    [start_worker(Host, N)
    || N <- lists:seq(1, pool_size(Host))].

stop_workers(Host) ->
    [stop_worker(Host, N)
    || N <- lists:seq(1, pool_size(Host))].
    
start_worker(Host, N) ->
    Proc = srv_name(Host, N),
    ChildSpec =
    {Proc,
     {?MODULE, start_link, [Proc, Host, 0]},
     permanent,
     5000,
     worker,
     [?MODULE]},
    supervisor:start_child(ejabberd_sup, ChildSpec),
    ok.

stop_worker(Host, N) ->
    Proc = srv_name(Host, N),
    supervisor:terminate_child(ejabberd_sup, Proc),
    supervisor:delete_child(ejabberd_sup, Proc).

start_link(Name, Host, Broker) ->
    gen_server:start_link({local, Name}, ?MODULE, [Host, Broker], []).

choose_server(Host, LUser) ->
    Size = pool_size(Host),
    %% N = 1 .. Size
    N = erlang:phash2(LUser, Size) + 1,
    srv_name(Host, N).

srv_name(Host, N) when is_binary(Host), is_integer(N) ->
    SHost = binary_to_list(Host),
    SN = integer_to_list(N),
    list_to_atom("mod_offline_writer_" ++ SHost ++ "_" ++ SN).


%% API
%% ------------------------------------------------------------------

produce(LUser, LServer, Broker, Topic, Part, Bins) ->
    Server = choose_server(LServer, LUser),
    produce_request(Server, Topic, Part, Bins).

produce_request(Server, Topic, Part, Bins) ->
    gen_server:cast(Server, {produce_request, {Topic, Part, Bins}}).

%%====================================================================
%% Internal functions
%%====================================================================

run_flush(State=#state{acc=[]}) ->
    State;
run_flush(State=#state{conn=Conn, flush_interval_tref=TRef, acc=Acc}) ->
    ?DEBUG("Multiproduce to ~p topics.", [length(Acc)]),
    Magic = 1,
    Compression = 0,
    TopicPartMsgs = [
        {Topic, Part, [{Magic, Compression, Msg} || Msg <- Msgs]}
        || {Topic, Part, Msgs} <- Acc],
    kafka_simple_api:multi_produce(Conn, TopicPartMsgs),
    TRef =/= undefined andalso erlang:cancel_timer(TRef),
    State#state{acc=[], flush_interval_tref=undefined}.

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
init([Host, Broker]) ->
    {ok, #state{host=Host, conn=Broker}}.

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

handle_cast({produce_request, TopicPartBins},
            State=#state{acc=Acc, flush_interval_tref=TRef, flush_interval=Int,
                         max_packet_size=Max}) ->
    TRef2 = case {Acc, TRef} of
            {[], undefined} -> erlang:send_after(Int, self(), flush);
            {_, _} -> TRef
            end,
    Req = TopicPartBins,
    State2 = State#state{acc=[Req|Acc], flush_interval_tref=TRef2},
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

