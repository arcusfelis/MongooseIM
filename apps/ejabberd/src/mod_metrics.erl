%%%-------------------------------------------------------------------
%%% @author Michal Piotrowski <michal.piotrowski@erlang-solutions.com>
%%% @copyright (C) 2013, Erlang Solutions Ltd.
%%% @doc Implementation of MongooseIM metrics.
%%%
%%% @end
%%% Created : 23 Apr 2013 by Michal Piotrowski <michal.piotrowski@erlang-solutions.com>
%%%-------------------------------------------------------------------
-module (mod_metrics).

-behaviour (gen_mod).

-export ([start/2, stop/1]).
-export([start_link/1]).

%% gen_server callbacks
-export([init/1, handle_call/3, handle_cast/2, handle_info/2,
         terminate/2, code_change/3]).

-define(REST_LISTENER, ejabberd_metrics_rest).
-define(PROCNAME, ejabberd_metrics).
-include("ejabberd.hrl").
-include("jlib.hrl").

-record(state, {host}).

-spec start(binary(), list()) -> ok.
start(Host, Opts) ->
    init_folsom(Host),
    start_cowboy(Opts),
    metrics_hooks(add, Host),
    start_server(Host),
    ok.

-spec stop(binary()) -> ok.
stop(Host) ->
    stop_cowboy(),
    metrics_hooks(delete, Host),
    stop_server(Host),
    ok.

init_folsom(Host) ->
    folsom:start(),
    lists:foreach(fun(Name) ->
        folsom_metrics:new_spiral(Name),
        folsom_metrics:tag_metric(Name, Host)
    end, get_general_counters(Host)),

    lists:foreach(fun(Name) ->
        folsom_metrics:new_counter(Name),
        folsom_metrics:tag_metric(Name, Host)
    end, get_total_counters(Host)),

    %% Called from `ejabberd_router:do_route/3'.
    folsom_metrics:new_histogram(route_time),

    folsom_metrics:new_gauge({Host, modMamMessageQueueLength}),
    folsom_metrics:tag_metric({Host, modMamMessageQueueLength}, Host),
    folsom_metrics:new_gauge({Host, odbcQueryQueueLength}),
    folsom_metrics:tag_metric({Host, odbcQueryQueueLength}, Host).

metrics_hooks(Op, Host) ->
    lists:foreach(fun(Hook) ->
        apply(ejabberd_hooks, Op, Hook)
    end, ejabberd_metrics_hooks:get_hooks(Host)).

-define (GENERAL_COUNTERS, [
         sessionSuccessfulLogins,
         sessionAuthAnonymous,
         sessionAuthFails,
         sessionLogouts,
         xmppMessageSent,
         xmppMessageReceived,
         xmppMessageBounced,
         xmppPresenceSent,
         xmppPresenceReceived,
         xmppIqSent,
         xmppIqReceived,
         xmppStanzaSent,
         xmppStanzaReceived,
         xmppStanzaDropped,
         xmppStanzaCount,
         xmppErrorTotal,
         xmppErrorBadRequest,
         xmppErrorIq,
         xmppErrorMessage,
         xmppErrorPresence,
         xmppIqTimeouts,
         modRosterSets,
         modRosterGets,
         modPresenceSubscriptions,
         modPresenceUnsubscriptions,
         modRosterPush,
         modRegisterCount,
         modUnregisterCount,
         modPrivacySets,
         modPrivacySetsActive,
         modPrivacySetsDefault,
         modPrivacyPush,
         modPrivacyGets,
         modPrivacyStanzaBlocked,
         modPrivacyStanzaAll
         ]).

get_general_counters(Host) ->
    [{Host, Counter} || Counter <- ?GENERAL_COUNTERS].

-define (TOTAL_COUNTERS, [
         sessionCount
         ]).

get_total_counters(Host) ->
    [{Host, Counter} || Counter <- ?TOTAL_COUNTERS].

start_cowboy(Opts) ->
    NumAcceptors = gen_mod:get_opt(num_acceptors, Opts, 10),
    IP = gen_mod:get_opt(ip, Opts, {0,0,0,0}),
    case gen_mod:get_opt(port, Opts, undefined) of
        undefined ->
            ok;
        Port ->
            Dispatch = cowboy_router:compile([{'_', [
                                {"/metrics", ?REST_LISTENER, [available_metrics]},
                                {"/metrics/m", ?REST_LISTENER, [sum_metrics]},
                                {"/metrics/m/:metric", ?REST_LISTENER, [sum_metric]},
                                {"/metrics/host/:host/:metric", ?REST_LISTENER, [host_metric]},
                                {"/metrics/host/:host", ?REST_LISTENER, [host_metrics]}
                                ]}]),
            case cowboy:start_http(?REST_LISTENER, NumAcceptors,
                                   [{port, Port}, {ip, IP}],
                                   [{env, [{dispatch, Dispatch}]}]) of
                {error, {already_started, _Pid}} ->
                    ok;
                {ok, _Pid} ->
                    ok;
                {error, Reason} ->
                    {error, Reason}
            end
    end.

stop_cowboy() ->
    cowboy:stop(?REST_LISTENER).

start_server(Host) ->
    Proc = gen_mod:get_module_proc(Host, ?PROCNAME),
    ChildSpec =
	{Proc,
	 {?MODULE, start_link, [Host]},
	 permanent,
	 1000,
	 worker,
	 [?MODULE]},
    supervisor:start_child(ejabberd_sup, ChildSpec),
    ok.

stop_server(Host) ->
    Proc = gen_mod:get_module_proc(Host, ?PROCNAME),
    supervisor:terminate_child(ejabberd_sup, Proc),
    supervisor:delete_child(ejabberd_sup, Proc),
    ok.

%%====================================================================
%% API
%%====================================================================

start_link(Host) ->
    gen_server:start_link(?MODULE, [Host], []).

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
    {ok, _TRef} = timer:send_interval(1000, update_metrics),
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
handle_call(Msg, _From, State) ->
    ?WARNING_MSG("Strange message ~p.", [Msg]),
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

handle_info(update_metrics, State=#state{host=Host}) ->
    Workers = ejabberd_odbc_sup:get_pids(Host),
    Lengths = [message_queue_len(Pid) || Pid <- Workers],
    folsom_metrics:notify({{Host, odbcQueryQueueLength}, lists:sum(Lengths)}),
    case mod_mam_odbc_async_writer:queue_length(Host) of
    {ok, MamWriterQLen} ->
        folsom_metrics:notify({{Host, modMamMessageQueueLength}, MamWriterQLen});
    {error, _Reason} ->
        skip
    end,
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


%%====================================================================
%% Internal functions
%%====================================================================

message_queue_len(Pid) ->
    {message_queue_len, Len} = erlang:process_info(Pid, message_queue_len),
    Len.
