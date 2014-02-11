-module(alarm_logger).
-behaviour(gen_event).

-export([add_handler/0,
         delete_handler/0]).

%% gen_event callbacks
-export([init/1,
         handle_event/2,
         handle_call/2,
         handle_info/2,
         code_change/3,
         terminate/2]).

-include("ejabberd.hrl").
-define(ALARM_MANAGER, alarm_handler).
-record(state, {}).

%% =======================================================================

add_handler() ->
    ok = gen_event:add_handler(?ALARM_MANAGER, ?MODULE, []).

%% @doc Delete an installed handler
%% @end
delete_handler() ->
    etorrent_event:delete_handler(?ALARM_MANAGER, ?MODULE, []).

%% =======================================================================

%% @private
init([]) ->
    {ok, #state{}}.

%% @private
handle_event({set_alarm, {AlarmId, AlarmDescr}}, State) ->
    ?WARNING_MSG("Set alarm ~p. Details ~p", [AlarmId, AlarmDescr]),
    {ok, State};
handle_event({clear_alarm, AlarmId}, State) ->
    ?WARNING_MSG("Clear alarm ~p.", [AlarmId]),
    {ok, State};
handle_event(Event, State) ->
    ?WARNING_MSG("Unknown event ~p", [Event]),
    {ok, State}.

%% @private
handle_info(_, State) ->
    {ok, State}.

%% @private
terminate(_, State) ->
    State.

%% @private
handle_call(null, State) ->
    {ok, null, State}.

%% @private
code_change(_OldVsn, State, _Extra) ->
    {ok, State}.
