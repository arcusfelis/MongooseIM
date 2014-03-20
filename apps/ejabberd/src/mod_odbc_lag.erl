%%%----------------------------------------------------------------------
%%% File    : mod_odbc_lag.erl
%%% Author  : Michael Uvarov <michael.uvarov@erlang-solutions.com>
%%% Purpose : Measure database delay
%%%
%%% MongooseIM, Copyright (C) 2014   Erlang Solutions
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

-module(mod_odbc_lag).
-author('bjc@kublai.com').

-behavior(gen_mod).
-behavior(gen_server).

-include("ejabberd.hrl").
-include("jlib.hrl").

-define(SUPERVISOR, ejabberd_sup).
-define(DEFAULT_PING_INTERVAL, 5). % seconds

%% API
-export([start_link/2]).

%% gen_mod callbacks
-export([start/2, stop/1]).

%% gen_server callbacks
-export([init/1, terminate/2, handle_call/3, handle_cast/2,
         handle_info/2, code_change/3]).

-record(state, {host, ping_interval}).

%%====================================================================
%% API
%%====================================================================
start_link(Host, Opts) ->
    Proc = gen_mod:get_module_proc(Host, ?MODULE),
    gen_server:start_link({local, Proc}, ?MODULE, [Host, Opts], []).

%%====================================================================
%% gen_mod callbacks
%%====================================================================
start(Host, Opts) ->
    Proc = gen_mod:get_module_proc(Host, ?MODULE),
    PingSpec = {Proc, {?MODULE, start_link, [Host, Opts]},
                transient, 2000, worker, [?MODULE]},
    supervisor:start_child(?SUPERVISOR, PingSpec).

stop(Host) ->
    Proc = gen_mod:get_module_proc(Host, ?MODULE),
    gen_server:call(Proc, stop),
    supervisor:delete_child(?SUPERVISOR, Proc).

%%====================================================================
%% gen_server callbacks
%%====================================================================
init([Host, Opts]) ->
    PingInterval = gen_mod:get_opt(ping_interval, Opts, ?DEFAULT_PING_INTERVAL),
    timer:send_interval(PingInterval * 1000, ping),
    {ok, #state{host = Host, ping_interval = PingInterval}}.

handle_call(stop, _From, State) ->
    {stop, normal, ok, State};
handle_call(_Req, _From, State) ->
    {reply, {error, badarg}, State}.

handle_cast(_Msg, State) ->
    {noreply, State}.

handle_info(ping, State=#state{host=Host}) ->
    {Mcs3, _} = timer:tc(fun() ->
        ejabberd_odbc:sql_query(Host, "select 1"),
        ejabberd_odbc:sql_query(Host, "select 2"),
        ejabberd_odbc:sql_query(Host, "select 3")
        end),
    Mcs = Mcs3 div 3,
    ejabberd_hooks:run(odbc_lag, Host, [Host, Mcs]),
    {noreply, State#state{}};
handle_info(_Info, State) ->
    {noreply, State}.

code_change(_OldVsn, State, _Extra) ->
    {ok, State}.

terminate(_Reason, State) ->
    {ok, State}.

