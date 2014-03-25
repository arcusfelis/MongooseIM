%%%----------------------------------------------------------------------
%%% File    : mod_offline_purger.erl
%%% Author  : Uvarov Michael <michael.uvarov@erlang-solutios.com>
%%% Purpose : Delete old offline messages
%%% Created : 11 Jul 2009 by Brian Cully <bjc@kublai.com>
%%%
%%%
%%% MongooseIM, Copyright (C) 2014 Erlang Solutions
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

-module(mod_offline_purger).
-author('michael.uvarov@erlang-solutions.com').

-behavior(gen_mod).
-behavior(gen_server).

-include("ejabberd.hrl").
-include("jlib.hrl").

-define(SUPERVISOR, ejabberd_sup).

%% API
-export([start_link/2]).

%% gen_mod callbacks
-export([start/2, stop/1]).

%% gen_server callbacks
-export([init/1, terminate/2, handle_call/3, handle_cast/2,
         handle_info/2, code_change/3]).

-record(state, {host,
                max_age,
                retry_interval}).

default_max_age() ->
    %% 3 weeks
    timer:hours(21 * 24).

default_retry_interval() ->
    timer:minutes(1).

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
    %% In milliseconds
    MaxAge = gen_mod:get_opt(max_age, Opts, default_max_age()),
    RetryInterval = gen_mod:get_opt(retry_interval, Opts, default_retry_interval()),
    retry_after(Host, RetryInterval),
    {ok, #state{host = Host,
                max_age = MaxAge,
                retry_interval = RetryInterval}}.

handle_call(stop, _From, State) ->
    {stop, normal, ok, State};
handle_call(_Req, _From, State) ->
    {reply, {error, badarg}, State}.

handle_cast(_Msg, State) ->
    {noreply, State}.

handle_info(retry_now,
            State=#state{host = Host,
                         max_age = MaxAge,
                         retry_interval = RetryInterval}) ->
    try_delete_old_messages_and_wait(Host, MaxAge, RetryInterval),
    {noreply, State};
handle_info({delete_now, NextId},
            State=#state{host = Host,
                         retry_interval = RetryInterval}) ->
    delete_old_messages_and_wait(Host, NextId, RetryInterval),
    {noreply, State};
handle_info(_Info, State) ->
    {noreply, State}.

code_change(_OldVsn, State, _Extra) ->
    {ok, State}.

terminate(_Reason, #state{}) ->
    ok.

%%====================================================================
%% Internal
%%====================================================================

maximum_row_to_delete_next(Host) ->
    Res = ejabberd_odbc:sql_query(Host, maximum_row_to_delete_next_sql()),
    case Res of
        {selected, [<<"id">>, <<"timestamp">>], [{BId, BTimeStamp}]} ->
            Id = bin_to_int(BId),
            TimeStamp = micro_to_milliseconds(bin_to_int(BTimeStamp)),
            {ok, {Id, TimeStamp}};
        {selected, _, []} ->
            %% Table contains less than 5000 records
            {error, not_enough};
        {error, Reason} ->
            ?DEBUG("SQL error ~p", [Reason]),
            {error, Reason}
    end.

maximum_row_to_delete_next_sql() ->
    <<"SELECT id, timestamp FROM offline_message "
      "ORDER BY id LIMIT 1 OFFSET 5000">>.

delete_old_messages(Host, Id) ->
    ejabberd_odbc:sql_query(Host, delete_old_messages_sql(Id)).

delete_old_messages_sql(Id) ->
    [<<"DELETE FROM offline_message "
       "WHERE id <= ">>, integer_to_list(Id)].


try_delete_old_messages_and_wait(Host, MaxAge, RetryInterval) ->
    CurTimestamp = now_to_milliseconds(now()),
    case maximum_row_to_delete_next(Host) of
        {ok, {NextId, NextTimeStamp}} ->
            case select_strategy(CurTimestamp, NextTimeStamp, MaxAge) of
                {wait, Delay} ->
                    delete_old_messages_after(Host, Delay, NextId);
                delete_now ->
                    delete_old_messages_and_wait(Host, NextId, RetryInterval)
            end;
        {error, _} ->
            retry_after(Host, RetryInterval)
    end.

delete_old_messages_and_wait(Host, NextId, RetryInterval) ->
    delete_old_messages(Host, NextId),
    retry_after(Host, RetryInterval).

delete_old_messages_after(_Host, Delay, NextId) ->
    erlang:send_after(Delay, self(), {delete_now, NextId}).

retry_after(_Host, Delay) ->
    erlang:send_after(Delay, self(), retry_now).

select_strategy(CurTimestamp, NextTimeStamp, MaxAge)
    %% Too young to die
    when (CurTimestamp - NextTimeStamp) < MaxAge ->
    {wait, CurTimestamp - NextTimeStamp - MaxAge};
select_strategy(_CurTimestamp, _NextTimeStamp, _MaxAge) ->
    delete_now.

bin_to_int(B) ->
    list_to_integer(binary_to_list(B)).

now_to_microseconds({Mega, Secs, Micro}) ->
    (1000000 * Mega + Secs) * 1000000 + Micro.

now_to_milliseconds(Now) ->
    micro_to_milliseconds(now_to_microseconds(Now)).

micro_to_milliseconds(Microseconds) ->
    Microseconds div 1000.
