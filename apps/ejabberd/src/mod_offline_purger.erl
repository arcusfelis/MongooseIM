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
-behaviour(locks_leader).

-include("ejabberd.hrl").
-include("jlib.hrl").

-define(SUPERVISOR, ejabberd_sup).

%% API
-export([start_link/2,
         next_purge/1]).

%% gen_mod callbacks
-export([start/2, stop/1]).

-export([init/1,
         elected/3,
         surrendered/3,
         handle_DOWN/3,
         handle_leader_call/4,
         handle_leader_cast/3,
         from_leader/3,
         handle_call/4,
         handle_cast/3,
         handle_info/3,
         terminate/2,
         code_change/4]).

-record(state, {host,
                max_age,
                page_size,
                retry_tref,
                retry_interval,
                purge_interval}).

default_max_age() ->
    %% 3 weeks
    timer:hours(21 * 24).

default_retry_interval() ->
    timer:minutes(1).

default_page_size() ->
    5000.

%%====================================================================
%% API
%%====================================================================
start_link(Host, Opts) ->
    Proc = gen_mod:get_module_proc(Host, ?MODULE),
    locks_leader:start_link(Proc, ?MODULE, [Host, Opts], []).

next_purge(Host) ->
    Proc = gen_mod:get_module_proc(Host, ?MODULE),
    locks_leader:leader_call(Proc, next_purge).

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
    PageSize = gen_mod:get_opt(page_size, Opts, default_page_size()),
    RetryInterval = gen_mod:get_opt(retry_interval, Opts, default_retry_interval()),
    PurgeInterval = gen_mod:get_opt(purge_interval, Opts, RetryInterval),
    {ok, #state{host = Host,
                max_age = MaxAge,
                page_size = PageSize,
                purge_interval = PurgeInterval,
                retry_interval = RetryInterval}}.

elected(State=#state{retry_tref = undefined,
                     host = Host, retry_interval = RetryInterval}, _I, _Cand) ->
    %% Start counting
    ?INFO_MSG("Become a purging leader", []),
    RetryTRef = retry_after(Host, RetryInterval),
    {ok, sync, State#state{retry_tref = RetryTRef}};
elected(State, _I, _Cand) ->
    %% Already started
    {ok, sync, State}.

surrendered(State=#state{retry_tref = undefined}, sync, _I) ->
    {ok, State};
surrendered(State=#state{retry_tref = RetryTRef}, sync, _I) ->
    %% Stop counting
    ?INFO_MSG("Stop being a purging leader", []),
    erlang:cancel_timer(RetryTRef),
    {ok, State#state{retry_tref = undefined}}.

handle_DOWN(_Pid, State, _I) ->
    {ok, State}.

handle_leader_call(next_purge, _From, #state{retry_tref = RetryTRef} = State, _I) ->
    TimerInfo = timer_info(RetryTRef),
    {reply, {node(), TimerInfo}, State};
handle_leader_call(_Msg, _From, #state{} = State, _I) ->
    ?WARNING_MSG("Unexpected message ~p from ~p", [_Msg, _From]),
    {reply, ok, State}.

handle_leader_cast(_Msg, #state{} = State, _I) ->
    ?WARNING_MSG("Unexpected async message ~p", [_Msg]),
    {ok, State}.

from_leader(sync, #state{} = State, _I) ->
    {ok, State}.

handle_call(stop, _From, State, _I) ->
    {stop, normal, ok, State};
handle_call(_Req, _From, State, _I) ->
    {reply, {error, badarg}, State}.

handle_cast(_Msg, State, _I) ->
    {noreply, State}.

handle_info(retry_now,
            State=#state{host = Host,
                         max_age = MaxAge,
                         page_size = PageSize,
                         retry_interval = RetryInterval,
                         purge_interval = PurgeInterval}, _I) ->
    RetryTRef = try_delete_old_messages_and_wait(Host, MaxAge,
        RetryInterval, PurgeInterval, PageSize),
    {noreply, State#state{retry_tref = RetryTRef}};
handle_info({delete_now, NextId},
            State=#state{host = Host,
                         purge_interval = PurgeInterval}, _I) ->
    RetryTRef = delete_old_messages_and_wait(Host, NextId, PurgeInterval),
    {noreply, State#state{retry_tref = RetryTRef}};
handle_info(_Info, State, _I) ->
    {noreply, State}.

code_change(_OldVsn, State, _I, _Extra) ->
    {ok, State}.

terminate(_Reason, #state{}) ->
    ok.

%%====================================================================
%% Internal
%%====================================================================

maximum_row_to_delete_next(Host, PageSize) ->
    Res = ejabberd_odbc:sql_query(Host, maximum_row_to_delete_next_sql(PageSize)),
    case Res of
        {selected, [<<"id">>, <<"timestamp">>], [{BId, BTimeStamp}]} ->
            Id = bin_to_int(BId),
            TimeStamp = micro_to_milliseconds(bin_to_int(BTimeStamp)),
            {ok, {Id, TimeStamp}};
        {selected, _, []} ->
            %% Table contains less than 5000 records
            ?INFO_MSG("Not enough messages. Do nothing", []),
            {error, not_enough};
        {error, Reason} ->
            ?DEBUG("SQL error ~p", [Reason]),
            {error, Reason}
    end.

maximum_row_to_delete_next_sql(PageSize) ->
    [<<"SELECT id, timestamp FROM offline_message "
       "ORDER BY id LIMIT 1 OFFSET ">>, integer_to_list(PageSize)].

delete_old_messages(Host, Id) ->
    ejabberd_odbc:sql_query(Host, delete_old_messages_sql(Id)).

delete_old_messages_sql(Id) ->
    [<<"DELETE FROM offline_message "
       "WHERE id <= ">>, integer_to_list(Id)].


try_delete_old_messages_and_wait(Host, MaxAge, RetryInterval, PurgeInterval, PageSize) ->
    CurTimestamp = now_to_milliseconds(now()),
    case maximum_row_to_delete_next(Host, PageSize) of
        {ok, {NextId, NextTimeStamp}} ->
            case select_strategy(CurTimestamp, NextTimeStamp, MaxAge) of
                {wait, Delay} ->
                    delete_old_messages_after(Host, Delay, NextId);
                delete_now ->
                    delete_old_messages_and_wait(Host, NextId, PurgeInterval)
            end;
        {error, _} ->
            retry_after(Host, RetryInterval)
    end.

delete_old_messages_and_wait(Host, NextId, PurgeInterval) ->
    ?INFO_MSG("Deleting old messages up to ~p", [NextId]),
    delete_old_messages(Host, NextId),
    retry_after(Host, PurgeInterval).

delete_old_messages_after(_Host, Delay, NextId) ->
    ?INFO_MSG("Wait ~p to delete old messages up to ~p", [Delay, NextId]),
    erlang:send_after(Delay, self(), {delete_now, NextId}).

retry_after(_Host, Delay) ->
    erlang:send_after(Delay, self(), retry_now).

select_strategy(CurTimestamp, NextTimeStamp, MaxAge)
    %% Too young to die
    when (CurTimestamp - NextTimeStamp) < MaxAge ->
    {wait, MaxAge - CurTimestamp + NextTimeStamp};
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

timer_info(undefined) ->
    undefined;
timer_info(RetryTRef) ->
    erlang:read_timer(RetryTRef).
