%%%----------------------------------------------------------------------
%%% Buffered writer
%%%
%%% File    : mod_offline_odbc.erl
%%% Author  : Alexey Shchepin <alexey@process-one.net>
%%% Purpose : Store and manage offline messages in relational database.
%%% Created :  5 Jan 2003 by Alexey Shchepin <alexey@process-one.net>
%%%
%%%
%%% ejabberd, Copyright (C) 2002-2011   ProcessOne
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

-module(mod_offline_odbc_async).
-behaviour(mod_offline).

-export([init/2,
         pop_messages/2,
         write_messages/4,
         remove_expired_messages/1,
         remove_old_messages/2,
         remove_user/2]).

%% Internal exports
-export([start_link/3]).

%% gen_server callbacks
-export([init/1, handle_call/3, handle_cast/2, handle_info/2,
         terminate/2, code_change/3]).

%% For debugging
-export([queue_length/1,
         queue_lengths/1]).

-include("ejabberd.hrl").
-include("jlib.hrl").
-include("mod_offline.hrl").

-record(state, {
    flush_interval=500,
    max_packet_size=30,
    max_subscribers=100,
    host,
    conn,
    number,
    acc=[],
    subscribers=[],
    flush_interval_tref}).

worker_prefix() ->
    "ejabberd_mod_offline_writer".

worker_count(_Host) ->
    16.

worker_names(Host) ->
    [{N, worker_name(Host, N)} || N <- lists:seq(0, worker_count(Host) - 1)].

worker_name(Host, N) ->
    list_to_atom(worker_prefix() ++ "_" ++ binary_to_list(Host) ++ "_" ++ integer_to_list(N)).

select_worker(Host, LUser) ->
    N = worker_number(Host, LUser),
    worker_name(Host, N).

%% 0 .. N - 1
worker_number(Host, LUser) ->
    erlang:phash2(LUser, worker_count(Host)).

-define(OFFLINE_TABLE_LOCK_THRESHOLD, 1000).

init(Host, _Opts) ->
    start_workers(Host),
    ok.

pop_messages(LUser, LServer) ->
    US = {LUser, LServer},
    To = jlib:make_jid(LUser, LServer, <<>>),
    SUser = ejabberd_odbc:escape(LUser),
    SServer = ejabberd_odbc:escape(LServer),
    TimeStamp = now(),
    STimeStamp = encode_timestamp(TimeStamp),
    case odbc_queries:pop_offline_messages(LServer, SUser, SServer, STimeStamp) of
        {atomic, {selected, [<<"timestamp">>,<<"from_jid">>,<<"packet">>], Rows}} ->
            {ok, rows_to_records(US, To, Rows)};
        {aborted, Reason} ->
            {error, Reason};
        {error, Reason} ->
            {error, Reason}
    end.

rows_to_records(US, To, Rows) ->
    [row_to_record(US, To, Row) || Row <- Rows].

row_to_record(US, To, {STimeStamp, SFrom, SPacket}) ->
    Packet = xml_stream:parse_element(SPacket),
    TimeStamp = microseconds_to_now(list_to_integer(binary_to_list(STimeStamp))),
    From = jlib:binary_to_jid(SFrom),
    #offline_msg{us = US,
             timestamp = TimeStamp,
             expire = undefined,
             from = From,
             to = To,
             packet = Packet}.


write_messages(LUser, LServer, Msgs, MaxOfflineMsgs) ->
    SUser = ejabberd_odbc:escape(LUser),
    SServer = ejabberd_odbc:escape(LServer),
    write_messages_t(LServer, LUser, SUser, SServer, Msgs, MaxOfflineMsgs).

write_messages_t(LServer, LUser, SUser, SServer, Msgs, MaxOfflineMsgs) ->
    case is_message_count_threshold_reached(
                 LServer, SUser, SServer, Msgs, MaxOfflineMsgs) of
        false ->
            write_all_messages_t(LServer, LUser, SUser, SServer, Msgs);
        true ->
            discard_all_messages_t(Msgs)
    end.

is_message_count_threshold_reached(LServer, SUser, SServer, Msgs, MaxOfflineMsgs) ->
    Len = length(Msgs),
    case MaxOfflineMsgs of
        infinity ->
            false;
        MaxOfflineMsgs when Len > MaxOfflineMsgs ->
            true;
        MaxOfflineMsgs ->
            %% Only count messages if needed.
            MaxArchivedMsg = MaxOfflineMsgs - Len,
            %% Do not need to count all messages in archive.
            MaxOfflineMsgs < count_offline_messages(
                LServer, SUser, SServer, MaxArchivedMsg + 1)
    end.

write_all_messages_t(LServer, LUser, SUser, SServer, Msgs) ->
    Rows = [record_to_row(SUser, SServer, Msg) || Msg <- Msgs],
    Worker = select_worker(LServer, LUser),
    gen_server:cast(Worker, {archive_messages, Rows}),
    ok.

record_to_row(SUser, SServer, #offline_msg{
        from = From, packet = Packet, timestamp = TimeStamp, expire = Expire}) ->
    SFrom = ejabberd_odbc:escape(jlib:jid_to_binary(From)),
    SPacket = ejabberd_odbc:escape(xml:element_to_binary(Packet)),
    STimeStamp = encode_timestamp(TimeStamp),
    SExpire = maybe_encode_timestamp(Expire),
    odbc_queries:prepare_offline_message(SUser, SServer, STimeStamp, SExpire, SFrom, SPacket).

discard_all_messages_t(Msgs) ->
    {discarded, Msgs, []}.

remove_user(LUser, LServer) ->
    SUser   = ejabberd_odbc:escape(LUser),
    SServer = ejabberd_odbc:escape(LServer),
    odbc_queries:remove_offline_messages(LServer, SUser, SServer).

remove_expired_messages(LServer) ->
    TimeStamp = now(),
    STimeStamp = encode_timestamp(TimeStamp),
    odbc_queries:remove_expired_offline_messages(LServer, STimeStamp).

remove_old_messages(LServer, Days) ->
    TimeStamp = fallback_timestamp(Days, now()),
    STimeStamp = encode_timestamp(TimeStamp),
    odbc_queries:remove_old_offline_messages(LServer, STimeStamp).

count_offline_messages(LServer, SUser, SServer, Limit) ->
    case odbc_queries:count_offline_messages(LServer, SUser, SServer, Limit) of
        {selected, [_], [{Count}]} ->
            list_to_integer(binary_to_list(Count));
        Error ->
            ?ERROR_MSG("count_offline_messages failed ~p", [Error]),
            0
    end.

fallback_timestamp(Days, {MegaSecs, Secs, _MicroSecs}) ->
    S = MegaSecs * 1000000 + Secs - 60 * 60 * 24 * Days,
    MegaSecs1 = S div 1000000,
    Secs1 = S rem 1000000,
    {MegaSecs1, Secs1, 0}.

encode_timestamp(TimeStamp) ->
    integer_to_list(now_to_microseconds(TimeStamp)).

maybe_encode_timestamp(never) ->
    "null";
maybe_encode_timestamp(TimeStamp) ->
    encode_timestamp(TimeStamp).

now_to_microseconds({Mega, Secs, Micro}) ->
    (1000000 * Mega + Secs) * 1000000 + Micro.

microseconds_to_now(MicroSeconds) when is_integer(MicroSeconds) ->
    Seconds = MicroSeconds div 1000000,
    {Seconds div 1000000, Seconds rem 1000000, MicroSeconds rem 1000000}.



%%====================================================================
%% API
%%====================================================================

start_workers(Host) ->
    [start_worker(WriterProc, N, Host)
     || {N, WriterProc} <- worker_names(Host)].

stop_workers(Host) ->
    [stop_worker(WriterProc) ||  {_, WriterProc} <- worker_names(Host)].

start_worker(WriterProc, N, Host) ->
    WriterChildSpec =
    {WriterProc,
     {?MODULE, start_link, [WriterProc, N, Host]},
     permanent,
     5000,
     worker,
     [?MODULE]},
    supervisor:start_child(ejabberd_sup, WriterChildSpec).

stop_worker(Proc) ->
    supervisor:terminate_child(ejabberd_sup, Proc),
    supervisor:delete_child(ejabberd_sup, Proc).


start_link(ProcName, N, Host) ->
    gen_server:start_link({local, ProcName}, ?MODULE, [Host, N], []).

%% For folsom.
queue_length(Host) ->
    Len = lists:sum(queue_lengths(Host)),
    {ok, Len}.

queue_lengths(Host) ->
    [worker_queue_length(SrvName) || {_, SrvName} <- worker_names(Host)].

worker_queue_length(SrvName) ->
    case whereis(SrvName) of
    undefined ->
        0;
    Pid ->
        {message_queue_len, Len} = erlang:process_info(Pid, message_queue_len),
        Len
    end.


%%====================================================================
%% Internal functions
%%====================================================================

run_flush(State=#state{acc=[]}) ->
    State;
run_flush(State=#state{host=Host, conn=Conn, number=N,
                       flush_interval_tref=TRef, acc=Acc, subscribers=Subs}) ->
    MessageCount = length(Acc),
    cancel_and_flush_timer(TRef),
    ?DEBUG("Flushed ~p entries.", [MessageCount]),
    Result = odbc_queries:push_offline_messages(Conn, Acc),
    case Result of
        {updated, _Count} -> ok;
        {error, Reason} ->
            ejabberd_hooks:run(drop_offline_messages, Host, [Host, MessageCount]),
            ?ERROR_MSG("archive_message query failed with reason ~p", [Reason]),
            ok
    end,
    spawn_link(fun() ->
            [gen_server:reply(Sub, ok) || Sub <- Subs],
            ejabberd_hooks:run(flush_offline_messages, Host, [Host, MessageCount])
        end),
    erlang:garbage_collect(),
    State#state{acc=[], subscribers=[], flush_interval_tref=undefined}.


cancel_and_flush_timer(undefined) ->
    ok;
cancel_and_flush_timer(TRef) ->
    case erlang:cancel_timer(TRef) of
        false ->
            receive
                flush -> ok
            after 0 -> ok
            end;
        _ -> ok
    end.

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
init([Host, N]) ->
    %% Use a private ODBC-connection.
    {ok, Conn} = ejabberd_odbc:get_dedicated_connection(Host),
    {ok, #state{host=Host, conn=Conn, number=N}}.

%%--------------------------------------------------------------------
%% Function: %% handle_call(Request, From, State) -> {reply, Reply, State} |
%%                                      {reply, Reply, State, Timeout} |
%%                                      {noreply, State} |
%%                                      {noreply, State, Timeout} |
%%                                      {stop, Reason, Reply, State} |
%%                                      {stop, Reason, State}
%% Description: Handling call messages
%%--------------------------------------------------------------------
handle_call(_, _From, State=#state{acc=[]}) ->
    {reply, ok, State}.

%%--------------------------------------------------------------------
%% Function: handle_cast(Msg, State) -> {noreply, State} |
%%                                      {noreply, State, Timeout} |
%%                                      {stop, Reason, State}
%% Description: Handling cast messages
%%--------------------------------------------------------------------

handle_cast({archive_messages, Rows},
            State=#state{acc=Acc, flush_interval_tref=TRef, flush_interval=Int,
                         max_packet_size=Max}) ->
    Count = length(Rows),
    TRef2 = case {Acc, TRef} of
            {[], undefined} -> erlang:send_after(Int, self(), flush);
            {_, _} -> TRef
            end,
    State2 = State#state{acc=Rows ++ Acc, flush_interval_tref=TRef2},
    case length(Acc) + Count >= Max of
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
    {noreply, run_flush(State#state{flush_interval_tref=undefined})}.

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

