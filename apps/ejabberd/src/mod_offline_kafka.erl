%%%----------------------------------------------------------------------
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

-module(mod_offline_kafka).
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

-record(offline_msg_offset, {us, offset}).

init(Host, Opts) ->
    mnesia:create_table(offline_msg_offset,
            [{disc_copies, [node()]},
             {type, set},
             {attributes, record_info(fields, offline_msg_offset)}]),
    start_workers(Host),
    mod_offline_kafka_writer:start(Host, Opts),
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
    list_to_atom("mod_offline_" ++ SHost ++ "_" ++ SN).


%% API
%% ------------------------------------------------------------------

pop_messages(LUser, LServer) ->
    US = {LUser, LServer},
    To = jlib:make_jid(LUser, LServer, <<>>),
    Topic   = make_topic(LUser, LServer),
    Broker  = 0,
    Part    = 0,
    Offset = get_offset(US),
    Server = choose_server(LServer, LUser),
    MaxSize = 65535,
    case fetch_request(Server, Topic, Part, Offset, MaxSize) of
        {ok, []} ->
            {ok, []};
        {ok, {Bins, Size}} ->
            set_offset(US, Offset + Size),
            {ok, binaries_to_records(US, To, Bins)};
        {error, Reason} ->
            ?ERROR_MSG("Fetch for ~p with offset ~p failed with reason ~p",
                [Topic, Offset, Reason]),
            {error, kafka_error}
    end.

fetch_request(Server, Topic, Part, Offset, MaxSize) ->
    gen_server:call(Server, {fetch_request, {Topic, Part, Offset, MaxSize}}).

get_offset(US) ->
    case mnesia:dirty_read(offline_msg_offset, US) of
        [#offline_msg_offset{offset = Offset}] ->
            Offset;
        [] ->
            0
    end.

set_offset(US, Offset) ->
    mnesia:dirty_write(#offline_msg_offset{us = US, offset = Offset}).

binaries_to_records(US, To, Bins) ->
    [binary_to_record(US, To, Bin) || Bin <- Bins].

binary_to_record(US, To, Bin) ->
    term_to_record(US, To, binary_to_term(Bin)).

term_to_record(US, To, {TimeStamp, From, Packet}) ->
    #offline_msg{us = US,
             timestamp = TimeStamp,
             expire = undefined,
             from = From,
             to = To,
             packet = Packet}.


write_messages(LUser, LServer, Msgs, MaxOfflineMsgs) ->
    case is_message_count_threshold_reached(
                 LUser, LServer, Msgs, MaxOfflineMsgs) of
        false ->
            write_all_messages_t(LUser, LServer, Msgs);
        true ->
            discard_all_messages_t(Msgs)
    end.

write_all_messages_t(LUser, LServer, Msgs) ->
    Topic   = make_topic(LUser, LServer),
    Broker  = 0,
    Part    = 0,
    Bins = [record_to_binary(Msg) || Msg <- Msgs],
    mod_offline_kafka_writer:produce(LUser, LServer, Broker, Topic, Part, Bins).

make_topic(LUser, LServer) ->
    make_binary_jid(LUser, LServer).

make_binary_jid(LUser, LServer) ->
    <<LUser/binary, "@", LServer/binary>>.

%% Ignore the expire field.
record_to_binary(#offline_msg{
        from = From, packet = Packet, timestamp = TimeStamp}) ->
    term_to_binary({TimeStamp, From, Packet}).

discard_all_messages_t(Msgs) ->
    {discarded, Msgs, []}.

is_message_count_threshold_reached(_LUser, _LServer, _Msgs, _MaxOfflineMsgs) ->
    %% TODO
    false.

%% Not supported by Kafka.
%% Messages will be deleted by timeout.
remove_user(_LUser, _LServer) ->
    ok.

remove_expired_messages(_LServer) ->
    ok.

remove_old_messages(_LServer, _Days) ->
    ok.

%%====================================================================
%% Internal functions
%%====================================================================

run_flush(State=#state{acc=[]}) ->
    State;
run_flush(State=#state{conn=Conn, flush_interval_tref=TRef, acc=Acc}) ->
    ?DEBUG("Multifetch ~p topics.", [length(Acc)]),
    {Clients, TopicPartitionOffsets} = lists:unzip(Acc),
    TRef =/= undefined andalso erlang:cancel_timer(TRef),
    {ok, Messages} = kafka_simple_api:multi_fetch(Conn, TopicPartitionOffsets),
    [gen_server:reply(From, {ok, Message})
     || {From, Message} <- lists:zip(Clients, Messages)],
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
    {ok, Conn} = kafka_server:start_link(['10.100.0.43', 9092]),
    {ok, #state{host=Host, conn=Conn}}.

%%--------------------------------------------------------------------
%% Function: %% handle_call(Request, From, State) -> {reply, Reply, State} |
%%                                      {reply, Reply, State, Timeout} |
%%                                      {noreply, State} |
%%                                      {noreply, State, Timeout} |
%%                                      {stop, Reason, Reply, State} |
%%                                      {stop, Reason, State}
%% Description: Handling call messages
%%--------------------------------------------------------------------
handle_call({fetch_request, TopicPartitionOffset}, From,
            State=#state{acc=Acc, flush_interval_tref=TRef, flush_interval=Int,
                         max_packet_size=Max}) ->
    TRef2 = case {Acc, TRef} of
            {[], undefined} -> erlang:send_after(Int, self(), flush);
            {_, _} -> TRef
            end,
    Req = {From, TopicPartitionOffset},
    State2 = State#state{acc=[Req|Acc], flush_interval_tref=TRef2},
    case length(Acc) + 1 >= Max of
        true -> {noreply, run_flush(State2)};
        false -> {noreply, State2}
    end.

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

