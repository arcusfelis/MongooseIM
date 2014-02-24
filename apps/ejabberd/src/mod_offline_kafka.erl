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

-include("ejabberd.hrl").
-include("jlib.hrl").
-include("mod_offline.hrl").

-define(OFFLINE_TABLE_LOCK_THRESHOLD, 1000).

-record(offline_msg_offsets, {us, offset}).

init(_Host, Opts) ->
    %% Servers will not be stopped with module.
    Servers = gen_mod:get_opt(kafka_servers, Opts, [{0, '127.0.0.1', 9092}]),
    kafka_server_sup:start_link(Servers),
    mnesia:create_table(offline_msg_offsets,
            [{disc_copies, [node()]},
             {type, bag},
             {attributes, record_info(fields, offline_msg_offsets)}]),
    ok.

pop_messages(LUser, LServer) ->
    US = {LUser, LServer},
    To = jlib:make_jid(LUser, LServer, <<>>),
    Topic   = make_topic(LUser, LServer),
    Broker  = 0,
    Part    = 0,
    Offset = get_offset(US),
    try kafka_simple_api:fetch(Broker, Topic, Part, Offset) of
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

get_offset(US) ->
    case mnesia:dirty_read(offline_msg_offsets, US) of
        [#offline_msg_offsets{offset = Offset}] ->
            Offset;
        [] ->
            0
    end.

set_offset(US, Offset) ->
    mnesia:dirty_write(#offline_msg_offsets{us = US, offset = Offset}).

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
    kafka_simple_api:produce(Broker, Topic, Part, Bins).

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
