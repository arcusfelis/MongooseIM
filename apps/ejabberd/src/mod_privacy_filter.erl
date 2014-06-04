%%%-------------------------------------------------------------------
%%% @author Uvarov Michael <arcusfelis@gmail.com>
%%% @copyright (C) 2014, Erlang Solutions LTD.
%%% @doc Filter all messages in a single place
%%% @end
%%%-------------------------------------------------------------------
-module(mod_privacy_filter).
-behavior(gen_mod).

%% ----------------------------------------------------------------------
%% Exports

%% gen_mod handlers
-export([start/2, stop/1]).

%% ejabberd handlers
-export([filter_packet/1]).


%% ----------------------------------------------------------------------
%% gen_mod callbacks

-spec start(Host :: ejabberd:server(), Opts :: list()) -> any().
start(Host, Opts) ->
    ejabberd_hooks:add(filter_local_packet, Host, ?MODULE, filter_packet, 80),
    ok.


-spec stop(Host :: ejabberd:server()) -> any().
stop(Host) ->
    ejabberd_hooks:delete(filter_local_packet, Host, ?MODULE, filter_packet, 80),
    ok.

%% ----------------------------------------------------------------------
%% hooks and handlers

%% @doc Handle an incoming message.
-type fpacket() :: {From :: ejabberd:jid(),
                    To :: ejabberd:jid(),
                    Packet :: jlib:xmlel()}.
-spec filter_packet(Value :: fpacket() | drop) -> fpacket() | drop.
filter_packet(drop) ->
    drop;
filter_packet({From, To, Packet}) ->
    case ejabberd_sm:is_privacy_allow(From, To, Packet) of
        true ->
            {From, To, Packet};
        false ->
            drop
    end.
