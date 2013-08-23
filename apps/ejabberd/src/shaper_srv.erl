-module(shaper_srv).
-behaviour(gen_server).
-define(SERVER, ?MODULE).

%% ------------------------------------------------------------------
%% API Function Exports
%% ------------------------------------------------------------------

-export([start_link/0,
         wait/4]).

%% ------------------------------------------------------------------
%% Record definitions
%% ------------------------------------------------------------------

-record(state, {
        max_delay :: non_neg_integer(),
        shapers :: dict()
    }).

%% ------------------------------------------------------------------
%% gen_server Function Exports
%% ------------------------------------------------------------------

-export([init/1, handle_call/3, handle_cast/2, handle_info/2,
         terminate/2, code_change/3]).

%% ------------------------------------------------------------------
%% API Function Definitions
%% ------------------------------------------------------------------

start_link() ->
    gen_server:start_link({local, ?SERVER}, ?MODULE, [], []).

srv(_Host) -> ?SERVER.

-spec wait(_Host, _Action, _FromJID, _Size) -> ok | {error, max_delay_reached}.
wait(Host, Action, FromJID, Size) ->
    gen_server:call(srv(Host), {wait, Host, Action, FromJID, Size}).

%% ------------------------------------------------------------------
%% gen_server Function Definitions
%% ------------------------------------------------------------------

init(Args) ->
    State = #state{
        shapers = dict:new(),
        max_delay = proplists:get_value(max_delay, Args, 3000)
    },
    {ok, State}.

handle_call({wait, Host, Action, FromJID, Size},
            FromRef, State=#state{shapers=Shapers, max_delay=MaxDelayMs}) ->
    Key = new_key(Host, Action, FromJID),
    Shaper = find_or_create_shaper(Key, Shapers),
    case shaper:update(Shaper, Size) of
        {Shaper, 0} ->
            {reply, ok, State};
        {UpdatedShaper, 0} ->
            NewShapers = dict:store(Key, UpdatedShaper, Shapers),
            NewState = #state{shapers=NewShapers},
            {reply, ok, NewState};
        {UpdatedShaper, DelayMs} when DelayMs < MaxDelayMs ->
            NewShapers = dict:store(Key, UpdatedShaper, Shapers),
            reply_after(DelayMs, FromRef),
            NewState = #state{shapers=NewShapers},
            {noreply, NewState};
        {_, _} ->
            {reply, {error, max_delay_reached}, State}
    end.

handle_cast(_Msg, State) ->
    {noreply, State}.

handle_info(_Info, State) ->
    {noreply, State}.

terminate(_Reason, _State) ->
    ok.

code_change(_OldVsn, State, _Extra) ->
    {ok, State}.

%% ------------------------------------------------------------------
%% Internal Function Definitions
%% ------------------------------------------------------------------

new_key(Host, Action, FromJID) ->
    {Host, Action, FromJID}.

find_or_create_shaper(Key, Shapers) ->
    case dict:find(Key, Shapers) of
        {ok, Shaper} -> Shaper;
        error -> create_shaper(Key)
    end.

create_shaper(Key) ->
    shaper:new(request_shaper_name(Key)).

request_shaper_name({Host, Action, FromJID}) ->
    get_shaper_name(Host, Action, FromJID, default_shaper()).

default_shaper() ->
    none.

get_shaper_name(Host, Action, FromJID, Default) ->
    case acl:match_rule(Host, Action, FromJID) of
        deny -> Default;
        Value -> Value
    end.

reply_after(DelayMs, FromJID) ->
    timer:apply_after(DelayMs, gen_server, reply, [FromJID, ok]).
