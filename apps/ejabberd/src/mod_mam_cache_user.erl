%%%-------------------------------------------------------------------
%%% @author Uvarov Michael <arcusfelis@gmail.com>
%%% @copyright (C) 2013, Uvarov Michael
%%% @doc Stores cache using ETS-table.
%%% This module is a proxy for `mod_mam_odbc_user'.
%%% 
%%% This module is a tuple module (not parametrized).
%%% @end
%%%-------------------------------------------------------------------
-module(mod_mam_cache_user).
-export([start/2,
         start_link/0,
         required_modules/2,
         archive_id/3,
         clean_cache/2,
         remove_archive/4]).


%% gen_server callbacks
-export([init/1, handle_call/3, handle_cast/2, handle_info/2,
         terminate/2, code_change/3]).

%-type module() :: atom() | {atom(), term()}.
-type hidden_state() :: {atom(), atom()}.

-include("ejabberd.hrl").
-include("jlib.hrl").

-record(state, {}).

%% @private
srv_name() ->
    mod_mam_cache.

tbl_name_archive_id() ->
    mod_mam_cache_table_archive_id.

group_name() ->
    mod_mam_cache.

su_key(LServer, LUserName) ->
    {LServer, LUserName}.

%%====================================================================
%% API
%%====================================================================

required_modules(LServer, HiddenState) ->
    [user_base_module(LServer, HiddenState)].

start(_Host, _HiddenState) ->
    WriterChildSpec =
    {mod_mam_cache_user,
     {mod_mam_cache_user, start_link, []},
     permanent,
     5000,
     worker,
     [mod_mam_cache_user]},
    supervisor:start_child(ejabberd_sup, WriterChildSpec).

start_link() ->
    gen_server:start_link({local, srv_name()}, ?MODULE, [], []).

archive_id(LServer, UserName, HiddenState) ->
    case lookup_archive_id(LServer, UserName) of
        not_found ->
            UserId = forward_archive_id(LServer, UserName, HiddenState),
            cache_archive_id(LServer, UserName, UserId),
            UserId;
        UserId ->
            UserId
    end.

remove_archive(LServer, UserName, UserId, HiddenState) ->
    clean_cache(LServer, UserName),
    forward_remove_archive(LServer, UserName, UserId, HiddenState).

%%====================================================================
%% Internal functions
%%====================================================================

%% @doc Put an user id into cache.
%% @private
cache_archive_id(LServer, UserName, UserId) ->
    gen_server:call(srv_name(), {cache_archive_id, LServer, UserName, UserId}).

lookup_archive_id(LServer, UserName) ->
    try
        ets:lookup_element(tbl_name_archive_id(), su_key(LServer, UserName), 2)
    catch error:badarg ->
        not_found
    end.

-spec user_base_module(Host :: binary(), hidden_state()) -> module().
user_base_module(_Host, {?MODULE, BaseMod}) ->
    BaseMod.

forward_archive_id(LServer, UserName, HiddenState) ->
    M = user_base_module(LServer, HiddenState),
    M:archive_id(LServer, UserName).

forward_remove_archive(LServer, UserName, UserId, HiddenState) ->
    M = user_base_module(LServer, HiddenState),
    M:remove_archive(LServer, UserName, UserId).

clean_cache(Server, User) ->
    %% Send a broadcast message.
    case pg2:get_members(group_name()) of
        Pids when is_list(Pids) ->
            [gen_server:cast(Pid, {remove_user, User, Server})
            || Pid <- Pids],
            ok;
        {error, _Reason} -> ok
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
init([]) ->
    pg2:create(group_name()),
    pg2:join(group_name(), self()),
    TOpts = [named_table, protected,
             {write_concurrency, false},
             {read_concurrency, true}],
    ets:new(tbl_name_archive_id(), TOpts),
    {ok, #state{}}.

%%--------------------------------------------------------------------
%% Function: %% handle_call(Request, From, State) -> {reply, Reply, State} |
%%                                      {reply, Reply, State, Timeout} |
%%                                      {noreply, State} |
%%                                      {noreply, State, Timeout} |
%%                                      {stop, Reason, Reply, State} |
%%                                      {stop, Reason, State}
%% Description: Handling call messages
%%--------------------------------------------------------------------
handle_call({cache_archive_id, LServer, UserName, UserId}, _From, State) ->
    ets:insert(tbl_name_archive_id(), {su_key(LServer, UserName), UserId}),
    {reply, ok, State}.


%%--------------------------------------------------------------------
%% Function: handle_cast(Msg, State) -> {noreply, State} |
%%                                      {noreply, State, Timeout} |
%%                                      {stop, Reason, State}
%% Description: Handling cast messages
%%--------------------------------------------------------------------

handle_cast({remove_user, User, Server}, State) ->
    ets:delete(tbl_name_archive_id(), su_key(Server, User)),
    {noreply, State};
handle_cast(Msg, State) ->
    ?WARNING_MSG("Strange message ~p.", [Msg]),
    {noreply, State}.

%%--------------------------------------------------------------------
%% Function: handle_info(Info, State) -> {noreply, State} |
%%                                       {noreply, State, Timeout} |
%%                                       {stop, Reason, State}
%% Description: Handling all non call/cast messages
%%--------------------------------------------------------------------

handle_info(_Msg, State) ->
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

