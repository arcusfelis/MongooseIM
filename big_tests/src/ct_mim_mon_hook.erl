%%% @doc Checks that mongoose nodes are still running
-module(ct_mim_mon_hook).

%% Callbacks
 -export([id/1]).
 -export([init/2]).

 -export([post_init_per_suite/4]).

 -export([terminate/1]).

 -record(state, { nodes }).

 %% Return a unique id for this CTH.
 id(Opts) ->
   "ct_mim_mon_hook".

 %% Always called before any other callback function. Use this to initiate
 %% any common state. 
 init(Id, Opts) ->
     {ok, #state{}}.

 %% Called after init_per_suite.
 post_init_per_suite(Suite,Config,Return,State = #state{nodes = undefined}) ->
     Nodes = get_nodes(Config),
     AliveNodes = get_alive_nodes(Nodes),
     post_init_per_suite(Suite,Config,Return,State#state{nodes = AliveNodes});

 post_init_per_suite(Suite,Config,Return,State = #state{nodes = Nodes}) ->
     AliveNodes = get_alive_nodes(Nodes),
     case Nodes of
         AliveNodes ->
             {Return, State};
         _ ->
             Reason = {dead_nodes, Nodes -- AliveNodes},
             {{fail,Reason}, State}
     end.

 %% Called when the scope of the CTH is done
 terminate(State) ->
     ok.

is_mongoose_loaded(Node) ->
    case rpc:call(Node, application, which_applications, []) of
        Apps when is_list(Apps) ->
            lists:keymember(mongooseim, 1, Apps);
        Other ->
            false
    end.

 get_nodes(Config) ->
     %% Parse big_tests/test.config
     Hosts = proplists:get_value(hosts, Config, []),
     [proplists:get_value(node, HostProps) || {_Name, HostProps} <- Hosts].

get_alive_nodes(Nodes) ->
    [Node || Node <- Nodes, is_mongoose_loaded(Node)].
