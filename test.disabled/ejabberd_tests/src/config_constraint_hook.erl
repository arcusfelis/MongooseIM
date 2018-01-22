%%% @doc Ensures that tests do not change configuration.
-module(config_constraint_hook).

%% @doc Add the following line in your *.spec file to enable
%% reasonable, progress error reporting for your common tests:
%% {ct_hooks, [config_constraint_hook]}.

%% Callbacks
-export([id/1]).
-export([init/2]).
-export([pre_init_per_suite/3]).
-export([post_end_per_suite/4]).
-record(state, { options }).

%% @doc Return a unique id for this CTH.
id(_Opts) ->
    "config_constraint_hook_001".

%% @doc Always called before any other callback function. Use this to initiate
%% any common state.
init(_Id, _Opts) ->
    {ok, #state{  }}.

pre_init_per_suite(Suite, Config, State) ->
    {Config, State#state{options = dump_options()}}.

post_end_per_suite(Suite, _Config, Return, State=#state{options=PreOptions}) ->
    PostOptions = dump_options(),
    case PostOptions of
        PreOptions ->
            ok;
        _ ->
            ct:pal("issue=suite_changed_options "
                   "suite=~p ~n"
                   "PreOptions=~p. ~n"
                   "PostOptions=~p. ~n",
                   [Suite, PreOptions, PostOptions])
    end,
    {Return, State}.

dump_options() ->
    escalus_ejabberd:rpc(gen_mod, dump_options, []).
