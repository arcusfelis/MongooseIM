-module(mim_ct_master).
-export([start_slave/1]).

start_slave(Name) ->
    Opts = ct_slave_opts(),
    {ok, Localhost} = inet:gethostname(),
    Host = list_to_atom(Localhost),
    case timer:tc(fun() -> ct_slave:start(Host, Name, Opts) end) of
        {Time, {ok, Node}} ->
            io:format("Starting slave took ~p milliseconds", [Time div 1000]),
            Paths = code:get_path(),
            ok = rpc:call(Node, code, add_paths, [Paths]),
            Node;
        Other ->
            error({failed_to_start, Name, Other, Opts})
    end.

ct_slave_opts() ->
    [{monitor_master, true},
     {env, os:list_env_vars()},
     {boot_timeout, 15}, %% seconds
     {init_timeout, 10}, %% seconds
     {erl_flags, case os:getenv("EXTRA_ARGS") of false -> ""; ExtraArgs -> ExtraArgs end ++ " -hidden"},
     {startup_timeout, 10}]. %% seconds
