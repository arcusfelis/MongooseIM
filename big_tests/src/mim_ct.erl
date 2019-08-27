-module(mim_ct).
-export([run/1]).
-export([run_jobs/2]).

-export([ct_run/1]).


run_jobs(MasterConfig, JobConfigs) ->
    %% Read configs sequentially
    JobConfigs1 = [load_test_config(Job) || Job <- add_job_numbers(JobConfigs)],
    {MasterConfig1, JobConfigs2} = mim_ct_db:init_master(MasterConfig, JobConfigs1),
    MasterConfig2 = mim_ct_cover:start_cover(MasterConfig1),
    HelperState = mim_ct_helper:before_start(),
    JobResults = mim_ct_parallel:parallel_map(fun(Job) -> do_job(MasterConfig2, Job) end, JobConfigs2),
    GoodResults = [GoodResult || {ok, GoodResult} <- JobResults],
    {CtResults, TestConfigs} = lists:unzip(GoodResults),
    Result = mim_ct_helper:after_test(CtResults, HelperState),
    mim_ct_cover:analyze_cover(MasterConfig1),
    {Result, TestConfigs}.

do_job(MasterConfig, Job = #{slave_node := SlaveName}) ->
    RunConfig = maps:merge(MasterConfig, Job),
    Node = mim_ct_master:start_slave(SlaveName),
    rpc:call(Node, mim_ct, ct_run, [RunConfig]).

run(RunConfig) ->
    RunConfig0 = load_test_config(RunConfig),
    RunConfig1 = mim_ct_cover:start_cover(RunConfig0),
    HelperState = mim_ct_helper:before_start(),
    {CtResult, TestConfig} = ct_run(RunConfig1),
    Result = mim_ct_helper:after_test([CtResult], HelperState),
    mim_ct_cover:analyze_cover(RunConfig1),
    io:format("CtResult ~p~n", [CtResult]),
    {Result, [TestConfig]}.

ct_run(RunConfig) ->
    try
        do_ct_run(RunConfig)
    catch Class:Reason ->
              Stacktrace = erlang:get_stacktrace(),
              { {error, {Class, Reason, Stacktrace}}, RunConfig }
    end.

load_test_config(RunConfig = #{test_config := TestConfigFile}) ->
    {ok, TestConfig} = file:consult(TestConfigFile),
    %% RunConfig overrides TestConfig
    maps:merge(maps:from_list(TestConfig), RunConfig).

do_ct_run(RunConfig = #{test_spec := TestSpec, test_config := TestConfigFile, test_config_out := TestConfigFileOut}) ->
    mim_ct_preload:load_test_modules(TestSpec),
    TestConfig2 = mim_ct_cover:add_cover_node_to_hosts(RunConfig),
    TestConfig3 = init_hosts(TestConfig2),
    TestConfigFileOut2 = filename:absname(TestConfigFileOut, path_helper:test_dir([])),
    ok = write_terms(TestConfigFileOut, mim_ct_config_ports:preprocess(maps:to_list(TestConfig3))),
    CtOpts = [{spec, TestSpec},
              {userconfig, {ct_config_plain, [TestConfigFileOut2]}}, 
              {auto_compile, maps:get(auto_compile, RunConfig, true)}],
    {CtRunTime, CtResult} = timer:tc(fun() -> ct:run_test(CtOpts) end),
    mim_ct_helper:report_progress("~nct_run for ~ts took ~ts~n",
                                     [TestSpec, mim_ct_helper:microseconds_to_string(CtRunTime)]),
    {CtResult, TestConfig3}.

write_terms(Filename, List) ->
    Format = fun(Term) -> io_lib:format("~tp.~n", [Term]) end,
    Text = lists:map(Format, List),
    file:write_file(Filename, Text).

init_hosts(TestConfig) ->
    TestConfig1 = load_hosts(TestConfig),
    io:format("all hosts loaded~n", []),
    TestConfig2 = mim_ct_ports:rewrite_ports(TestConfig1),
    TestConfig3 = mim_ct_db:init_job(TestConfig2),
    TestConfig4 = add_prefix(TestConfig3),
    make_hosts(TestConfig4).

load_hosts(TestConfig = #{hosts := Hosts}) ->
    Hosts2 = [{HostId, maps:to_list(load_host(HostId, maps:from_list(HostConfig), TestConfig))} || {HostId, HostConfig} <- Hosts],
    TestConfig#{hosts => Hosts2}.

make_hosts(TestConfig = #{hosts := Hosts}) ->
    %% Start nodes in parallel
    F = fun({HostId, HostConfig}) ->
            {HostId, maps:to_list(make_host(HostId, maps:from_list(HostConfig)))}
        end,
    Results = mim_ct_parallel:parallel_map(F, Hosts),
    %% TODO report bad results
    [] = [BadResult || {error, BadResult} <- Results],
    Hosts2 = [GoodResult || {ok, GoodResult} <- Results],
    TestConfig#{hosts => Hosts2}.

load_host(HostId, HostConfig, TestConfig = #{repo_dir := RepoDir, prefix := Prefix}) ->
    HostConfig1 = HostConfig#{repo_dir => RepoDir, build_dir => "_build/" ++ Prefix ++ atom_to_list(HostId), prototype_dir => "_build/mim1", prefix => Prefix},
    Result = mim_node:load(maybe_add_preset(HostConfig1, TestConfig), TestConfig),
    io:format("~p loaded~n", [HostId]),
    Result.

make_host(HostId, HostConfig) ->
    Result = mim_node:make(HostConfig),
    io:format("~p started~n", [HostId]),
    Result.

maybe_add_preset(HostConfig, _TestConfig = #{preset := Preset}) ->
    HostConfig#{preset => Preset};
maybe_add_preset(HostConfig, _TestConfig) ->
    HostConfig.


ensure_nodes_running(TestConfig) ->
    Nodes = get_mongoose_nodes(TestConfig),
    Results = [{Node, net_adm:ping(Node)} || Node <- Nodes],
    Pangs = [Node || {Node, pang} <- Results],
    case Pangs of
        [] ->
            ok;
        _ ->
            io:format("ensure_nodes_running failed, results = ~p", [Results]),
            error({nodes_down, Pangs})
    end.

get_mongoose_nodes(TestConfig = #{hosts := Hosts}) ->
    [get_existing(node, Host) || Host <- Hosts].

get_existing(Key, Proplist) ->
    case lists:keyfind(Key, 1, Proplist) of
        {Key, Value} ->
            Value;
        _ ->
            error({not_found, Key, Proplist})
    end.

add_prefix(TestConfig = #{prefix := Prefix}) ->
    add_prefix_to_opts([ejabberd_node, ejabberd2_node], Prefix, TestConfig);
add_prefix(TestConfig) ->
    TestConfig.

add_prefix_to_opts([Opt|Opts], Prefix, TestConfig) ->
    add_prefix_to_opts(Opts, Prefix, add_prefix_to_opt(Opt, Prefix, TestConfig));
add_prefix_to_opts([], _Prefix, TestConfig) ->
    TestConfig.

add_prefix_to_opt(Opt, Prefix, TestConfig) ->
    case maps:find(Opt, TestConfig) of
        {ok, Value} ->
            Value2 = add_prefix(Prefix, Value),
            TestConfig#{Opt => Value2};
        error ->
            TestConfig
    end.

add_prefix(Prefix, Value) when is_atom(Value) ->
    list_to_atom(Prefix ++ atom_to_list(Value)).

add_job_numbers(JobConfigs) ->
    add_job_numbers(JobConfigs, 1).

add_job_numbers([Job|JobConfigs], N) ->
    [Job#{job_number => N}|add_job_numbers(JobConfigs)];
add_job_numbers([], _) ->
    [].
