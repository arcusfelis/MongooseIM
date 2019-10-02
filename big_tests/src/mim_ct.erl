-module(mim_ct).
-export([run/1]).
-export([run_jobs/2]).

-export([ct_run/1]).


run_jobs(MasterConfig, JobConfigs) ->
    %% Read configs sequentially
    JobConfigs1 = [load_test_config(Job) || Job <- add_job_numbers(JobConfigs)],
    InitDbFun = fun() -> mim_ct_db:init_master(MasterConfig, JobConfigs1) end,
    {MasterConfig1, JobConfigs2} =
        mim_ct_helper:buffer_fold("db.init_master", "Init Databases", InitDbFun, "_build/init_db_out.txt"),
    MasterConfig2 = mim_ct_cover:start_cover(MasterConfig1),
    HelperState = mim_ct_helper:before_start(),
    io:format("Jobs started ~p~n", [job_numbers(JobConfigs2)]),
    JobResults = mim_ct_parallel:parallel_map(fun(Job) -> do_job(MasterConfig2, Job) end, JobConfigs2),
    GoodResults = [GoodResult || {ok, GoodResult} <- JobResults],
    {CtResults, TestConfigs} = lists:unzip(GoodResults),
    io:format("Jobs completed ~p~n", [job_numbers(TestConfigs)]),
    Result = mim_ct_helper:after_test(CtResults, TestConfigs, HelperState),
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
    Result = mim_ct_helper:after_test([CtResult], [TestConfig], HelperState),
    mim_ct_cover:analyze_cover(RunConfig1),
    io:format("CtResult ~p~n", [CtResult]),
    {Result, [TestConfig]}.

ct_run(RunConfig = #{prefix := Prefix}) ->
    try
        Fun = fun() -> do_ct_run(RunConfig) end,
        FilenameOut = "_build/" ++ Prefix ++ "_out.txt",
        mim_ct_helper:buffer_fold("ct_run." ++ Prefix, "ct_run " ++ Prefix, Fun, FilenameOut)
    catch Class:Reason ->
              Stacktrace = erlang:get_stacktrace(),
              { {error, {Class, Reason, Stacktrace}}, RunConfig }
    end.

load_test_config(RunConfig = #{test_config := TestConfigFile}) ->
    {ok, TestConfig} = file:consult(TestConfigFile),
    %% RunConfig overrides TestConfig
    maps:merge(maps:from_list(TestConfig), RunConfig).

do_ct_run(RunConfig = #{test_spec := TestSpec}) ->
    mim_ct_preload:load_test_modules(TestSpec),
    TestConfig2 = mim_ct_cover:add_cover_node_to_hosts(RunConfig),
    TestConfig3 = init_hosts(TestConfig2),
    do_ct_run_if_no_errors(TestConfig3).

do_ct_run_if_no_errors(TestConfig) ->
    do_ct_run_if_no_errors(TestConfig, mim_ct_error:has_errors(TestConfig)).

do_ct_run_if_no_errors(TestConfig = #{test_spec := TestSpec}, true) ->
    ShortErrors = mim_ct_error:get_short_errors(TestConfig),
    mim_ct_helper:report_progress("Don't even try to run CT for ~p, short_errors=~1000p~n", [TestSpec, ShortErrors]),
    CtResult = {error, {skip_ct_run, ShortErrors}},
    {CtResult, TestConfig};
do_ct_run_if_no_errors(TestConfig = #{test_spec := TestSpec, test_config_out := TestConfigFileOut}, false) ->
    TestConfigFileOut2 = filename:absname(TestConfigFileOut, path_helper:test_dir([])),
    ok = write_terms(TestConfigFileOut, mim_ct_config:preprocess(maps:to_list(TestConfig))),
    CtOpts = [{spec, TestSpec},
              {userconfig, {ct_config_plain, [TestConfigFileOut2]}}, 
              {auto_compile, maps:get(auto_compile, TestConfig, true)}],
    {CtRunTime, CtResult} = timer:tc(fun() -> ct:run_test(CtOpts) end),
    mim_ct_helper:report_progress("~nct_run for ~ts took ~ts~n",
                                     [TestSpec, mim_ct_helper:microseconds_to_string(CtRunTime)]),
    {CtResult, TestConfig}.

write_terms(Filename, List) ->
    Format = fun(Term) -> io_lib:format("~tp.~n", [Term]) end,
    Text = lists:map(Format, List),
    file:write_file(Filename, Text).

init_hosts(TestConfig) ->
    TestConfig0 = maybe_disable_hosts(TestConfig),
    TestConfig1 = load_hosts(TestConfig0),
    io:format("all hosts loaded~n", []),
    TestConfig2 = mim_ct_ports:rewrite_ports(TestConfig1),
    TestConfig3 = mim_ct_db:init_job(TestConfig2),
    TestConfig4 = add_prefix(TestConfig3),
    make_hosts(TestConfig4).

load_hosts(TestConfig = #{hosts := Hosts}) ->
    Hosts2 = [{HostId, maps:to_list(load_host(HostId, maps:from_list(HostConfig), TestConfig))} || {HostId, HostConfig} <- Hosts],
    TestConfig#{hosts => Hosts2}.

make_hosts(TestConfig = #{test_spec := TestSpec, hosts := Hosts}) ->
    F = fun() -> do_make_hosts(TestConfig, TestSpec, Hosts) end,
    mim_ct_helper:travis_fold("make_hosts", "Start MongooseIM nodes", F).

do_make_hosts(TestConfig, TestSpec, Hosts) ->
    %% Start nodes in parallel
    F = fun({HostId, HostConfig}) ->
            {HostId, maps:to_list(make_host(HostId, maps:from_list(HostConfig)))}
        end,
    {StartTime, Results} = timer:tc(fun() -> mim_ct_parallel:parallel_map(F, Hosts) end),
    mim_ct_helper:report_progress("~nStarting hosts for ~ts took ~ts~n",
                                     [TestSpec, mim_ct_helper:microseconds_to_string(StartTime)]),
    Hosts2 = [handle_make_host_result(Result, Host) || {Result, Host} <- lists:zip(Results, Hosts)],
    mark_job_failed_if_host_failed(TestConfig#{hosts => Hosts2}).

handle_make_host_result({ok, {HostId, HostConfig}}, {HostId, _InitialHostConfig}) ->
    {HostId, HostConfig};
handle_make_host_result({error, Error}, {HostId, InitialHostConfig}) ->
    io:format("make_host_failed_hard reason=~p, host_id=~p~n", [Error, HostId]),
    {HostId, [{error, {make_host_failed_hard, Error}}|InitialHostConfig]}.

mark_job_failed_if_host_failed(TestConfig = #{test_spec := TestSpec}) ->
    case get_failed_hosts(TestConfig) of
        [] ->
            TestConfig; %% all hosts are fine
        FailedHosts ->
            Extra = #{issue => make_host_failed,
                      failed_hosts => FailedHosts,
                      test_spec => TestSpec,
                      reasons => get_failed_hosts_errors(TestConfig)},
            mim_ct_error:add_error(make_host_failed, Extra, TestConfig)
    end.

get_failed_hosts(TestConfig=#{hosts := Hosts}) ->
    [HostId || {HostId, Host} <- Hosts, lists:keymember(error, 1, Host)].

get_failed_hosts_errors(TestConfig=#{hosts := Hosts}) ->
    [{HostId, proplists:get_value(error, Host)} || {HostId, Host} <- Hosts, lists:keymember(error, 1, Host)].

load_host(HostId, HostConfig, TestConfig = #{repo_dir := RepoDir, prefix := Prefix}) ->
    HostConfig1 = HostConfig#{repo_dir => RepoDir, build_dir => "_build/" ++ Prefix ++ atom_to_list(HostId), prototype_dir => "_build/mim1", prefix => Prefix, host_id => HostId},
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
    [Job#{job_number => N}|add_job_numbers(JobConfigs, N+1)];
add_job_numbers([], _) ->
    [].

job_numbers(TestConfigs) ->
    [N || #{job_number := N} <- TestConfigs].


maybe_disable_hosts(TestConfig = #{enabled_hosts := EnabledHosts, hosts := Hosts}) ->
    Hosts2 = [H || H = {Id, _} <- Hosts, lists:member(Id, EnabledHosts)],
    DisabledHosts = [Id || {Id, _} <- Hosts, not lists:member(Id, EnabledHosts)],
    TestConfig#{hosts => Hosts2, disabled_hosts => DisabledHosts};
maybe_disable_hosts(TestConfig = #{disabled_hosts := DisabledHosts, hosts := Hosts}) ->
    Hosts2 = [H || H = {Id, _} <- Hosts, not lists:member(Id, DisabledHosts)],
    TestConfig#{hosts => Hosts2};
maybe_disable_hosts(TestConfig) ->
    TestConfig.
