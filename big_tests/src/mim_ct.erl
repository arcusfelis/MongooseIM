-module(mim_ct).
-export([run/1]).

run(RunConfig = #{test_spec := TestSpec, test_config := TestConfigFile, test_config_out := TestConfigFileOut}) ->
    RunConfig1 = mim_ct_cover:start_cover(RunConfig),
    HelperState = mim_ct_helper:before_start(),
    {CtResult, TestConfig} = ct_run(RunConfig1),
    Result = mim_ct_helper:after_test([CtResult], HelperState),
    mim_ct_cover:analyze_cover(RunConfig),
    {Result, [TestConfig]}.

ct_run(RunConfig = #{test_spec := TestSpec, test_config := TestConfigFile, test_config_out := TestConfigFileOut}) ->
    mim_ct_preload:load_test_modules(TestSpec),
    {ok, TestConfig} = file:consult(TestConfigFile),
    %% RunConfig overrides TestConfig
    TestConfig1 = maps:merge(maps:from_list(TestConfig), RunConfig),
    TestConfig2 = mim_ct_cover:add_cover_node_to_hosts(TestConfig1),
    TestConfig3 = init_hosts(TestConfig2),
    TestConfigFileOut2 = filename:absname(TestConfigFileOut, path_helper:test_dir([])),
    ok = write_terms(TestConfigFileOut, mim_ct_config_ports:preprocess(maps:to_list(TestConfig3))),
    CtOpts = [{spec, TestSpec},
              {userconfig, {ct_config_plain, [TestConfigFileOut2]}}, 
              {auto_compile, maps:get(auto_compile, RunConfig, true)}],
    CtResult = ct:run_test(CtOpts),
    {CtResult, TestConfig3}.

write_terms(Filename, List) ->
    Format = fun(Term) -> io_lib:format("~tp.~n", [Term]) end,
    Text = lists:map(Format, List),
    file:write_file(Filename, Text).

init_hosts(TestConfig) ->
    TestConfig1 = load_hosts(TestConfig),
    io:format("all hosts loaded~n", []),
    TestConfig2 = mim_ct_ports:rewrite_ports(TestConfig1),
    make_hosts(TestConfig2).

load_hosts(TestConfig = #{hosts := Hosts}) ->
    Hosts2 = [{HostId, maps:to_list(load_host(HostId, maps:from_list(HostConfig), TestConfig))} || {HostId, HostConfig} <- Hosts],
    TestConfig#{hosts => Hosts2}.

make_hosts(TestConfig = #{hosts := Hosts}) ->
    Hosts2 = [{HostId, maps:to_list(make_host(HostId, maps:from_list(HostConfig)))} || {HostId, HostConfig} <- Hosts],
    TestConfig#{hosts => Hosts2}.

load_host(HostId, HostConfig, TestConfig = #{repo_dir := RepoDir}) ->
    HostConfig1 = HostConfig#{repo_dir => RepoDir, build_dir => "_build/ng" ++ atom_to_list(HostId), prototype_dir => "_build/mim1"},
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
