-module(mim_ct).
-export([run/1]).

run(RunConfig = #{test_spec := TestSpec, test_config := TestConfigFile, test_config_out := TestConfigFileOut}) ->
    {ok, TestConfig} = file:consult(TestConfigFile),
    %% RunConfig overrides TestConfig
    TestConfig1 = maps:merge(maps:from_list(TestConfig), RunConfig),
    TestConfig2 = mim_ct_cover:start_cover(TestConfig1),
    TestConfig3 = start_nodes(TestConfig2),
    TestConfigFileOut2 = filename:absname(TestConfigFileOut, path_helper:test_dir([])),
    ok = write_terms(TestConfigFileOut, mim_ct_config_ports:preprocess(maps:to_list(TestConfig3))),
    CtOpts = [{spec, TestSpec}, {userconfig, {ct_config_plain, [TestConfigFileOut2]}}],
    HelperState = mim_ct_helper:before_start(),
    CtResult = ct:run_test(CtOpts),
    Result = mim_ct_helper:after_test([CtResult], HelperState),
    TestConfig4 = mim_ct_cover:analyze_cover(TestConfig3),
    {Result, TestConfig4}.

write_terms(Filename, List) ->
    Format = fun(Term) -> io_lib:format("~tp.~n", [Term]) end,
    Text = lists:map(Format, List),
    file:write_file(Filename, Text).

start_nodes(TestConfig = #{hosts := Hosts}) ->
    Hosts1 = add_host_numbers(Hosts),
    Hosts2 = [{HostId, maps:to_list(start_host(HostId, maps:from_list(HostConfig), TestConfig))} || {HostId, HostConfig} <- Hosts1],
    TestConfig#{hosts => Hosts2}.

start_host(HostId, HostConfig = #{host_number := HostNumber}, TestConfig = #{repo_dir := RepoDir, first_port := FirstPort}) ->
    ReservedPortsPerHost = 100,
    FirstHostPort = FirstPort + (HostNumber - 1) * ReservedPortsPerHost,
    HostConfig1 = HostConfig#{repo_dir => RepoDir, build_dir => "_build/ng" ++ atom_to_list(HostId), prototype_dir => "_build/mim1", first_port => FirstHostPort},
    Result = mim_node:make(maybe_add_preset(HostConfig1, TestConfig), TestConfig),
    io:format("~p started~n", [HostId]),
    Result.

add_host_numbers(Hosts) ->
    [{HostId, [{host_number, HostNum}|HostConfig]} || {HostNum, {HostId, HostConfig}} <- lists:zip(lists:seq(1, length(Hosts)), Hosts)].

maybe_add_preset(HostConfig, TestConfig = #{preset := Preset}) ->
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
        
