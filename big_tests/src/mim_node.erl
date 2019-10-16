-module(mim_node).
-export([load/2]).
-export([make/1]).
-export([print_node_logs/1]).

%% Sets variables without touching anything on disk
%% Options:
%% - prototype_dir = "_build/name"
%% - build_dir = "_build/name"
%% - vars = "mim1.vars.config"
%% - repo_dir = REPO_DIR, abs path
load(NodeConfig, TestConfig) ->
    NodeConfig0 = maybe_set_cookie(NodeConfig, TestConfig),
    NodeConfig1 = make_abs_paths(NodeConfig0),
    NodeConfig2 = overlay_vars(NodeConfig1),
    NodeConfig3 = apply_preset(NodeConfig2, TestConfig),
    apply_prefix(NodeConfig3).

maybe_set_cookie(NodeConfig=#{ejabberd_cookie := Cookie}, TestConfig) ->
    set_cookie(NodeConfig);
maybe_set_cookie(NodeConfig, TestConfig=#{ejabberd_cookie := Cookie}) ->
    set_cookie(NodeConfig#{ejabberd_cookie => Cookie});
maybe_set_cookie(NodeConfig, _TestConfig) ->
    NodeConfig.

make(NodeConfig = #{node := Node}) ->
    io:format("making ~p~n", [Node]),
    set_cookie(NodeConfig),
    NodeConfig1 = copy_release(NodeConfig),
    io:format("apply_template ~p~n", [Node]),
    NodeConfig2 = apply_template(NodeConfig1),
    NodeConfig3 = stop(NodeConfig2),
    %% Start with clean logs
    NodeConfig4 = clean_logs(NodeConfig3),
    io:format("starting ~p~n", [Node]),
    NodeConfig5 = start(NodeConfig4),
    NodeConfig6 = assert_node_running(NodeConfig5),
    assert_can_register_users(NodeConfig6).

stop(NodeConfig = #{node := Node}) ->
    StopReturns = rpc:call(Node, init, stop, []),
    io:format("StopReturns ~p for ~p~n", [StopReturns, Node]),
    wait_for_pang(Node),
    io:format("wait_for_pang returns ~p~n", [Node]),
    NodeConfig.

wait_for_pang(Node) ->
    case net_adm:ping(Node) of
        pang ->
            ok;
        pong ->
            timer:sleep(100),
            wait_for_pang(Node)
    end.

assert_can_register_users(NodeConfig = #{node := Node, host_id := HostId}) ->
    Fun = fun() ->
            UserName = <<"ct_user_from_", (atom_to_binary(HostId, utf8))/binary>>,
            Host = hd(rpc:call(Node, ejabberd_config, get_global_option, [hosts])),
            Result = exist_is_ok(rpc:call(Node, ejabberd_auth, try_register, [UserName, Host, <<"ct_password123">>])),
            rpc:call(Node, ejabberd_auth, remove_user, [UserName, Host]),
            Result
          end,
    Opts = #{time_left => timer:seconds(60),
             sleep_time => 1000,
             name => assert_can_register_users_timeout},
    try
        mongoose_helper:wait_until(Fun, ok, Opts),
        NodeConfig
    catch Class:Reason ->
        mark_as_failed(NodeConfig, assert_can_register_users, #{wait_until_returns => {Class, Reason}})
    end.

exist_is_ok({error, exist}) ->
    ok;
exist_is_ok(Result) ->
    Result.

assert_node_running(NodeConfig = #{node := Node, build_dir := BuildDir}) ->
    case net_adm:ping(Node) of
        pang ->
            mark_as_failed(NodeConfig, assert_node_running);
        pong ->
            NodeConfig
    end.

print_node_logs(#{node := Node, build_dir := BuildDir}) ->
    print_logs(Node, BuildDir);
print_node_logs(NodeConfig) ->
    io:format("Skip print_node_logs for ~p~n", [NodeConfig]),
    ok.

print_logs(Node, BuildDir) ->
    LogDir = filename:join(BuildDir, "rel/mongooseim/log"),
    case file:list_dir(LogDir) of
        {ok, LogFiles} ->
            LogFiles1 = lists:sort(LogFiles),
            [print_log_file(Node, filename:join(LogDir, LogFile)) || LogFile <- LogFiles1];
        Other ->
            io:format("No logs in ~p - ~p~n", [LogDir, Other])
    end.

print_log_file(Node, LogFile) ->
    F = fun() ->
            case file:read_file(LogFile) of
                {ok, Bin} ->
                    catch io:format("~n~ts~n", [preprocess_log(Bin)]);
                Error ->
                    catch io:format("Failed to read file ~p~n", [Error])
            end
        end,
    mim_ct_helper:travis_fold("Log", "Log from " ++ atom_to_list(Node) ++ " " ++ filename:basename(LogFile), F).

clean_logs(NodeConfig = #{build_dir := BuildDir}) ->
    LogDir = filename:join(BuildDir, "rel/mongooseim/log"),
    case file:list_dir(LogDir) of
        {ok, LogFiles} ->
            [file:delete(filename:join(LogDir, LogFile)) || LogFile <- LogFiles],
            NodeConfig;
        _ ->
            %% Directory does not exist
            NodeConfig
    end.

start(NodeConfig = #{build_dir := BuildDir, node := Node}) ->
    Ctl = filename:join(BuildDir, "rel/mongooseim/bin/mongooseimctl"),
    Envs = mim_ct_cover:add_start_envs(NodeConfig, #{}),
    CmdOpts = #{env => Envs},
    StartResult = mim_ct_sh:run([Ctl, "start"], CmdOpts),
    io:format("waiting for ~p~n", [Node]),
    StartedResult = erlsh:run([Ctl, "started"]),
    {_, _, StatusResult} = erlsh:run([Ctl, "status"]),
    io:format("Node status ~p~n~ts~n", [Node, StatusResult]),
    NodeConfig#{start_result => StartResult, started_result => StartedResult, status_result => StatusResult}.


make_abs_paths(NodeConfig = #{prototype_dir := ProtoDir, build_dir := BuildDir, repo_dir := RepoDir}) ->
    NodeConfig#{
        prototype_dir => filename:absname(ProtoDir, RepoDir),
        build_dir => filename:absname(BuildDir, RepoDir)}.

copy_release(NodeConfig = #{prototype_dir := FromDir, build_dir := ToDir}) ->
    CopyResult = erlsh:run(["rsync", 
            "--exclude", "rel/mongooseim/Mnesia.*",
            "--exclude", "rel/mongooseim/var",
            "--exclude", "rel/mongooseim/log",
            "-al", FromDir ++ "/", ToDir ++ "/"]),
    NodeConfig#{copy_result => CopyResult}.


apply_template(NodeConfig = #{build_dir := BuildDir, repo_dir := RepoDir}) ->
    RelDir = BuildDir ++ "/rel/mongooseim",
    Templates = templates(RelDir),
    NodeConfig1 = NodeConfig#{output_dir => list_to_binary(RelDir)},
    [render_template(filename:absname(In, RepoDir), Out, NodeConfig1) || {In, Out} <- Templates],
    validate_term_files(RelDir),
    NodeConfig1.

validate_term_files(RelDir) ->
    TermFiles = term_files(),
    [validate_term_file(RelDir, TermFile) || TermFile <- TermFiles].

validate_term_file(RelDir, TermFile) ->
    FilePath = filename:absname(TermFile, RelDir),
    case file:consult(FilePath) of
        {ok, _} ->
            ok;
        Other ->
            print_file_with_lines(FilePath),
            error({validate_term_file_failed, [
                    {filename, FilePath},
                    {reason, Other}]
                  })
                    
    end.

print_file_with_lines(FilePath) ->
    case file:read_file(FilePath) of
        {ok, Bin} ->
            io:format("print_file_with_lines ~ts:~n~ts~n", [FilePath, format_with_line_numbers(Bin)]),
            ok;
        Other ->
            Other
    end.

format_with_line_numbers(Bin) ->
    LinesWithNumbers = enumerate(lines(Bin)),
    [io_lib:format("~p: ~ts~n", [LineNum, LineText]) 
     || {LineNum, LineText} <- LinesWithNumbers].

enumerate(List) ->
    lists:zip(lists:seq(1, length(List)), List).

lines(Bin) ->
    binary:split(Bin, [<<"\n">>, <<"\r">>], [global]).

overlay_vars(NodeConfig = #{vars := VarsFile, repo_dir := RepoDir}) ->
    Vars = consult_map(filename:absname("rel/vars.config", RepoDir)),
    NodeVars = consult_map(filename:absname("rel/" ++ VarsFile, RepoDir)),
    %% NodeVars overrides Vars
    Vars2 = maps:merge(Vars, NodeVars),
    %% NodeConfig overrides Vars2
    maps:merge(Vars2, NodeConfig).

consult_map(File) ->
    case file:consult(File) of
        {ok, Vars} ->
            maps:from_list(Vars);
        Other ->
            error({consult_map_failed, File, Other})
    end.

read_file(File) ->
    case file:read_file(File) of
        {ok, Bin} ->
            Bin;
        Other ->
            error({read_file_failed, File, Other})
    end.

%% Based on rebar.config overlay section
templates(RelDir) ->
    simple_templates(RelDir) ++ erts_templates(RelDir).

simple_templates(RelDir) ->
    [{In, RelDir ++ "/" ++ Out} || {In, Out} <- simple_templates()].

simple_templates() ->
    [
     {"rel/files/mongooseim",       "bin/mongooseim"},
     {"rel/files/mongooseimctl",    "bin/mongooseimctl"},
     {"rel/files/app.config",       "etc/app.config"},
     {"rel/files/vm.args",          "etc/vm.args"},
     {"rel/files/vm.dist.args",     "etc/vm.dist.args"},
     {"rel/files/mongooseim.cfg",   "etc/mongooseim.cfg"}
    ].

term_files() ->
    ["etc/app.config",
     "etc/mongooseim.cfg"].

erts_templates(RelDir) ->
    %% Usually one directory
    ErtsDirs = filelib:wildcard(RelDir ++ "/erts-*"),
    [{"rel/files/nodetool", ErtsDir ++ "/bin/nodetool"} || ErtsDir <- ErtsDirs].

render_template(In, Out, Vars) ->
    BinIn = read_file(In),
    %% Do render twice to allow templates in variables
    BinTmp = bbmustache:render(BinIn, Vars, render_opts()),
    BinOut = bbmustache:render(BinTmp, Vars, render_opts()),
    case file:write_file(Out, BinOut) of
        ok ->
            ok;
        Other ->
            error({write_file_failed, Out, Other})
    end.

render_opts() ->
    [{escape_fun, fun(X) -> X end}, {key_type, atom}, {value_serializer, fun(X) -> X end}].

rewrite_ports(NodeConfig = #{first_port := FirstPort}) ->
    PortKeys = [K || {K,V} <- maps:to_list(NodeConfig), K =/= first_port, is_port_option(K), is_integer(V)],
    PortValues = [maps:get(Key, NodeConfig) || Key <- PortKeys],
    UniquePorts = lists:usort(PortValues),
    NewPorts = lists:seq(FirstPort, FirstPort + length(UniquePorts) - 1),
    Mapping = maps:from_list(lists:zip(UniquePorts, NewPorts)),
    maps:map(fun(K,V) ->
                case lists:member(K, PortKeys) of
                    true ->
                        NewV = maps:get(V, Mapping),
                        io:format("Rewrite port ~p ~p to ~p~n", [K, V, NewV]),
                        NewV;
                    false ->
                        V
                end
             end, NodeConfig);
rewrite_ports(NodeConfig) ->
    NodeConfig.

is_port_option(K) ->
     lists:suffix("_port", atom_to_list(K)).

apply_preset(NodeConfig = #{preset := PresetName, cluster := Cluster}, TestConfig = #{ejabberd_presets := Presets}) 
    %% We apply preset options to `mim` and `reg` clusters
    %% Otherwise we would receive registration conflict in s2s suite, if presets are applied for fed.
    when Cluster =:= mim; Cluster =:= reg ->
    case proplists:get_value(PresetName, Presets) of
        undefined ->
            error(#{error => preset_not_found, preset_name => PresetName, ejabberd_presets => Presets});
        PresetVarsList ->
            PresetVars = maps:from_list(PresetVarsList),
            %% PresetVars overrides NodeConfig
            maps:merge(NodeConfig, PresetVars)
    end;
apply_preset(NodeConfig, _TestConfig) ->
    io:format("Ignore presets", []),
    NodeConfig.

apply_prefix(NodeConfig = #{node := Node, node_name := NodeName, prefix := Prefix}) ->
    NodeConfig#{node => list_to_atom(Prefix ++ atom_to_list(Node)), node_name => Prefix ++ NodeName};
apply_prefix(NodeConfig = #{}) ->
    io:format("Ignore prefix", []),
    NodeConfig.

set_cookie(NodeConfig = #{node := Node, ejabberd_cookie := Cookie}) ->
    io:format("set_cookie node=~p cookie=~p~n", [Node, Cookie]),
    erlang:set_cookie(Node, to_atom(Cookie)),
    NodeConfig;
set_cookie(NodeConfig) ->
    NodeConfig.

to_atom(X) when is_list(X) -> list_to_atom(X);
to_atom(X) when is_binary(X) -> to_atom(binary_to_list(X));
to_atom(X) when is_atom(X) -> X.


preprocess_log(Log) ->
    MaxLines  = 500,
    Lines = binary:split(Log, <<"\n">>, [global]),
    Filtered = filter_lines(Lines),
    BeforeFiltering = length(Lines),
    AfterFiltering = length(Filtered),
    SkippedCount = BeforeFiltering - AfterFiltering,
    SubLines = lists:sublist(Filtered, 1, MaxLines),
    Summary = iolist_to_binary(io_lib:format("TotalLines=~p, SkippedLines=~p",
                                             [BeforeFiltering, SkippedCount])),
    binary_join([Summary|SubLines], <<"\n">>).

binary_join(L, Sep) ->
    iolist_to_binary(join_s(L, Sep)).

join_s([], _Sep) ->
    [];
join_s([H|T], Sep) ->
    [H, [[Sep, X] || X <- T]].

filter_lines(Lines) ->
    [Line || Line <- Lines, pass_filtering(Line)].

pass_filtering(Line) ->
    not lists:any(fun(Bad) -> binary:match(Line, Bad) =/= nomatch end, filter_out_patterns()).

filter_out_patterns() ->
    [<<"issue=remove_user_failed">>,
     <<"event=outgoing_global_distrib_socket_closed">>,
     <<"event=cannot_delete_personal_data">>,
     <<"event=incoming_global_distrib_socket_closed">>,
     <<"Supervisor received unexpected message: {'ETS-TRANSFER'">>].

mark_as_failed(NodeConfig, Reason, Extra) ->
    NodeConfig#{error => Reason, error_extra => Extra}.

mark_as_failed(NodeConfig, Reason) ->
    NodeConfig#{error => Reason}.
