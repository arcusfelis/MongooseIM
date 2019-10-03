-module(mim_ct_db).
-export([init_master/2]).
-export([init_job/1]).
-export([print_job_logs/1]).

init_master(MasterConfig = #{repo_dir := RepoDir}, JobConfigs) ->
    MasterDbs = get_databases(MasterConfig, JobConfigs),
    io:format("MasterDbs ~p~n", [MasterDbs]),
    F = fun(DbType) ->
            {Time, InitDbReturns} = timer:tc(fun() -> init_db(DbType, RepoDir) end),
            {Status, _, Result} = InitDbReturns,
            io:format("DB ~p started:~n~ts~n", [DbType, Result]),
            case Status of
                skip ->
                    ok;
                _ ->
                    mim_ct_helper:report_progress("Starting database ~p took ~ts~n",
                                                  [DbType, mim_ct_helper:microseconds_to_string(Time)])
            end,
            ok
        end,
    mim_ct_parallel:parallel_map(F, MasterDbs),
    Results = [init_db(DbType, RepoDir) || DbType <- MasterDbs],
    io:format("init_master results are ~p~n", [Results]),
    {MasterConfig, JobConfigs}.

init_job(TestConfig = #{preset := Preset}) ->
    Dbs = preset_to_databases(Preset, TestConfig),
    init_job_dbs(Dbs, TestConfig);
init_job(TestConfig) ->
    TestConfig.

init_job_dbs([Db|Dbs], TestConfig) ->
    {Time, Result} = timer:tc(fun() -> init_job_db(Db, TestConfig) end),
    case Result of
        skip ->
            init_job_dbs(Dbs, TestConfig);
        TestConfig2 ->
            mim_ct_helper:report_progress("Starting database ~p took ~ts~n",
                                          [Db, mim_ct_helper:microseconds_to_string(Time)]),
            init_job_dbs(Dbs, TestConfig2)
    end;
init_job_dbs([], TestConfig) ->
    TestConfig.

init_job_db(mssql, TestConfig = #{prefix := Prefix, repo_dir := RepoDir}) ->
    DbName = Prefix ++ "_mim_db",
    setup_mssql_database(DbName, RepoDir),
    set_option(mssql_database, DbName, TestConfig);
init_job_db(mysql, TestConfig = #{prefix := Prefix, hosts := Hosts, repo_dir := RepoDir, job_number := JobNumber}) ->
    [DbPort] = get_ports(mysql_port, TestConfig),
    setup_mysql_container(DbPort, Prefix ++ "_mim_db_" ++ integer_to_list(JobNumber), RepoDir),
    TestConfig;
init_job_db(pgsql, TestConfig = #{prefix := Prefix, hosts := Hosts, repo_dir := RepoDir, job_number := JobNumber}) ->
    [DbPort] = get_ports(pgsql_port, TestConfig),
    setup_pgsql_container(DbPort, Prefix ++ "_mim_db_" ++ integer_to_list(JobNumber), RepoDir),
    TestConfig;
init_job_db(riak, TestConfig = #{prefix := Prefix, hosts := Hosts, repo_dir := RepoDir, job_number := JobNumber}) ->
    TestConfig2 = setup_riak_prefix(Prefix, RepoDir, TestConfig),
    set_option(riak_prefix, Prefix, TestConfig2);
init_job_db(ldap, TestConfig = #{job_number := JobNumber}) ->
    %% Use different ldap_base for each job
    set_option(ldap_suffix, integer_to_list(JobNumber), TestConfig);
init_job_db(redis, TestConfig = #{job_number := JobNumber}) ->
    set_option(redis_database, JobNumber, TestConfig);
init_job_db(Db, _TestConfig) ->
    io:format("init_job_db: Do nothing for db ~p~n", [Db]),
    skip.

set_option(OptName, OptValue, TestConfig) ->
    TestConfig2 = set_host_option(OptName, OptValue, TestConfig),
    set_preset_option(OptName, OptValue, TestConfig2).

%% Set option for all hosts
set_host_option(OptName, OptValue, TestConfig = #{hosts := Hosts}) ->
    Hosts2 = [{HostId, lists:keystore(OptName, 1, Host, {OptName, OptValue})} || {HostId, Host} <- Hosts],
    TestConfig#{hosts => Hosts2}.

%% Set option for all presets
set_preset_option(OptName, OptValue, TestConfig = #{ejabberd_presets := Presets}) ->
    Presets2 = [{PresetName, lists:keystore(OptName, 1, Preset, {OptName, OptValue})} || {PresetName, Preset} <- Presets],
    TestConfig#{ejabberd_presets => Presets2}.

get_ports(PortPropertyName, _TestConfig = #{hosts := Hosts}) when is_atom(PortPropertyName) ->
    %% We expect one or zero ports here
    lists:delete(none, lists:usort([proplists:get_value(PortPropertyName, Host, none) || {_HostId, Host} <- Hosts])).

get_databases(MasterConfig, JobConfigs) ->
    lists:usort(lists:append([get_job_databases(MasterConfig, JobConfig) || JobConfig <- JobConfigs])).

get_job_databases(MasterConfig, JobConfig) ->
    case get_job_preset(MasterConfig, JobConfig) of
        none ->
            [];
        Preset ->
            preset_to_databases(Preset, JobConfig)
    end.

get_job_preset(_MasterConfig, _JobConfig = #{preset := Preset}) ->
    Preset;
get_job_preset(_MasterConfig = #{preset := Preset}, _JobConfig) ->
    Preset;
get_job_preset(MasterConfig, _JobConfig) ->
    %% No preset
    none.

preset_to_databases(Preset, JobConfig = #{ejabberd_presets := Presets}) ->
    PresetOpts =  proplists:get_value(Preset, Presets, []),
    proplists:get_value(dbs, PresetOpts, []).

init_db(mysql, _RepoDir) ->
    {skip, 0, "skip for master"};
init_db(pgsql, _RepoDir) ->
    {skip, 0, "skip for master"};
init_db(redis, RepoDir) ->
    case mim_ct_ports:is_port_free(6379) of
        true ->
            %% Redis is already running
            do_init_db(redis, RepoDir);
        false ->
            {skip, 0, "redis is already running"}
    end;
init_db(DbType, RepoDir) ->
    do_init_db(DbType, RepoDir).

do_init_db(DbType, RepoDir) ->
    mim_ct_sh:run([filename:join([RepoDir, "tools", "travis-setup-db.sh"])], #{env => #{"DB" => atom_to_list(DbType), "DB_PREFIX" => "mim-ct1"}, cwd => RepoDir}).

setup_mssql_database(DbName, RepoDir) ->
    mim_ct_sh:run([filename:join([RepoDir, "tools", "setup-mssql-database.sh"])], #{env => #{"DB_NAME" => DbName, "DB_PREFIX" => "mim-ct1"}, cwd => RepoDir}).

setup_riak_prefix(Prefix, RepoDir, TestConfig) ->
    RiakContainer = "mim-ct1-riak",
    % docker exec -e SCRIPT_CMD="setup_prefix" -e RIAK_PREFIX="mim2" $NAME riak escript /setup_riak.escript
    {_,_,Result} = Return = mim_ct_sh:run(["docker", "exec", "-e", "SCRIPT_CMD=setup_prefix", "-e", "RIAK_PREFIX=" ++ Prefix, RiakContainer, "riak", "escript", "/setup_riak.escript"], #{}),
    io:format("setup_riak_prefix returns~n~ts~n", [Result]),
    handle_cmd_result(setup_riak_prefix, Return, TestConfig).

setup_mysql_container(DbPort, Prefix, RepoDir) ->
    Envs = #{"DB" => "mysql", "MYSQL_PORT" => integer_to_list(DbPort), "DB_PREFIX" => "mim-ct1-" ++ Prefix},
    CmdOpts = #{env => Envs, cwd => RepoDir},
    {done, _, Result} = mim_ct_sh:run([filename:join([RepoDir, "tools", "travis-setup-db.sh"])], CmdOpts),
    io:format("Setup mysql container ~p returns ~ts~n", [DbPort, Result]),
    ok.

setup_pgsql_container(DbPort, Prefix, RepoDir) ->
    Envs = #{"DB" => "pgsql", "PGSQL_PORT" => integer_to_list(DbPort), "DB_PREFIX" => "mim-ct1-" ++ Prefix},
    CmdOpts = #{env => Envs, cwd => RepoDir},
    {done, _, Result} = mim_ct_sh:run([filename:join([RepoDir, "tools", "travis-setup-db.sh"])], CmdOpts),
    io:format("Setup pgsql container ~p returns ~ts~n", [DbPort, Result]),
    ok.

handle_cmd_result(CmdName, {done, 0, Result} = Return, TestConfig) ->
    save_return(CmdName, Return, TestConfig);
handle_cmd_result(CmdName, Return, TestConfig) ->
    TestConfig2 = save_return(CmdName, Return, TestConfig),
    mim_ct_error:add_error(handle_cmd_result_failed, #{command => CmdName, return => Return}, TestConfig2).

save_return(CmdName, Return, TestConfig) ->
    Old = maps:get(cmd_returns, TestConfig, []),
    TestConfig#{cmd_returns => [{CmdName, Return} | Old]}.

handle_exit_code(_DbType, _ExitCode=0, TestConfig) ->
    TestConfig;
handle_exit_code(DbType, ExitCode, TestConfig) ->
    mim_ct_error:add_error(riak_failed_to_start, #{exit_code => ExitCode}, TestConfig).

print_job_logs(TestConfig = #{preset := Preset}) ->
    Dbs = preset_to_databases(Preset, TestConfig),
    print_job_logs_for_dbs(Dbs, TestConfig).

print_job_logs_for_dbs(Dbs, TestConfig) ->
    [print_job_logs_for_db(Db, TestConfig) || Db <- Dbs],
    ok.

print_job_logs_for_db(fixme_riak, TestConfig = #{riak_container_name := RiakContainer, repo_dir := RepoDir}) ->
    print_container_inspect(RiakContainer),
    print_container_logs(RiakContainer),
    print_riak_logs_from_disk(RepoDir, RiakContainer),
    ok;
print_job_logs_for_db(_Db, _TestConfig) ->
    ok.

print_container_inspect(Container) ->
    {done, _, Result} = mim_ct_sh:run(["docker", "inspect", Container], #{}),
    F = fun() -> catch io:format("~n~ts~n", [Result]) end,
    mim_ct_helper:travis_fold("DbInspect", "Inspect from " ++ Container, F).

print_container_logs(Container) ->
    {done, _, Result} = mim_ct_sh:run(["docker", "logs", Container], #{}),
    F = fun() -> catch io:format("~n~ts~n", [Result]) end,
    mim_ct_helper:travis_fold("DbLog", "Log from " ++ Container, F).

print_riak_logs_from_disk(RepoDir, RiakContainer) ->
    LogDirDest = db_log_dir(RepoDir, RiakContainer),
    ok = filelib:ensure_dir(filename:join(LogDirDest, "ok")),
    %% Copies to "LogDirDest/riak" directory
    {done, _, Result} = mim_ct_sh:run(["docker", "cp", RiakContainer ++ ":/var/log/riak/", LogDirDest], #{}),
    io:format("docker cp returns ~ts~n", [Result]),
    Logs = list_files(filename:join(LogDirDest, "riak")),
    [print_log_file(RiakContainer, LogFile) || LogFile <- Logs],
    ok.

list_files(Dir) ->
    {ok, Files} = file:list_dir(Dir),
    FullNames = [filename:join(Dir, File) || File <- Files],
    [File || File <- FullNames, filelib:is_file(File)].

db_log_dir(RepoDir, Container) ->
    filename:join([RepoDir, "big_tests", "_build", "logs-" ++ Container ++ "-" ++ format_time()]).

print_log_file(Container, LogFile) ->
    F = fun() ->
            case file:read_file(LogFile) of
                {ok, Bin} ->
                    catch io:format("~n~ts~n", [Bin]);
                Error ->
                    catch io:format("Failed to read file ~p~n", [Error])
            end
        end,
    mim_ct_helper:travis_fold("DbLog", "Log from " ++ Container ++ " " ++ filename:basename(LogFile), F).

format_time() ->
    {{Year, Month, Day}, {Hour, Minute, Second}} = calendar:now_to_datetime(erlang:now()),
    lists:flatten(io_lib:format("~4..0w-~2..0w-~2..0wT~2..0w:~2..0w:~2..0w",[Year,Month,Day,Hour,Minute,Second])).

wait_for_riak(RiakPbPort, TestConfig = #{repo_dir := RepoDir}) ->
    load_riak_driver(RepoDir),
    case connect_to_riak(RepoDir, RiakPbPort, 5, []) of
        ok ->
             TestConfig;
        {error, Reason} ->
             io:format("riak_connect_failed~n", []),
             mim_ct_error:add_error(riak_connect_failed, #{reason => Reason}, TestConfig)
    end.

load_riak_driver(RepoDir) ->
    Dirs = filelib:wildcard(filename:absname("_build/default/lib/*/ebin", RepoDir)),
    code:add_paths(Dirs),
    application:ensure_all_started(ssl),
    application:ensure_all_started(riakc).

connect_to_riak(RepoDir, RiakPbPort, 0, Errors) ->
    {error, Errors};
connect_to_riak(RepoDir, RiakPbPort, Times, Errors) ->
    try
        try_connect_to_riak(RepoDir, RiakPbPort)
    catch Class:Reason ->
        io:format("try_connect_to_riak failed ~p~n", [Reason]),
        connect_to_riak(RepoDir, RiakPbPort, Times - 1, [{Class, Reason}|Errors])
    end.

try_connect_to_riak(RepoDir, RiakPbPort) ->
    %% Try 10 connections
    Pids = [new_riak_connection(RepoDir, RiakPbPort) || _ <- lists:seq(1, 10)],
    [wait_for_riak_connected(Pid) || Pid <- Pids],
    [riakc_pb_socket:stop(Pid) || Pid <- Pids],
    io:format("wait_for_riak_connected:done~n", []),
    ok.

new_riak_connection(RepoDir, RiakPbPort) ->
    CaCert = filename:join([RepoDir, "tools", "ssl", "ca", "cacert.pem"]),
    RiakOpts = [keepalive,auto_reconnect,{credentials,"ejabberd","mongooseim_secret"},{cacertfile, CaCert},
                {ssl_opts,[{ciphers,["AES256-SHA","DHE-RSA-AES128-SHA256"]},{server_name_indication,disable}]}],
    {ok, Pid} = riakc_pb_socket:start("127.0.0.1", RiakPbPort, RiakOpts),
    Pid.

wait_for_riak_connected(Pid) ->
    Fun = fun() -> riakc_pb_socket:is_connected(Pid) end,
    Opts = #{time_left => timer:seconds(10),
             sleep_time => 1000,
             name => wait_for_riak_connected_timeout},
    mongoose_helper:wait_until(Fun, true, Opts).
