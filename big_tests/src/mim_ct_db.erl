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
init_job_db(mysql, TestConfig = #{prefix := Prefix, hosts := Hosts, repo_dir := RepoDir}) ->
    [DbPort] = get_ports(mysql_port, TestConfig),
    setup_mysql_container(DbPort, Prefix ++ "_mim_db_" ++ integer_to_list(DbPort), RepoDir),
    TestConfig;
init_job_db(pgsql, TestConfig = #{prefix := Prefix, hosts := Hosts, repo_dir := RepoDir}) ->
    [DbPort] = get_ports(pgsql_port, TestConfig),
    setup_pgsql_container(DbPort, Prefix ++ "_mim_db_" ++ integer_to_list(DbPort), RepoDir),
    TestConfig;
init_job_db(riak, TestConfig = #{prefix := Prefix, hosts := Hosts, repo_dir := RepoDir}) ->
    [RiakPort] = get_ports(riak_port, TestConfig),
    [RiakPbPort] = get_ports(riak_pb_port, TestConfig),
    setup_riak_container(RiakPort, RiakPbPort, Prefix ++ "_mim_db_" ++ integer_to_list(RiakPort), RepoDir, TestConfig);
init_job_db(ldap, TestConfig = #{job_number := JobNumber}) ->
    %% Use different ldap_base for each job
    set_option(ldap_prefix, integer_to_list(JobNumber), TestConfig);
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
init_db(riak, _RepoDir) ->
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

setup_riak_container(RiakPort, RiakPbPort, Prefix, RepoDir, TestConfig) ->
    Envs = #{"DB" => "riak", 
            "RIAK_PORT" => integer_to_list(RiakPort),
            "RIAK_PB_PORT" => integer_to_list(RiakPbPort), 
            "DB_PREFIX" => "mim-ct1-" ++ Prefix},
    CmdOpts = #{env => Envs, cwd => RepoDir},
    {done, _, Result} = mim_ct_sh:run([filename:join([RepoDir, "tools", "travis-setup-db.sh"])], CmdOpts),
    io:format("Setup riak container ~p returns ~ts~n", [RiakPort, Result]),
    TestConfig#{riak_container_name => "mim-ct1-" ++ Prefix ++ "-riak"}.

print_job_logs(TestConfig = #{preset := Preset}) ->
    Dbs = preset_to_databases(Preset, TestConfig),
    print_job_logs_for_dbs(Dbs, TestConfig).

print_job_logs_for_dbs(Dbs, TestConfig) ->
    [print_job_logs_for_db(Db, TestConfig) || Db <- Dbs],
    ok.

print_job_logs_for_db(riak, TestConfig = #{riak_container_name := RiakContainer, repo_dir := RepoDir}) ->
    print_container_logs(RiakContainer),
    print_riak_logs_from_disk(RepoDir, RiakContainer),
    ok;
print_job_logs_for_db(_Db, _TestConfig) ->
    ok.

print_container_logs(Container) ->
    {done, _, Result} = mim_ct_sh:run(["docker", "logs", Container], #{}),
    F = fun() -> catch io:format("~n~ts~n", [Result]) end,
    mim_ct_helper:travis_fold("DbLog", "Log from " ++ Container, F).

print_riak_logs_from_disk(RepoDir, RiakContainer) ->
    LogDirDest = db_log_dir(RepoDir, RiakContainer),
    ok = filelib:ensure_dir(filename:join(LogDirDest, "ok")),
    {done, _, Result} = mim_ct_sh:run(["docker", "cp", RiakContainer ++ ":/var/log/riak/", LogDirDest], #{}),
    io:format("docker cp returns ~ts~n", [Result]),
    Logs = list_files(LogDirDest),
    [print_log_file(RiakContainer, LogFile) || LogFile <- Logs],
    ok.

list_files(Dir) ->
    {ok, Files} = file:list_dir(Dir),
    [File || File <- Files, filelib:is_file(File)].

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
