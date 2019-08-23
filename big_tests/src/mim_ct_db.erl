-module(mim_ct_db).
-export([init_master/2]).
-export([init_job/1]).

init_master(MasterConfig, JobConfigs) ->
    MasterDbs = get_databases(MasterConfig, JobConfigs),
    io:format("MasterDbs ~p~n", [MasterDbs]),
    Results = [init_db(DbType) || DbType <- MasterDbs],
    [io:format("DB ~p started:~n~ts~n", [DbType, Result]) || {DbType, {_, _, Result}} <- lists:zip(MasterDbs, Results)],
    {MasterConfig, JobConfigs}.

init_job(TestConfig = #{preset := Preset}) ->
    Dbs = preset_to_databases(Preset, TestConfig),
    init_job_dbs(Dbs, TestConfig);
init_job(TestConfig) ->
    TestConfig.

init_job_dbs([Db|Dbs], TestConfig) ->
    TestConfig2 = init_job_db(Db, TestConfig),
    init_job_dbs(Dbs, TestConfig2);
init_job_dbs([], TestConfig) ->
    TestConfig.

init_job_db(mssql, TestConfig = #{prefix := Prefix}) ->
    DbName = Prefix ++ "_mim_db",
    setup_mssql_database(DbName),
    TestConfig#{mssql_database => DbName};
init_job_db(mysql, TestConfig = #{prefix := Prefix, hosts := Hosts}) ->
    %% Usually just one port at this point, already rewritten
    %% But can be no ports (then do nothing)
    %% Or more than one ???
    MysqlPorts = lists:delete(none, lists:usort([proplists:get_value(mysql_port, Host, none) || {_HostId, Host} <- Hosts])),
    io:format("MysqlPorts ~p~n", [MysqlPorts]),
    [setup_mysql_container(DbPort, Prefix ++ "_mim_db_" ++ integer_to_list(DbPort)) || DbPort <- MysqlPorts],
    TestConfig;
init_job_db(pgsql, TestConfig = #{prefix := Prefix, hosts := Hosts}) ->
    PgsqlPorts = lists:delete(none, lists:usort([proplists:get_value(pgsql_port, Host, none) || {_HostId, Host} <- Hosts])),
    io:format("PgsqlPorts ~p~n", [PgsqlPorts]),
    [setup_pgsql_container(DbPort, Prefix ++ "_mim_db_" ++ integer_to_list(DbPort)) || DbPort <- PgsqlPorts],
    TestConfig;
init_job_db(Db, TestConfig) ->
    io:format("init_job_db: Do nothing for db ~p~n", [Db]),
    TestConfig.

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

init_db(mysql) ->
    {done, 0, "skip"};
init_db(pgsql) ->
    {done, 0, "skip"};
init_db(DbType) ->
    RepoDir = path_helper:repo_dir([]),
    mim_ct_sh:run([filename:join([RepoDir, "tools", "travis-setup-db.sh"])], #{env => #{"DB" => atom_to_list(DbType), "DB_PREFIX" => "mim-ct1"}, cwd => RepoDir}).

setup_mssql_database(DbName) ->
    RepoDir = path_helper:repo_dir([]),
    mim_ct_sh:run([filename:join([RepoDir, "tools", "setup-mssql-database.sh"])], #{env => #{"DB_NAME" => DbName, "DB_PREFIX" => "mim-ct1"}, cwd => RepoDir}).

setup_mysql_container(DbPort, Prefix) ->
    RepoDir = path_helper:repo_dir([]),
    Envs =#{env => #{"DB" => "mysql", "MYSQL_PORT" => integer_to_list(DbPort), "DB_PREFIX" => "mim-ct1-" ++ Prefix}, cwd => RepoDir},
    {done, _, Result} = mim_ct_sh:run([filename:join([RepoDir, "tools", "travis-setup-db.sh"])], Envs),
    io:format("Setup mysql container ~p returns ~ts~n", [DbPort, Result]),
    ok.

setup_pgsql_container(DbPort, Prefix) ->
    RepoDir = path_helper:repo_dir([]),
    Envs =#{env => #{"DB" => "pgsql", "PGSQL_PORT" => integer_to_list(DbPort), "DB_PREFIX" => "mim-ct1-" ++ Prefix}, cwd => RepoDir},
    {done, _, Result} = mim_ct_sh:run([filename:join([RepoDir, "tools", "travis-setup-db.sh"])], Envs),
    io:format("Setup pgsql container ~p returns ~ts~n", [DbPort, Result]),
    ok.
