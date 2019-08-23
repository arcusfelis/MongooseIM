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
    [DbPort] = get_ports(mysql_port, TestConfig),
    setup_pgsql_container(DbPort, Prefix ++ "_mim_db_" ++ integer_to_list(DbPort)),
    TestConfig;
init_job_db(pgsql, TestConfig = #{prefix := Prefix, hosts := Hosts}) ->
    [DbPort] = get_ports(pgsql_port, TestConfig),
    setup_pgsql_container(DbPort, Prefix ++ "_mim_db_" ++ integer_to_list(DbPort)),
    TestConfig;
init_job_db(riak, TestConfig = #{prefix := Prefix, hosts := Hosts}) ->
    [RiakPort] = get_ports(riak_port, TestConfig),
    [RiakPbPort] = get_ports(riak_pb_port, TestConfig),
    setup_riak_container(RiakPort, RiakPbPort, Prefix ++ "_mim_db_" ++ integer_to_list(RiakPort)),
    TestConfig;
init_job_db(redis, TestConfig = #{job_number := JobNumber, hosts := Hosts}) ->
    Hosts2 = [{HostId, lists:keystore(redis_database, 1, Host, {redis_database, JobNumber})} || {HostId, Host} <- Hosts],
    TestConfig#{hosts => Hosts2};
init_job_db(Db, TestConfig) ->
    io:format("init_job_db: Do nothing for db ~p~n", [Db]),
    TestConfig.

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

init_db(mysql) ->
    {done, 0, "skip for master"};
init_db(pgsql) ->
    {done, 0, "skip for master"};
init_db(riak) ->
    {done, 0, "skip for master"};
init_db(DbType) ->
    RepoDir = path_helper:repo_dir([]),
    mim_ct_sh:run([filename:join([RepoDir, "tools", "travis-setup-db.sh"])], #{env => #{"DB" => atom_to_list(DbType), "DB_PREFIX" => "mim-ct1"}, cwd => RepoDir}).

setup_mssql_database(DbName) ->
    RepoDir = path_helper:repo_dir([]),
    mim_ct_sh:run([filename:join([RepoDir, "tools", "setup-mssql-database.sh"])], #{env => #{"DB_NAME" => DbName, "DB_PREFIX" => "mim-ct1"}, cwd => RepoDir}).

setup_mysql_container(DbPort, Prefix) ->
    RepoDir = path_helper:repo_dir([]),
    Envs = #{"DB" => "mysql", "MYSQL_PORT" => integer_to_list(DbPort), "DB_PREFIX" => "mim-ct1-" ++ Prefix},
    CmdOpts = #{env => Envs, cwd => RepoDir},
    {done, _, Result} = mim_ct_sh:run([filename:join([RepoDir, "tools", "travis-setup-db.sh"])], CmdOpts),
    io:format("Setup mysql container ~p returns ~ts~n", [DbPort, Result]),
    ok.

setup_pgsql_container(DbPort, Prefix) ->
    RepoDir = path_helper:repo_dir([]),
    Envs = #{"DB" => "pgsql", "PGSQL_PORT" => integer_to_list(DbPort), "DB_PREFIX" => "mim-ct1-" ++ Prefix},
    CmdOpts = #{env => Envs, cwd => RepoDir},
    {done, _, Result} = mim_ct_sh:run([filename:join([RepoDir, "tools", "travis-setup-db.sh"])], CmdOpts),
    io:format("Setup pgsql container ~p returns ~ts~n", [DbPort, Result]),
    ok.

setup_riak_container(RiakPort, RiakPbPort, Prefix) ->
    RepoDir = path_helper:repo_dir([]),
    Envs = #{"DB" => "riak", 
            "RIAK_PORT" => integer_to_list(RiakPort),
            "RIAK_PB_PORT" => integer_to_list(RiakPbPort), 
            "DB_PREFIX" => "mim-ct1-" ++ Prefix},
    CmdOpts = #{env => Envs, cwd => RepoDir},
    {done, _, Result} = mim_ct_sh:run([filename:join([RepoDir, "tools", "travis-setup-db.sh"])], CmdOpts),
    io:format("Setup riak container ~p returns ~ts~n", [RiakPort, Result]),
    ok.
