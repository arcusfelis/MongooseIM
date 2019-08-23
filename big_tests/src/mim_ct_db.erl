-module(mim_ct_db).
-export([init_master/2]).
-export([init_job/1]).

init_master(MasterConfig = #{repo_dir := RepoDir}, JobConfigs) ->
    MasterDbs = get_databases(MasterConfig, JobConfigs),
    io:format("MasterDbs ~p~n", [MasterDbs]),
    F = fun(DbType) ->
            {Time, InitDbReturns} = timer:tc(fun() -> init_db(DbType, RepoDir) end),
            {_, _, Result} = InitDbReturns,
            io:format("DB ~p started:~n~ts~n", [DbType, Result]),
            mim_ct_helper:report_progress("Starting database ~p took ~ts~n",
                                          [DbType, mim_ct_helper:microseconds_to_string(Time)]),
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
    {Time, TestConfig2} = timer:tc(fun() -> init_job_db(Db, TestConfig) end),
    mim_ct_helper:report_progress("Starting database ~p took ~ts~n",
                                  [Db, mim_ct_helper:microseconds_to_string(Time)]),
    init_job_dbs(Dbs, TestConfig2);
init_job_dbs([], TestConfig) ->
    TestConfig.

init_job_db(mssql, TestConfig = #{prefix := Prefix, repo_dir := RepoDir}) ->
    DbName = Prefix ++ "_mim_db",
    setup_mssql_database(DbName, RepoDir),
    TestConfig#{mssql_database => DbName};
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
    setup_riak_container(RiakPort, RiakPbPort, Prefix ++ "_mim_db_" ++ integer_to_list(RiakPort), RepoDir),
    TestConfig;
init_job_db(ldap, TestConfig = #{prefix := Prefix, hosts := Hosts, repo_dir := RepoDir}) ->
    [LdapPort] = get_ports(ldap_port, TestConfig),
    [LdapSecurePort] = get_ports(ldap_secure_port, TestConfig),
    setup_ldap_container(LdapPort, LdapSecurePort, Prefix ++ "_mim_db_" ++ integer_to_list(LdapPort), RepoDir),
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

init_db(mysql, _RepoDir) ->
    {done, 0, "skip for master"};
init_db(pgsql, _RepoDir) ->
    {done, 0, "skip for master"};
init_db(riak, _RepoDir) ->
    {done, 0, "skip for master"};
init_db(ldap, _RepoDir) ->
    {done, 0, "skip for master"};
init_db(DbType, RepoDir) ->
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

setup_riak_container(RiakPort, RiakPbPort, Prefix, RepoDir) ->
    Envs = #{"DB" => "riak", 
            "RIAK_PORT" => integer_to_list(RiakPort),
            "RIAK_PB_PORT" => integer_to_list(RiakPbPort), 
            "DB_PREFIX" => "mim-ct1-" ++ Prefix},
    CmdOpts = #{env => Envs, cwd => RepoDir},
    {done, _, Result} = mim_ct_sh:run([filename:join([RepoDir, "tools", "travis-setup-db.sh"])], CmdOpts),
    io:format("Setup riak container ~p returns ~ts~n", [RiakPort, Result]),
    ok.

setup_ldap_container(LdapPort, LdapSecurePort, Prefix, RepoDir) ->
    Envs = #{"DB" => "ldap", 
            "LDAP_PORT" => integer_to_list(LdapPort),
            "LDAP_SECURE_PORT" => integer_to_list(LdapSecurePort), 
            "DB_PREFIX" => "mim-ct1-" ++ Prefix},
    CmdOpts = #{env => Envs, cwd => RepoDir},
    {done, _, Result} = mim_ct_sh:run([filename:join([RepoDir, "tools", "travis-setup-db.sh"])], CmdOpts),
    io:format("Setup ldap container ~p returns ~ts~n", [LdapPort, Result]),
    ok.
