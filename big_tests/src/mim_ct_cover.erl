-module(mim_ct_cover).
-export([start_cover/1]).
-export([analyze_cover/1]).
-export([add_cover_node_to_hosts/1]).

start_cover(TestConfig = #{cover_node := CoverNode}) ->
    io:format("Cover is already started at node ~p~n", [CoverNode]),
    TestConfig;
start_cover(TestConfig = #{repo_dir := RepoDir, cover_enabled := true, cover_lib_dir := Dir}) ->
    Dir2 = filename:absname(Dir, RepoDir),
    code:add_path(Dir2),
    cover:start([]),
    {Time, Compiled} = timer:tc(fun() ->
                            Results = cover:compile_beam_directory(Dir2),
                            Ok = [X || X = {ok, _} <- Results],
                            NotOk = Results -- Ok,
                            #{ok => length(Ok), failed => NotOk}
                        end),
    mim_ct_helper:travis_fold("cover.out", "cover compiled output", fun() ->
            io:format("cover: compiled ~p~n", [Compiled])
        end),
    mim_ct_helper:report_progress("~nCover compilation took ~ts~n",
                                  [mim_ct_helper:microseconds_to_string(Time)]),
    add_cover_node_to_config(node(), TestConfig);
start_cover(TestConfig = #{}) ->
    io:format("cover disabled", []),
    TestConfig.

analyze_cover(TestConfig = #{repo_dir := RepoDir, cover_enabled := true}) ->
    %% Import small tests cover
    Files = filelib:wildcard(RepoDir ++ "/_build/**/cover/*.coverdata"),
    io:format("Files: ~p", [Files]),
    mim_ct_helper:report_time("Import cover data into run_common_test node", fun() ->
            [cover:import(File) || File <- Files]
        end),
    mim_ct_helper:report_time("Export merged cover data", fun() ->
            cover:export("/tmp/mongoose_combined.coverdata")
        end),
    case os:getenv("TRAVIS_JOB_ID") of
        false ->
            mim_cover_report:make_html(modules_to_analyze());
        _ ->
            ok
    end,
    case os:getenv("KEEP_COVER_RUNNING") of
        "1" ->
            io:format("Skip stopping cover~n"),
            ok;
        _ ->
            mim_ct_helper:report_time("Stopping cover on MongooseIM nodes", fun() ->
                        cover:stop(cover:which_nodes())
                    end)
    end,
    TestConfig;
analyze_cover(TestConfig) ->
    io:format("Skip analyze_cover~n", []),
    TestConfig.

modules_to_analyze() ->
    lists:usort(cover:imported_modules() ++ cover:modules()).

add_cover_node_to_config(Node, TestConfig) ->
    TestConfig#{cover_node => Node}.

add_cover_node_to_hosts(TestConfig = #{cover_enabled := true, cover_node := CoverNode}) ->
    add_opt_to_hosts(cover_node, CoverNode, TestConfig);
add_cover_node_to_hosts(TestConfig) ->
    io:format("Skip add_cover_node_to_hosts~n", []),
    TestConfig.

add_opt_to_hosts(OptName, OptValue, TestConfig = #{hosts := Hosts}) ->
    Hosts2 = [{Name, [{OptName, OptValue}|Props]} || {Name, Props} <- Hosts],
    TestConfig#{hosts => Hosts2}.
    
