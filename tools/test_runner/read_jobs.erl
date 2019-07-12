%% Takes env variables: 
%% - JOBS - limit to only jobs (or all by default)
-module(read_jobs).
-export([main/0]).
-export([main/1]).

main() ->
    main([]).

main(Args) ->
    {ok, Config} = file:consult("tools/test_runner/jobs.config"),
    Config2 = filter_jobs(Config, os:getenv("JOBS")),
    do(Args, Config2),
    erlang:halt().

%% ./read_jobs.sh list_erlang_versions
%% "20" "21"
do([list_erlang_versions|_], Config) ->
    print_values(lists:usort(list_erlang_versions(Config)));
do([list_jobs|_], Config) ->
    print_values(list_jobs(Config));
do([read_variables, Job], Config) ->
    read_variables(atom_to_list(Job), Config).

list_erlang_versions(Config) ->
    [maps:get("ERLANG_VERSION", Job) || Job <- Config].

list_jobs(Config) ->
    [maps:get("JOB", Job) || Job <- Config].

print_values(List) ->
    [io:format("~ts ", [P]) || P <- List].

filter_jobs(Config, false) ->
    Config;
filter_jobs(Config, "") ->
    Config;
filter_jobs(Config, ConcatenatedJobs) ->
    JobNames = string:tokens(ConcatenatedJobs, " "),
    [Job || Job = #{"JOB" := JobName} <- Config,
            lists:member(JobName, JobNames)].

read_variables(Job, Config) ->
    [JobConfig] = filter_jobs(Config, Job),
    io:format("~ts", [make_variables(JobConfig)]).

make_variables(KV) ->
    [io_lib:format("~s=~p~n", [K, V]) || {K,V} <- maps:to_list(KV)].
