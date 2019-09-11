%% During dev you would use something similar to:
%% TEST_HOSTS="mim" ./tools/travis-test.sh -c false -s false -p odbc_mssql_mnesia
%%
%% If you also want to start just mim1 node use:
%% DEV_NODES="mim1" TEST_HOSTS="mim" ./tools/travis-test.sh -c false -s false -p odbc_mssql_mnesia
%%
%% TEST_HOSTS variable contains host names from hosts in big_tests/test.config.
%% DEV_NODES variable contains release names from profiles in rebar.config.
%% Release names are also used to name directories in the _build directory.
%%
%% Valid TEST_HOSTS are mim, mim2, mim3, fed, reg.
%% Valid DEV_NODES are mim1, mim2, mim3, fed1, reg1.
%%
%% Example with two nodes:
%% DEV_NODES="mim1 mim2" TEST_HOSTS="mim mim2" ./tools/travis-test.sh -c false -s false -p odbc_mssql_mnesia
%%
%% Environment variable PRESET_ENABLED is true by default.
%% PRESET_ENABLED=false disables preset application and forces to run
%% one preset.
-module(run_common_test).

-export([main/1]).
-import(mim_ct_helper, [report_time/2, report_progress/2]).


%%
%% Entry %% 
-record(opts, {test,
               spec,
               cover,
               preset = all}).

%% Accepted options formatted as:
%% {opt_name, opt_index_in_opts_record, fun value_sanitizer/1}.
%% -spec value_sanitizer(string()) -> NewValue :: any().
opts() ->
    [{test,   #opts.test,   fun quick_or_full/1},
     {spec,   #opts.spec,   fun list_to_atom/1},
     {cover,  #opts.cover,  fun parse_bool/1},
     {preset, #opts.preset, fun preset/1}].

%% Raw args are 'key=val' atoms.
%% Args are {key :: atom(), val :: string()} pairs.
%% "=" is an invalid character in option name or value.
main(RawArgs) ->
    Args = [raw_to_arg(Raw) || Raw <- RawArgs],
    Opts = apply_preset_enabled(args_to_opts(Args)),
    try
        run(Opts)
    catch Type:Reason ->
        Stacktrace = erlang:get_stacktrace(),
        io:format("TEST CRASHED~n Error type: ~p~n Reason: ~p~n Stacktrace:~n~p~n",
                               [Type, Reason, Stacktrace]),
        error_logger:error_msg("TEST CRASHED~n Error type: ~p~n Reason: ~p~n Stacktrace:~n~p~n",
                               [Type, Reason, Stacktrace]),
        %% Waiting for messages to be flushed
        timer:sleep(5000),
        init:stop("run_common_test:main/1 crashed")
    end.

run(#opts{test = quick, cover = Cover, spec = Spec}) ->
    error(todo);
run(#opts{test = full, spec = Spec, preset = [Preset|_], cover = Cover}) when is_boolean(Cover) ->
    Master = #{cover_enabled => Cover, cover_lib_dir => "_build/mim1/lib/mongooseim/ebin/",
               repo_dir => path_helper:repo_dir([]), auto_compile => auto_compile()},
    Job1 = #{test_spec => "default1.spec", test_config => "test.config", test_config_out => "_build/test1.config",
                         first_port => 6000, preset => Preset, slave_node => ct1, prefix => "ng1", username_suffix => <<"1j">>},
    Job2 = #{test_spec => "default2.spec", test_config => "test.config", test_config_out => "_build/test2.config",
                         first_port => 7000, preset => Preset, slave_node => ct2, prefix => "ng2", username_suffix => <<"2j">>},
    Job3 = #{test_spec => "default3.spec", test_config => "test.config", test_config_out => "_build/test3.config",
                         first_port => 8000, preset => Preset, slave_node => ct3, prefix => "ng3", username_suffix => <<"3j">>},
    Jobs = case Spec of
               'default.spec' ->
                   %% Three workers for the default spec
                   [Job1, Job2, Job3];
               _ ->
                   [Job1#{test_spec => atom_to_list(Spec)}]
           end,
    Result = mim_ct:run_jobs(Master, Jobs),
    case Result of
    {ok, _} ->
        init:stop(0);
    {{error, Failed}, _} ->
        io:format("run_common_test finishes~nFailed:~n~p~n", [Failed]),
        init:stop(1)
    end.

apply_preset_enabled(#opts{} = Opts) ->
    case os:getenv("PRESET_ENABLED") of
        "false" ->
            io:format("PRESET_ENABLED is set to false, enabling quick mode~n"),
            Opts#opts{test = quick};
        _ ->
            Opts
    end.


%%
%% Helpers
%%

repo_dir() ->
    case os:getenv("REPO_DIR") of
        false ->
            init:stop("Environment variable REPO_DIR is undefined");
        Value ->
            Value
    end.

args_to_opts(Args) ->
    {Args, Opts} = lists:foldl(fun set_opt/2, {Args, #opts{}}, opts()),
    Opts.

raw_to_arg(RawArg) ->
    ArgVal = atom_to_list(RawArg),
    case string:tokens(ArgVal, "=") of
        [Arg, Val] ->
            {list_to_atom(Arg), Val};
        [Arg] ->
            {list_to_atom(Arg), ""}
    end.

set_opt({Opt, Index, Sanitizer}, {Args, Opts}) ->
    Value = Sanitizer(proplists:get_value(Opt, Args)),
    {Args, setelement(Index, Opts, Value)}.

quick_or_full("quick") -> quick;
quick_or_full("full")  -> full.

preset(undefined) -> undefined;
preset(PresetList) ->
    [list_to_atom(Preset) || Preset <- string:tokens(PresetList, " ")].

parse_bool("true") ->
    true;
parse_bool(_) ->
    false.

auto_compile() ->
    %% Tell Common Tests that it should not compile any more files
    %% (they are compiled by rebar)
    case os:getenv("SKIP_AUTO_COMPILE") of
        "true" ->
            false;
        _ ->
            true
    end.

