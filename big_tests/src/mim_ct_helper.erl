-module(mim_ct_helper).
-export([report_time/2]).
-export([report_progress/2]).
-export([microseconds_to_string/1]).
-export([travis_fold/3]).
-export([buffer_fold/4]).

-export([before_start/0]).
-export([after_test/3]).

-include_lib("exml/include/exml.hrl").

report_time(Description, Fun) ->
    report_progress("~nExecuting ~ts~n", [Description]),
    Start = os:timestamp(),
    try
        Fun()
    after
        Microseconds = timer:now_diff(os:timestamp(), Start),
        Time = microseconds_to_string(Microseconds),
        report_progress("~ts took ~ts~n", [Description, Time])
    end.

microseconds_to_string(Microseconds) ->
    Milliseconds = Microseconds div 1000,
    SecondsFloat = Milliseconds / 1000,
    io_lib:format("~.3f seconds", [SecondsFloat]).

%% Writes onto travis console directly
report_progress(Format, Args) ->
    Message = io_lib:format(Format, Args),
    file:write_file("/tmp/progress", Message, [append]).

is_travis() ->
    false =/= os:getenv("TRAVIS_JOB_ID").

travis_fold(Id, Description, Fun) ->
    case is_travis() of
        false ->
            Fun();
        true ->
            io:format("travis_fold:start:~ts~n", [Id]),
            io:format("~ts~n", [Description]),
            Result = Fun(),
            io:format("travis_fold:end:~ts~n", [Id]),
            Result
    end.

%% Forward output into file, read and print it all at once after
buffer_fold(Id, Description, Fun, FilenameOut) ->
    case buffer_fold_enabled() of
        false ->
            Fun();
        true ->
            OldGroupLeader = get_group_leader(),
            {ok, File} = file:open(FilenameOut, [write]),
            group_leader(File, self()),
            try
                Fun()
            after
                group_leader(OldGroupLeader, self()),
                file:close(File),
                {ok, Bin} = file:read_file(FilenameOut),
                travis_fold(Id, Description, fun() -> io:format("~ts", [Bin]) end)
            end
    end.

get_group_leader() ->
    {group_leader, GroupLeader} = erlang:process_info(self(), group_leader),
    GroupLeader.

buffer_fold_enabled() ->
    not is_verbose().

is_verbose() ->
    "1" =:= os:getenv("VERBOSE").

before_start() ->
    #{before_start_dirs => ct_run_dirs()}.

after_test(CtResults, TestConfigs, #{before_start_dirs := CTRunDirsBeforeRun}) ->
    Results = [format_result(Res) || Res <- CtResults],
    %% Waiting for messages to be flushed
    timer:sleep(50),
    CTRunDirsAfterRun = ct_run_dirs(),
    NewCTRunDirs = CTRunDirsAfterRun -- CTRunDirsBeforeRun,
    print_ct_skips(NewCTRunDirs),
    print_ct_summaries(NewCTRunDirs),
    ExitStatusByGroups = exit_status_by_groups(NewCTRunDirs),
    ExitStatusByTestCases = process_results(Results),
    case {ExitStatusByGroups, ExitStatusByTestCases} of
        {ok, ok} ->
            ok;
        Other ->
            FailedTestConfigs = failed_test_configs(Results, TestConfigs, NewCTRunDirs),
            [mim_ct_error:print_errors(TestConfig) || TestConfig <- FailedTestConfigs],
            [print_stanza_logs(CTRunDir) || CTRunDir <- NewCTRunDirs],
            [maybe_print_all_groups_state(CTRunDir) || CTRunDir <- NewCTRunDirs],
            concat_ct_markdown(NewCTRunDirs),
            print_failed_test_configs(FailedTestConfigs),
            maybe_print_mim_logs(FailedTestConfigs),
            [mim_ct_db:print_job_logs(TestConfig) || TestConfig <- FailedTestConfigs],
            {error, #{exit_status_by_groups => ExitStatusByGroups,
                      exit_status_by_cases => ExitStatusByTestCases}}
    end.

failed_test_configs(Results, TestConfigs, NewCTRunDirs) ->
    ExitCodes = exit_status_per_test(Results, TestConfigs, NewCTRunDirs),
    Zip = lists:zip(ExitCodes, TestConfigs),
    [TestConfig || { {error, _}, TestConfig } <- Zip].

exit_status_per_test(Results, TestConfigs, NewCTRunDirs) ->
    [exit_status_per_test1(Result, TestConfig, NewCTRunDirs)
     || {Result, TestConfig} <- lists:zip(Results, TestConfigs)].

exit_status_per_test1(Result, TestConfig, NewCTRunDirs) ->
    ExitStatusByTestCases = process_results([Result]),
    case find_ct_dir(Result, TestConfig, NewCTRunDirs) of
        [] ->
            io:format("exit_status_per_test1: no_ct_dir~n", []),
            {error, no_ct_dir};
        CtDirs ->
            ExitStatusByGroups = exit_status_by_groups(CtDirs),
            case {ExitStatusByGroups, ExitStatusByTestCases} of
                {ok, ok} ->
                    ok;
                _ ->
                    {error, #{exit_status_by_groups => ExitStatusByGroups,
                              exit_status_by_cases => ExitStatusByTestCases}}
            end
    end.

test_node(#{slave_node := Slave}) -> Slave;
test_node(_) -> test.

%% TODO write unique test_id into log dirs and use this in future
find_ct_dir(Result, TestConfig, NewCTRunDirs) ->
    TestNode = test_node(TestConfig),
    JobCtRuns = filelib:wildcard("ct_report/ct_run." ++ atom_to_list(TestNode) ++ "*"),
    OrdJobCtRuns = ordsets:from_list(JobCtRuns),
    OrdNewCTRunDirs = ordsets:from_list(JobCtRuns),
    ordsets:union(OrdJobCtRuns, OrdNewCTRunDirs).

ct_run_dirs() ->
    filelib:wildcard("ct_report/ct_run*").

print_ct_summaries(CTRunDirs) ->
    [print_ct_summary(CTRunDir) || CTRunDir <- CTRunDirs],
    ok.

print_ct_summary(CTRunDir) ->
    case file:read_file(filename:join(CTRunDir, ct_summary_time)) of
        {ok, <<>>} ->
            ok;
        {ok, Bin} ->
            io:format("~n==========================~n~n", []),
            io:format("print_ct_summary ~ts:~n~n~ts", [CTRunDir, Bin]),
            io:format("~n==========================~n", []),
            ok;
        _ ->
            ok
    end.

print_ct_skips(CTRunDirs) ->
    [print_ct_skip(CTRunDir) || CTRunDir <- CTRunDirs],
    ok.

print_ct_skip(CTRunDir) ->
    case file:read_file(filename:join(CTRunDir, ct_skip_info)) of
        {ok, <<>>} ->
            ok;
        {ok, Bin} ->
            travis_fold("ct_skip_info", "print_ct_skip " ++ CTRunDir, fun() ->
                    io:format("~n==========================~n~n", []),
                    io:format("print_ct_skip ~ts:~n~n~ts", [CTRunDir, Bin]),
                    io:format("~n==========================~n", [])
                end);
        _ ->
            ok
    end.

exit_status_by_groups(CTRunDirs) ->
    case CTRunDirs of
        [] ->
            io:format("WARNING: ct_run directory has not been created~n",  []),
            ok;
        [_|_] ->
            Results = [anaylyze_groups_runs(CTRunDir) || CTRunDir <- CTRunDirs],
            case [X || X <- Results, X =/= ok] of
                [] ->
                    ok;
                Failed ->
                    {error, Failed}
            end
    end.

anaylyze_groups_runs(CTRunDir) ->
    case file:consult(CTRunDir ++ "/all_groups.summary") of
        {ok, Terms} ->
            case proplists:get_value(total_failed, Terms, undefined) of
                undefined ->
                    ok;
                0 ->
                    ok;
                Failed ->
                    {error, {total_failed, Failed}}
            end;
      {error, Error} ->
            error_logger:error_msg("Error reading all_groups.summary: ~p~n", [Error]),
            ok
    end.

maybe_print_all_groups_state(CTRunDir) ->
    File = CTRunDir ++ "/all_groups.state",
    case file:read_file(File) of
        {ok, Bin} ->
            mim_ct_helper:travis_fold("all_groups.state", "File " ++ File, fun() ->
                    catch io:format("~ts:~n~ts~n", [File, Bin])
                end);
        _ ->
            ok
    end.

format_result(Result) ->
    case Result of
        {error, Reason} ->
            {error, Reason};
        {Ok, Failed, {UserSkipped, AutoSkipped}} ->
            {ok, {Ok, Failed, UserSkipped, AutoSkipped}};
        Other ->
            {error, {unknown_error, Other}}
    end.

process_results(CTResults) ->
    Ok = 0,
    Failed = 0,
    UserSkipped = 0,
    AutoSkipped = 0,
    Errors = [],
    process_results(CTResults, {{Ok, Failed, UserSkipped, AutoSkipped}, Errors}).

process_results([], {StatsAcc, Errors}) ->
    write_stats_into_vars_file(StatsAcc),
    print_errors(Errors),
    print_stats(StatsAcc),
    exit_code(StatsAcc);
process_results([ {ok, RunStats} | T ], {StatsAcc, Errors}) ->
    process_results(T, {add(RunStats, StatsAcc), Errors});
process_results([ Error | T ], {StatsAcc, Errors}) ->
    process_results(T, {StatsAcc, [Error | Errors]}).

print_errors(Errors) ->
    [ print(standard_error, "~p~n", [E]) || E <- Errors ].

print_stats({Ok, Failed, _UserSkipped, AutoSkipped}) ->
    print(standard_error, "Tests:~n", []),
    Ok == 0 andalso print(standard_error,         "  ok          : ~b~n", [Ok]),
    Failed > 0 andalso print(standard_error,      "  failed      : ~b~n", [Failed]),
    AutoSkipped > 0 andalso print(standard_error, "  auto-skipped: ~b~n", [AutoSkipped]).

format_stats_as_vars({Ok, Failed, UserSkipped, AutoSkipped}) ->
    io_lib:format("CT_COUNTER_OK=~p~n"
                  "CT_COUNTER_FAILED=~p~n"
                  "CT_COUNTER_USER_SKIPPED=~p~n"
                  "CT_COUNTER_AUTO_SKIPPED=~p~n",
                  [Ok, Failed, UserSkipped, AutoSkipped]).


write_stats_into_vars_file(Stats) ->
    file:write_file("/tmp/ct_stats_vars", [format_stats_as_vars(Stats)]).

%% Fail if there are failed test cases, auto skipped cases,
%% or the number of passed tests is 0 (which is also strange - a misconfiguration?).
%% StatsAcc is similar (Skipped are not a tuple) to the success result from ct:run_test/1:
%%
%%     {Ok, Failed, UserSkipped, AutoSkipped}
%%
exit_code({Ok, Failed, UserSkipped, AutoSkipped})
  when Ok == 0; Failed > 0; AutoSkipped > 0 ->
    {error, #{ok => Ok, failed => Failed, user_skipped => UserSkipped, auto_skipped => AutoSkipped}};
exit_code({_, _, _, _}) ->
    ok.

print(Handle, Fmt, Args) ->
    io:format(Handle, Fmt, Args).


add({X1, X2, X3, X4},
    {Y1, Y2, Y3, Y4}) ->
    {X1 + Y1,
     X2 + Y2,
     X3 + Y3,
     X4 + Y4}.

print_stanza_logs(CTRunDir) ->
    StanzaFiles = filelib:wildcard(CTRunDir ++ "/*/*/log_private/*.xml"),
    MaxFiles = 10,
    StanzaFiles2 = lists:sublist(StanzaFiles, 1, MaxFiles),
    [print_stanza_file(StanzaFile) || StanzaFile <- StanzaFiles2].

print_stanza_file(StanzaFile) ->
    {ok,Bin}  = file:read_file(StanzaFile),
    ParsingResult = exml:parse(Bin),
    print_stanza_file(StanzaFile, Bin, ParsingResult).

print_stanza_file(StanzaFile, _Bin, {ok, HistoryElem}) ->
    #xmlel{name = <<"history">>, children = StanzaElems} = HistoryElem,
    Pretty = exml:to_pretty_iolist(StanzaElems),
    Description = filename:basename(StanzaFile),
    Fun = fun() -> io:format("~ts", [reindent(iolist_to_binary(Pretty))]) end,
    travis_fold("stanza.log", Description, Fun);
print_stanza_file(StanzaFile, <<>>, _Error) ->
    Description = filename:basename(StanzaFile) ++ " - empty",
    Fun = fun() -> ok end,
    travis_fold("stanza.log", Description, Fun);
print_stanza_file(StanzaFile, Bin, Error) ->
    Description = filename:basename(StanzaFile) ++ " - failed",
    Fun = fun() -> io:format("Bin = ~p~nError = ~p~n", [Bin, Error]) end,
    travis_fold("stanza.log", Description, Fun).

reindent(Bin) ->
    binary:replace(Bin, <<$\t>>, <<"  ">>, [global]).

%% Merge ct_markdown files.
%% We just simulate the old behaviour with one file.
%% Later we can rewrite tools/travis-publish-github-comment.sh into erlang.
concat_ct_markdown(NewCTRunDirs) ->
    Results = [file:read_file(filename:join(CTRunDir, "ct_markdown")) || CTRunDir <- NewCTRunDirs],
    Truncated = lists:sum([read_truncated_value(filename:join(CTRunDir, "ct_markdown_truncated")) || CTRunDir <- NewCTRunDirs]),
    Data = [Bin || {ok, Bin} <- Results],
    ok = file:write_file("/tmp/ct_markdown", Data),
    file:delete("/tmp/ct_markdown_truncated"),
    case Truncated of
        0 ->
            ok;
        _ ->
            ok = file:write_file("/tmp/ct_markdown_truncated", Data)
    end,
    ok.

read_truncated_value(TrFile) ->
    case file:read_file(TrFile) of
        {ok, Bin} ->
            binary_to_integer(Bin);
        _ ->
            0
    end.

print_failed_test_configs(FailedTestConfigs) ->
    FailedTestSpecs = [TestSpec || #{test_spec := TestSpec} <- FailedTestConfigs],
    io:format("FailedTestConfigs=~p~n", [FailedTestSpecs]).

maybe_print_mim_logs(TestConfigs) ->
    case is_travis() of
        true ->
            [mim_node:print_node_logs(Host) || Host <- all_hosts(TestConfigs)];
        false ->
            ok
    end.

all_hosts(TestConfigs) ->
    [maps:from_list(Host) || #{hosts := Hosts} <- TestConfigs, {_, Host} <- Hosts].
