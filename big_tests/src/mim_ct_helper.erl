-module(mim_ct_helper).
-export([report_time/2]).
-export([report_progress/2]).
-export([microseconds_to_string/1]).
-export([travis_fold/2]).

-export([before_start/0]).
-export([after_test/2]).

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

travis_fold(Description, Fun) ->
    case os:getenv("TRAVIS_JOB_ID") of
        false ->
            Fun();
        _ ->
            io:format("travis_fold:start:~ts~n", [Description]),
            Result = Fun(),
            io:format("travis_fold:end:~ts~n", [Description]),
            Result
    end.



before_start() ->
    #{before_start_dirs => ct_run_dirs()}.

after_test(CtResults, #{before_start_dirs := CTRunDirsBeforeRun}) ->
    Results = [format_result(Res) || Res <- CtResults],
    %% Waiting for messages to be flushed
    timer:sleep(50),
    CTRunDirsAfterRun = ct_run_dirs(),
    NewCTRunDirs = CTRunDirsAfterRun -- CTRunDirsBeforeRun,
    print_ct_summaries(NewCTRunDirs),
    ExitStatusByGroups = exit_status_by_groups(NewCTRunDirs),
    ExitStatusByTestCases = process_results(Results),
    case {ExitStatusByGroups, ExitStatusByTestCases} of
        {ok, ok} ->
            ok;
        Other ->
            {error, Other}
    end.

ct_run_dirs() ->
    filelib:wildcard("ct_report/ct_run*").

print_ct_summaries(CTRunDirs) ->
    [print_ct_summary(CTRunDir) || CTRunDir <- CTRunDirs],
    ok.

print_ct_summary(CTRunDir) ->
    case file:read_file(filename:join(CTRunDir, ct_summary)) of
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


format_result(Result) ->
    case Result of
        {error, Reason} ->
            {error, Reason};
        {Ok, Failed, {UserSkipped, AutoSkipped}} ->
            {ok, {Ok, Failed, UserSkipped, AutoSkipped}}
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
exit_code({Ok, Failed, _UserSkipped, AutoSkipped})
  when Ok == 0; Failed > 0; AutoSkipped > 0 ->
    {error, failed};
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
