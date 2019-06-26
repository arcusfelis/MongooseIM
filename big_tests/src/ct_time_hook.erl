%%% @doc Writes test start stop time.
%%% Based on example cth http://erlang.org/doc/apps/common_test/ct_hooks_chapter.html#example-cth
-module(ct_time_hook).

%% Callbacks
 -export([id/1]).
 -export([init/2]).

 -export([pre_init_per_suite/3]).
 -export([post_init_per_suite/4]).
 -export([pre_end_per_suite/3]).
 -export([post_end_per_suite/4]).

 -export([pre_init_per_group/4]).
 -export([post_init_per_group/5]).
 -export([pre_end_per_group/4]).
 -export([post_end_per_group/5]).

 -export([pre_init_per_testcase/4]).
 -export([post_init_per_testcase/5]).
 -export([pre_end_per_testcase/4]).
 -export([post_end_per_testcase/5]).

 -export([on_tc_fail/4]).
 -export([on_tc_skip/4]).

 -export([terminate/1]).

 -record(state, { file_handle, total, suite_total, ts, tcs, data }).

 %% Return a unique id for this CTH.
 id(Opts) ->
   proplists:get_value(filename, Opts, "/tmp/ct_time_hook.log").

 %% Always called before any other callback function. Use this to initiate
 %% any common state. 
 init(Id, Opts) ->
     {ok,D} = file:open(Id,[write]),
     {ok, #state{ file_handle = D, total = 0, data = [] }}.

 %% Called before init_per_suite is called.
 pre_init_per_suite(Suite,Config,State) ->
     {Config, State#state{ suite_total = 0, tcs = [] }}.

 %% Called after init_per_suite.
 post_init_per_suite(Suite,Config,Return,State) ->
     {Return, State}.

 %% Called before end_per_suite.
 pre_end_per_suite(Suite,Config,State) ->
     {Config, State}.

 %% Called after end_per_suite.
 post_end_per_suite(Suite,Config,Return,State) ->
     Data = {suites, Suite, State#state.suite_total, lists:reverse(State#state.tcs)},
     {Return, State#state{ data = [Data | State#state.data] ,
                           total = State#state.total + State#state.suite_total } }.

 %% Called before each init_per_group.
 pre_init_per_group(Suite,Group,Config,State) ->
     {Config, State}.

 %% Called after each init_per_group.
 post_init_per_group(Suite,Group,Config,Return,State) ->
     {Return, State}.

 %% Called before each end_per_group.
 pre_end_per_group(Suite,Group,Config,State) ->
     {Config, State}.

 %% Called after each end_per_group.
 post_end_per_group(Suite,Group,Config,Return,State) ->
     {Return, State}.

 %% Called before each init_per_testcase.
 pre_init_per_testcase(Suite,TC,Config,State) ->
     {Config, State#state{ ts = os:timestamp(), total = State#state.suite_total + 1 } }.

 %% Called after each init_per_testcase (immediately before the test case).
 post_init_per_testcase(Suite,TC,Config,Return,State) ->
     {Return, State}.

%% Called before each end_per_testcase (immediately after the test case).
pre_end_per_testcase(Suite,TC,Config,State) ->
     {Config, State}.

 %% Called after each end_per_testcase.
 post_end_per_testcase(Suite,TC,Config, ok, State = #state{ ts = {_,_,_}}) ->
     TCInfo = {testcase, Suite, TC, State#state.ts, timer:now_diff(os:timestamp(), State#state.ts)},
     {Return, State#state{ ts = undefined, tcs = [TCInfo | State#state.tcs] } };
 post_end_per_testcase(Suite,TC,Config,Return,State) ->
     {Return, State#state{ ts = undefined}}.

 %% Called after post_init_per_suite, post_end_per_suite, post_init_per_group,
 %% post_end_per_group and post_end_per_testcase if the suite, group or test case failed.
 on_tc_fail(Suite, TC, Reason, State) ->
     State.

 %% Called when a test case is skipped by either user action
 %% or due to an init function failing.  
 on_tc_skip(Suite, TC, Reason, State) ->
     State.

 %% Called when the scope of the CTH is done
 terminate(State) ->
     io:format(State#state.file_handle, "~p.~n",
                [{test_run, State#state.total, State#state.data}]),
     file:close(State#state.file_handle),
     ok.
