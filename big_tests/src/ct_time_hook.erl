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

 -record(state, { file_handle, ts, suite_ts, events }).

 %% Return a unique id for this CTH.
 id(Opts) ->
   proplists:get_value(filename, Opts, "/tmp/ct_time_hook.log").

 %% Always called before any other callback function. Use this to initiate
 %% any common state. 
 init(Id, Opts) ->
     {ok,D} = file:open(Id,[write]),
     {ok, #state{ file_handle = D }}.

 %% Called before init_per_suite is called.
 pre_init_per_suite(Suite,Config,State) ->
     {Config, State#state{ events = [] }}.

 %% Called after init_per_suite.
 post_init_per_suite(Suite,Config,Return,State) ->
     {Return, State#state{ suite_ts = os:timestamp() }}.

 %% Called before end_per_suite.
 pre_end_per_suite(Suite,Config,State) ->
     {Config, State}.

 %% Called after end_per_suite.
 post_end_per_suite(Suite,Config,Return,State) ->
     Info = suite_info(Suite, State),
     Data = [Info | lists:reverse(State#state.events)],
     [io:format(State#state.file_handle, "~p.~n", [X]) || X <- Data],
     {Return, State#state{ events = [], suite_ts = undefined } }.

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
     {Config, State#state{ ts = os:timestamp() } }.

 %% Called after each init_per_testcase (immediately before the test case).
 post_init_per_testcase(Suite,TC,Config,Return,State) ->
     {Return, State}.

%% Called before each end_per_testcase (immediately after the test case).
pre_end_per_testcase(Suite,TC,Config,State) ->
     {Config, State}.

 %% Called after each end_per_testcase.
 post_end_per_testcase(Suite,TC,Config, Return, State) ->
     TCInfo = testcase_info(Suite, TC, State),
     {Return, State#state{events = [TCInfo | State#state.events] } }.

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
     file:close(State#state.file_handle),
     ok.

suite_info(Suite, State = #state{suite_ts = {_,_,_}}) ->
     {suite, Suite, State#state.suite_ts, timer:now_diff(os:timestamp(), State#state.suite_ts)};
suite_info(Suite, _State) ->
     {suite, Suite, {0,0,0}, 0}.

testcase_info(Suite, TC, State = #state{ts = {_,_,_}}) ->
     {testcase, Suite, TC, State#state.ts, timer:now_diff(os:timestamp(), State#state.ts)};
testcase_info(Suite, TC, _State) ->
     {testcase, Suite, TC, {0,0,0}, 0}.
