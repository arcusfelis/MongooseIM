%%% @doc Stops tests if there are too many errors
-module(ct_showstopper_hook).

%% @doc Add the following line in your *.spec file to enable this:
%% {ct_hooks, [ct_markdown_errors_hook]}.

%% Callbacks
-export([id/1]).
-export([init/2]).
-export([post_init_per_suite/4,
         post_init_per_group/4,
         post_init_per_testcase/4]).
-export([post_end_per_suite/4,
         post_end_per_group/4,
         post_end_per_testcase/4]).

-record(state, { limit }).

%% @doc Return a unique id for this CTH.
id(_Opts) ->
    "ct_showstopper_hook_001".

%% @doc Always called before any other callback function. Use this to initiate
%% any common state.
init(_Id, _Opts) ->
    {ok, #state{ limit = 50 }}.

post_init_per_suite(SuiteName, Config, Return, State) ->
    State2 = handle_return(Return, Config, State),
    case should_stop(State2) of
        true ->
            {{fail,too_many_errors}, State};
        false ->
            {Return, State2}
    end.

post_init_per_group(GroupName, Config, Return, State) ->
    State2 = handle_return(Return, Config, State),
    {Return, State2}.

post_init_per_testcase(TC, Config, Return, State) ->
    State2 = handle_return(Return, Config, State),
    {Return, State2}.

post_end_per_suite(SuiteName, Config, Return, State) ->
    State2 = handle_return(Return, Config, State),
    {Return, State2}.

post_end_per_group(GroupName, Config, Return, State) ->
    State2 = handle_return(Return, Config, State),
    {Return, State2}.

%% @doc Called after each test case.
post_end_per_testcase(TC, Config, Return, State) ->
    State2 = handle_return(Return, Config, State),
    {Return, State2}.


handle_return(Return, Config, State) ->
    try handle_return_unsafe(Return, Config, State)
    catch Class:Error ->
        Stacktrace = erlang:get_stacktrace(),
        ct:pal("issue=handle_return_unsafe_failed reason=~p:~p~n"
               "stacktrace=~p", [Class, Error, Stacktrace]),
        State
    end.

handle_return_unsafe(Return, Config, State) ->
    case to_error_message(Return) of
        ok ->
            State;
        Error ->
            decrease_limit(State)
    end.

decrease_limit(State=#state{limit=Limit}) ->
    State#state{limit=Limit - 1}.

should_stop(#state{limit=Limit}) ->
    Limit =< 0.

to_error_message(Return) ->
    case Return of
        {'EXIT', _} ->
            Return;
        {fail, _} ->
            Return;
        {error, _} ->
            Return;
        {skip, _} ->
            ok;
        _ ->
            ok
    end.
