-module(mongoose_sanity_checks_SUITE).

-compile(export_all).

all() ->
    [is_mongooseim,
     {group, healthcheck}].

groups() ->
    %% Wait for mongooseim starting accepting connections
    [{healthcheck,  [{repeat_until_all_ok, 30}], healthcheck_cases()}].

healthcheck_cases() ->
    [alice_can_connect].


init_per_suite(Config) ->
    escalus:init_per_suite(Config).

end_per_suite(Config) ->
    escalus_fresh:clean(),
    escalus:end_per_suite(Config).

init_per_testcase(CaseName, Config) ->
    escalus:init_per_testcase(CaseName, Config).

end_per_testcase(CaseName, Config) ->
    escalus:end_per_testcase(CaseName, Config).


is_mongooseim(Config) ->
    mongooseim = escalus_server:name(Config).

alice_can_connect(Config) ->
    AliceSpec = escalus_fresh:create_fresh_user(Config, alice),
    try
        {ok, _, _} = escalus_connection:start(AliceSpec)
    catch Class:Reason ->
              ct:pal("alice_can_connect failed, waiting", []),
              Stacktrace = erlang:get_stacktrace(),
              timer:sleep(100),
              erlang:raise(Class, Reason, Stacktrace)
    end.
