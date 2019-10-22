-module(mongoose_sanity_checks_SUITE).

-compile(export_all).

all() ->
    [is_mongooseim,
     alice_can_connect].

init_per_suite(Config) ->
    escalus:init_per_suite(Config).

end_per_suite(Config) ->
    %% There would be something that we can't remove in some cases
    catch escalus_fresh:clean(),
    escalus:end_per_suite(Config).

init_per_testcase(CaseName, Config) ->
    escalus:init_per_testcase(CaseName, Config).

end_per_testcase(CaseName, Config) ->
    escalus:end_per_testcase(CaseName, Config).


is_mongooseim(Config) ->
    mongooseim = escalus_server:name(Config).

alice_can_connect(Config) ->
    mongoose_helper:wait_until(fun () -> catch try_connect(Config) end, ok, #{time_left => timer:seconds(30)}).

try_connect(Config) ->
    AliceSpec = escalus_fresh:create_fresh_user(Config, alice),
    {ok, _, _} = escalus_connection:start(AliceSpec),
    ok.
