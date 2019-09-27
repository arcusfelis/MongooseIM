-module(mim_ct_error).
-export([add_error/3]).
-export([has_errors/1]).
-export([get_short_errors/1]).
-export([print_errors/1]).

add_error(ShortError, Extra, TestConfig) ->
    Errors = maps:get(errors, TestConfig, []),
    TestConfig#{errors => [{ShortError, Extra}|Errors]}.

get_short_errors(TestConfig) ->
    Errors = maps:get(errors, TestConfig, []),
    [ShortError || {ShortError, _Extra} <- Errors].

has_errors(#{errors := [_|_]}) ->
    true;
has_errors(_) ->
    false.

print_errors(TestConfig) ->
    print_errors(TestConfig, has_errors(TestConfig)).

print_errors(TestConfig = #{test_spec := TestSpec, errors := Errors}, true) ->
    mim_ct_helper:travis_fold("mim_ct_error", "mim_ct_error " ++ TestSpec, fun() ->
                    io:format("~n==========================~n~n", []),
                    io:format("~tp", [Errors]),
                    io:format("~n==========================~n", [])
                end);
print_errors(_TestConfig, _) ->
    ok.
