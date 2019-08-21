-module(mim_ct_preload).
-export([load_test_modules/1]).

load_test_modules(TestSpec) ->
    %% Read test spec properties
    Props = consult_file(TestSpec),
    Modules = lists:usort(test_modules(Props)),
    [try_load_module(M) || M <- Modules].

test_modules([H|T]) when is_tuple(H) ->
    test_modules_list(tuple_to_list(H)) ++ test_modules(T);
test_modules([_|T]) ->
    test_modules(T);
test_modules([]) ->
    [].

test_modules_list([suites, _, Suite|_]) ->
    [Suite];
test_modules_list([groups, _, Suite|_]) ->
    [Suite];
test_modules_list([cases, _, Suite|_]) ->
    [Suite];
test_modules_list(_) ->
    [].

try_load_module(Module) ->
    case code:is_loaded(Module) of
        true -> already_loaded;
        _ -> code:load_file(Module)
    end.

consult_file(File) ->
    case file:consult(File) of
        {ok, Bin} ->
            Bin;
        Other ->
            error({read_file_failed, File, Other})
    end.
