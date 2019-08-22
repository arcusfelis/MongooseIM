-module(mim_ct_sh).
-export([run/2]).

%% Based on https://github.com/proger/erlsh/blob/master/src/erlsh.erl

run([Command|Args], Opts) ->
    Cwd = maps:get(cwd, Opts, "."),
    Env = maybe_map_to_list(maps:get(env, Opts, [])),
    Port = erlang:open_port({spawn_executable, find_command(Command)},
        [stream, stderr_to_stdout, binary, exit_status,
            {args, Args}, {cd, Cwd}, {env, Env}]),
    sh_loop(Port, binary).

%
% private functions
%

sh_loop(Port, binary) ->
    sh_loop(Port, fun(Chunk, Acc) -> [Chunk|Acc] end, []).

sh_loop(Port, Fun, Acc) when is_function(Fun) ->
    sh_loop(Port, Fun, Acc, fun erlang:iolist_to_binary/1).

sh_loop(Port, Fun, Acc, Flatten) when is_function(Fun) ->
    receive
        {Port, {data, {eol, Line}}} ->
            sh_loop(Port, Fun, Fun({eol, Line}, Acc), Flatten);
        {Port, {data, {noeol, Line}}} ->
            sh_loop(Port, Fun, Fun({noeol, Line}, Acc), Flatten);
        {Port, {data, Data}} ->
            sh_loop(Port, Fun, Fun(Data, Acc), Flatten);
        {Port, {exit_status, Status}} ->
            {done, Status, Flatten(lists:reverse(Acc))}
    end.

find_command(C) ->
    case filename:pathtype(C) of
        absolute -> C;
        relative -> case filename:split(C) of
                [C] -> os:find_executable(C);
                _ -> C % smth like deps/erlsh/priv/fdlink
            end;
        _ -> C
    end.

maybe_map_to_list(Map) when is_map(Map) -> maps:to_list(Map);
maybe_map_to_list(List) -> List.

