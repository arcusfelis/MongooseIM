-module(mim_ct_parallel).
-export([parallel_map/2]).

%% mim_ct_parallel:parallel_map(fun(X) -> X end, [1,2,3]).
%% [1,2,3]
parallel_map(F, List) ->
    Tag = make_ref(),
    Parent = self(),
    Pids = [proc_lib:spawn(fun() -> Result = F(X), Parent ! {result, Tag, self(), Result} end) || X <- List],
    Refs = [erlang:monitor(process, Pid) || Pid <- Pids],
    wait_for_monitors(Tag, Refs, []).

wait_for_monitors(_Tag, [], Results) ->
    lists:reverse(Results);
wait_for_monitors(Tag, [MonRef|Refs], Results) ->
    receive
        {'DOWN', MonRef, process, Pid, normal} ->
            receive
                {result, Tag, Pid, Result} ->
                    wait_for_monitors(Tag, Refs, [{ok, Result}|Results])
                after 5000 -> %% should not happen
                    wait_for_monitors(Tag, Refs, [{error, result_timeout}|Results])
            end;
        {'DOWN', MonRef, process, Pid, Reason} ->
            wait_for_monitors(Tag, Refs, [{error, Reason}|Results])
    end.
