-module(mim_ct_ports).
-export([rewrite_ports/1]).

-export([is_port_free/1]).

rewrite_ports(TestConfig = #{hosts := Hosts, first_port := FirstPort}) ->
    F = fun() -> do_rewrite_ports(TestConfig, Hosts, FirstPort) end,
    mim_ct_helper:travis_fold("rewrite_ports", "Rewrite ports", F);
rewrite_ports(TestConfig = #{}) ->
    io:format("rewrite_ports skipped~n", []),
    TestConfig.

do_rewrite_ports(TestConfig, Hosts, FirstPort) ->
    io:format("rewrite_ports~n", []),
    UniquePorts = lists:usort(lists:flatmap(fun host_to_ports/1, Hosts)),
    NewPorts = make_ports(FirstPort, length(UniquePorts)),
    Mapping = maps:from_list(lists:zip(UniquePorts, NewPorts)),
    io:format("Mapping ~p~n", [Mapping]),
    Hosts2 = apply_mapping_for_hosts(Hosts, Mapping),
    TestConfig#{hosts => Hosts2}.

host_to_ports({_HostId, HostConfig}) ->
    PortValues = [V || {K,V} <- HostConfig, is_port_option(K, V)],
    lists:usort(PortValues).

apply_mapping_for_hosts(Hosts, Mapping) ->
    [{HostId, apply_mapping_for_host(HostConfig, Mapping)} || {HostId, HostConfig} <- Hosts].

apply_mapping_for_host(HostConfig, Mapping) ->
    lists:map(fun({K,V}) ->
                case is_port_option(K, V) of
                    true ->
                        NewV = maps:get(V, Mapping),
                        io:format("Rewrite port ~p ~p to ~p~n", [K, V, NewV]),
                        {K, NewV};
                    false ->
                        {K, V}
                end
             end, HostConfig).

is_port_option(K, V) when is_integer(V) ->
     lists:suffix("_port", atom_to_list(K));
is_port_option(_K, _V) ->
    false.

make_ports(NextPort, Count) when Count > 0 ->
    case is_port_free(NextPort) of
        true ->
            [NextPort|make_ports(NextPort + 1, Count - 1)];
        false ->
            %% Try another one
            make_ports(NextPort + 1, Count)
    end;
make_ports(_, _) ->
    [].

is_port_free(PortNum) ->
    case gen_tcp:connect("localhost", PortNum, []) of
        {error,econnrefused} ->
            true;
        {ok, Conn} ->
            gen_tcp:close(Conn),
            io:format("is_port_free: not free ~p~n", [PortNum]),
            false;
        Other ->
            io:format("is_port_free: maybe not free ~p, reason=~p~n", [PortNum, Other]),
            false
    end.

