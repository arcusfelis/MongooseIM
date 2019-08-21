-module(mim_ct_ports).
-export([rewrite_ports/1]).

rewrite_ports(TestConfig = #{hosts := Hosts, first_port := FirstPort}) ->
    io:format("rewrite_ports~n", []),
    UniquePorts = lists:usort(lists:flatmap(fun host_to_ports/1, Hosts)),
    NewPorts = lists:seq(FirstPort, FirstPort + length(UniquePorts) - 1),
    Mapping = maps:from_list(lists:zip(UniquePorts, NewPorts)),
    io:format("Mapping ~p~n", [Mapping]),
    Hosts2 = apply_mapping_for_hosts(Hosts, Mapping),
    TestConfig#{hosts => Hosts2};
rewrite_ports(TestConfig = #{}) ->
    io:format("rewrite_ports skipped~n", []),
    TestConfig.

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

