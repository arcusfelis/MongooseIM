-module(mim_ct_config_ports).
-export([preprocess/1]).

preprocess(Config) ->
    Hosts = proplists:get_value(hosts, Config, []),
    Users = proplists:get_value(escalus_users, Config, []),
    Users1 = remove_users_from_disabled_hosts(Config, Users),
    Users2 = replace_ports_in_user_specs(Users1, Hosts),
    Config2 = set_escalus_port(Config, Hosts),
    lists:keyreplace(escalus_users, 1, Config2, {escalus_users, Users2}).

replace_ports_in_user_specs(Users, Hosts) ->
    [replace_ports_in_user_spec(User, Spec, Hosts) || {User, Spec} <- Users].

replace_ports_in_user_spec(User, Spec, Hosts) ->
    case proplists:get_value(host_port, Spec) of
        {Host, PortName} ->
            Port = get_host_port(Host, PortName, Hosts),
            %% Add port into userspec
            %% Keep host_port for informational purposes
            {User, [{port, Port}|Spec]};
        undefined ->
            {User, Spec}
    end.

set_escalus_port(Config, Hosts) ->
    case proplists:get_value(escalus_host_port, Config) of
        undefined ->
            Config;
        {Host, PortName} ->
            Port = get_host_port(Host, PortName, Hosts),
            %% Add escalus_host at the config root
            [{escalus_port, Port}|Config]
    end.

get_host_port(Host, PortName, Hosts) ->
    case proplists:get_value(Host, Hosts) of
        undefined ->
            error({host_not_found, Host});
        HostConfig ->
            case proplists:get_value(PortName, HostConfig) of
                undefined ->
                    error({port_option_not_found, Host, PortName});
                Port ->
                    Port
            end
    end.

remove_users_from_disabled_hosts(Config, Users) ->
    DisabledHosts = proplists:get_value(disabled_hosts, Config, []),
    [{User, Spec} || {User, Spec} <- Users, not is_disabled_user(User, Spec, DisabledHosts)].

is_disabled_user(_User, Spec, DisabledHosts) ->
    case proplists:get_value(host_port, Spec) of
        {Host, _PortName} ->
            lists:member(Host, DisabledHosts);
        undefined ->
            false
    end.
