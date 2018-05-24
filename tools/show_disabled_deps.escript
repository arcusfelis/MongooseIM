#!/usr/bin/env escript
main(_) ->
    {ok, Config} = file:consult("rebar.config"),
    {ok, Data} = file:script("rebar.config.script", [{'CONFIG', Config}]),
    DisabledDeps = proplists:get_value(disabled_deps, Data, []),
    DisabledPlugins = proplists:get_value(disabled_plugins, Data, []),
    DisabledApps = DisabledDeps ++ DisabledPlugins,
    [io:format(user, "~p\n", [DisabledApp]) || DisabledApp <- DisabledApps].
