#!/usr/bin/env escript
main(_) ->
    {ok, Config} = file:consult("rebar.config"),
    {ok, Data} = file:script("rebar.config.script", [{'CONFIG', Config}]),
    io:format("~p.\n", [Data]).
