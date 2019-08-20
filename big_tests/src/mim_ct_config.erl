%% Docs:
%% http://erlang.org/doc/apps/common_test/config_file_chapter.html
-module(mim_ct_config).
-export([read_config/1, check_parameter/1]).

read_config(ConfigFile) ->
    case file:consult(ConfigFile) of
    {ok,Config} ->
        Mods = proplists:get_value(preprocess_config_modules, Config, []),
        {ok, preprocess_config(Mods, Config)};
    {error,Reason} ->
        {error,{config_file_error, file:format_error(Reason)}}
    end.

% check if config file exists
check_parameter(File)->
    case filelib:is_file(File) of
    true->
        {ok,{file,File}};
    false->
        {error,{nofile,File}}
    end.

preprocess_config([Mod|Mods], Config) ->
    preprocess_config(Mods, Mod:preprocess(Config));
preprocess_config([], Config) ->
    Config.
