-module(mim_ct_config).
-export([preprocess/1]).

preprocess(Config) ->
    Mods = proplists:get_value(preprocess_config_modules, Config, []),
    preprocess_config(Mods, Config).

preprocess_config([Mod|Mods], Config) ->
    preprocess_config(Mods, Mod:preprocess(Config));
preprocess_config([], Config) ->
    Config.
