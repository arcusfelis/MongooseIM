-module(mim_ct_config_users).
-export([preprocess/1]).

preprocess(Config) ->
    Users = proplists:get_value(escalus_users, Config, []),
    Suffix = proplists:get_value(username_suffix, Config, <<>>),
    Users2 = add_suffix_in_user_specs(Users, Suffix),
    lists:keyreplace(escalus_users, 1, Config, {escalus_users, Users2}).

add_suffix_in_user_specs(Users, Suffix) ->
    [add_suffix_in_user_spec(User, Spec, Suffix) || {User, Spec} <- Users].

add_suffix_in_user_spec(User, Spec, Suffix) ->
    case proplists:get_value(username, Spec) of
        Username when is_binary(Username) ->
            Username2 = <<Username/binary, Suffix/binary>>,
            {User, lists:keyreplace(username, 1, Spec, {username, Username2})};
        undefined ->
            {User, Spec}
    end.
