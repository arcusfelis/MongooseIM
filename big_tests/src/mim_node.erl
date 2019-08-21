-module(mim_node).
-export([load/2]).
-export([make/1]).

%% Sets variables without touching anything on disk
%% Options:
%% - prototype_dir = "_build/name"
%% - build_dir = "_build/name"
%% - vars = "mim1.vars.config"
%% - repo_dir = REPO_DIR, abs path
load(NodeConfig, TestConfig) ->
    NodeConfig1 = make_abs_paths(NodeConfig),
    NodeConfig2 = overlay_vars(NodeConfig1),
    apply_preset(NodeConfig2, TestConfig).

make(NodeConfig) ->
    NodeConfig1 = copy_release(NodeConfig),
    NodeConfig2 = apply_template(NodeConfig1),
    start(NodeConfig2).

start(NodeConfig = #{build_dir := BuildDir}) ->
    Ctl = filename:join(BuildDir, "rel/mongooseim/bin/mongooseimctl"),
    StartResult = os:cmd(Ctl ++ " start"),
    StartedResult = os:cmd(Ctl ++ " started"),
    StatusResult = os:cmd(Ctl ++ " status"),
    NodeConfig#{start_result => StartResult, started_result => StartedResult, status_result => StatusResult}.


make_abs_paths(NodeConfig = #{prototype_dir := ProtoDir, build_dir := BuildDir, repo_dir := RepoDir}) ->
    NodeConfig#{
        prototype_dir => filename:absname(ProtoDir, RepoDir),
        build_dir => filename:absname(BuildDir, RepoDir)}.

copy_release(NodeConfig = #{prototype_dir := FromDir, build_dir := ToDir}) ->
    %% FIXME escaping
    CopyResult = os:cmd("rsync --exclude rel/mongooseim/Mnesia.* "
           "--exclude rel/mongooseim/var "
           "--exclude rel/mongooseim/log "
           "-al " ++ FromDir ++ "/" ++ " " ++ ToDir ++ "/"),
    NodeConfig#{copy_result => CopyResult}.


apply_template(NodeConfig = #{build_dir := BuildDir, repo_dir := RepoDir}) ->
    RelDir = BuildDir ++ "/rel/mongooseim",
    Templates = templates(RelDir),
    NodeConfig1 = NodeConfig#{output_dir => list_to_binary(RelDir)},
    [render_template(filename:absname(In, RepoDir), Out, NodeConfig1) || {In, Out} <- Templates],
    NodeConfig1.


overlay_vars(NodeConfig = #{vars := VarsFile, repo_dir := RepoDir}) ->
    Vars = consult_map(filename:absname("rel/vars.config", RepoDir)),
    NodeVars = consult_map(filename:absname("rel/" ++ VarsFile, RepoDir)),
    %% NodeVars overrides Vars
    Vars2 = maps:merge(Vars, NodeVars),
    %% NodeConfig overrides Vars2
    maps:merge(Vars2, NodeConfig).

consult_map(File) ->
    case file:consult(File) of
        {ok, Vars} ->
            maps:from_list(Vars);
        Other ->
            error({consult_map_failed, File, Other})
    end.

read_file(File) ->
    case file:read_file(File) of
        {ok, Bin} ->
            Bin;
        Other ->
            error({read_file_failed, File, Other})
    end.

%% Based on rebar.config overlay section
templates(RelDir) ->
    simple_templates(RelDir) ++ erts_templates(RelDir).

simple_templates(RelDir) ->
    [{In, RelDir ++ "/" ++ Out} || {In, Out} <- simple_templates()].

simple_templates() ->
    [
     {"rel/files/mongooseim",       "bin/mongooseim"},
     {"rel/files/mongooseimctl",    "bin/mongooseimctl"},
     {"rel/files/app.config",       "etc/app.config"},
     {"rel/files/vm.args",          "etc/vm.args"},
     {"rel/files/vm.dist.args",     "etc/vm.dist.args"},
     {"rel/files/mongooseim.cfg",   "etc/mongooseim.cfg"}
    ].

erts_templates(RelDir) ->
    %% Usually one directory
    ErtsDirs = filelib:wildcard(RelDir ++ "/erts-*"),
    [{"rel/files/nodetool", ErtsDir ++ "/bin/nodetool"} || ErtsDir <- ErtsDirs].

render_template(In, Out, Vars) ->
    BinIn = read_file(In),
    %% Do render twice to allow templates in variables
    BinTmp = bbmustache:render(BinIn, Vars, render_opts()),
    BinOut = bbmustache:render(BinTmp, Vars, render_opts()),
    case file:write_file(Out, BinOut) of
        ok ->
            ok;
        Other ->
            error({write_file_failed, Out, Other})
    end.

render_opts() ->
    [{escape_fun, fun(X) -> X end}, {key_type, atom}, {value_serializer, fun(X) -> X end}].

rewrite_ports(NodeConfig = #{first_port := FirstPort}) ->
    PortKeys = [K || {K,V} <- maps:to_list(NodeConfig), K =/= first_port, is_port_option(K), is_integer(V)],
    PortValues = [maps:get(Key, NodeConfig) || Key <- PortKeys],
    UniquePorts = lists:usort(PortValues),
    NewPorts = lists:seq(FirstPort, FirstPort + length(UniquePorts) - 1),
    Mapping = maps:from_list(lists:zip(UniquePorts, NewPorts)),
    maps:map(fun(K,V) ->
                case lists:member(K, PortKeys) of
                    true ->
                        NewV = maps:get(V, Mapping),
                        io:format("Rewrite port ~p ~p to ~p~n", [K, V, NewV]),
                        NewV;
                    false ->
                        V
                end
             end, NodeConfig);
rewrite_ports(NodeConfig) ->
    NodeConfig.

is_port_option(K) ->
     lists:suffix("_port", atom_to_list(K)).

apply_preset(NodeConfig = #{preset := PresetName}, TestConfig = #{ejabberd_presets := Presets}) ->
    case proplists:get_value(PresetName, Presets) of
        undefined ->
            error(#{error => preset_not_found, preset_name => PresetName, ejabberd_presets => Presets});
        PresetVarsList ->
            PresetVars = maps:from_list(PresetVarsList),
            %% PresetVars overrides NodeConfig
            maps:merge(NodeConfig, PresetVars)
    end;
apply_preset(NodeConfig, _TestConfig) ->
    io:format("Ignore presets", []),
    NodeConfig.
