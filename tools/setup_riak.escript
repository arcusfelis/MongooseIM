#!/usr/bin/env escript
-mode(compile).

main(_) ->
    application:ensure_all_started(ssl),
    application:ensure_all_started(inets),
    %% Set current directory to script directory
    file:set_cwd(filename:dirname(escript:script_name())),
    Cookie = riak,
    io:format("~nsetup_riak START~n", []),
    MyPid = os:getpid(),
    MyName = list_to_atom("setup_riak" ++ MyPid),
    {ok, _} = net_kernel:start([node_name(MyName)]),
    erlang:set_cookie(node(), Cookie),
    RiakNode = node_name(riak),
    case net_adm:ping(RiakNode) of
        pang ->
            io:format("Failed to ping ~p~n", [RiakNode]),
            init:stop(1);
        pong ->
            io:format("Riak node ~p~n", [RiakNode]),
            try do_command(os:getenv("SCRIPT_CMD"), RiakNode)
            catch Class:Reason ->
                %% This script runs inside michalwski/docker-riak, which still uses OTP 20.3
                Stacktrace = erlang:get_stacktrace(),
                io:format("Failed ~p:~p~n~p~n", [Class, Reason, Stacktrace]),
                init:stop(1)
            end,
        io:format("setup_riak DONE~n", [])
    end.

node_name(ShortName) ->
    list_to_atom(atom_to_list(ShortName) ++ "@" ++ format_ip(local_ip_v4())).

format_ip({A,B,C,D}) ->
    integer_to_list(A) ++ "." ++
    integer_to_list(B) ++ "." ++
    integer_to_list(C) ++ "." ++
    integer_to_list(D).

local_ip_v4() ->
    {ok, Addrs} = inet:getifaddrs(),
    hd([
         Addr || {_, Opts} <- Addrs, {addr, Addr} <- Opts,
         size(Addr) == 4, Addr =/= {127,0,0,1}
    ]).

do_command("setup_prefix", RiakNode) ->
    setup_prefix(RiakNode);
do_command(_, RiakNode) ->
    setup_riak_node(RiakNode).

setup_riak_node(RiakNode) ->
    io:format("setup_riak_node for ~p~n", [RiakNode]),
    Prefix = get_prefix(),
    setup_access(RiakNode),
    setup_schemas(),
    setup_indexes(Prefix),
    setup_types(RiakNode, Prefix).

%% Skips setting up schemas and access
setup_prefix(RiakNode) ->
    io:format("setup_prefix for ~p~n", [RiakNode]),
    Prefix = get_prefix(),
    setup_indexes(Prefix),
    setup_types(RiakNode, Prefix).

setup_access(RiakNode) ->
    User = "ejabberd",
    Password = "mongooseim_secret",
    ok = rpc:call(RiakNode, riak_core_console, security_enable, [[]]),
    ok = rpc:call(RiakNode, riak_core_console, add_user, [[User, "password=" ++ Password]]),
    ok = rpc:call(RiakNode, riak_core_console, add_source, [["all", "127.0.0.1/32", "password"]]),
    ok = rpc:call(RiakNode, riak_core_console, add_source, [[User, "0.0.0.0/0", "password"]]),
    ok = rpc:call(RiakNode, riak_core_console, grant, [[permissions(), "on", "any", "to", User]]),
    ok = rpc:call(RiakNode, riak_core_console, ciphers, [[ciphers()]]),
    ok.

ciphers() ->
    "AES256-SHA:DHE-RSA-AES128-SHA256".

permissions() ->
    "riak_kv.get,riak_kv.put,riak_kv.delete,riak_kv.index,"
    "riak_kv.list_keys,riak_kv.list_buckets,riak_kv.mapreduce,"
    "riak_core.get_bucket,riak_core.set_bucket,riak_core.set_bucket_type,"
    "riak_core.get_bucket_type,search.admin,search.query".

setup_types(RiakNode, Prefix) ->
    BinPrefix = list_to_binary(Prefix),
    Def = [{last_write_wins, true}, {dvv_enabled, false}],
    Map = [{datatype, map}],
    Set = [{datatype, set}],
    create_and_activate_bucket_type(RiakNode, <<BinPrefix/binary, "users">>, Map),
    create_and_activate_bucket_type(RiakNode, <<BinPrefix/binary, "rosters">>, Map),
    create_and_activate_bucket_type(RiakNode, <<BinPrefix/binary, "roster_versions">>, Def),
    create_and_activate_bucket_type(RiakNode, <<BinPrefix/binary, "private">>, Def),
    create_and_activate_bucket_type(RiakNode, <<BinPrefix/binary, "vcard">>, [{search_index, <<BinPrefix/binary, "vcard">>}|Def]),
    create_and_activate_bucket_type(RiakNode, <<BinPrefix/binary, "mam_yz">>, [{search_index, <<BinPrefix/binary, "mam">>}|Map]),
    create_and_activate_bucket_type(RiakNode, <<BinPrefix/binary, "last">>, Def),
    create_and_activate_bucket_type(RiakNode, <<BinPrefix/binary, "offline">>, Def),
    create_and_activate_bucket_type(RiakNode, <<BinPrefix/binary, "privacy_defaults">>, Def),
    create_and_activate_bucket_type(RiakNode, <<BinPrefix/binary, "privacy_lists_names">>, Set),
    create_and_activate_bucket_type(RiakNode, <<BinPrefix/binary, "privacy_lists">>, Def),
    ok.


create_and_activate_bucket_type(Node, Type, Props) ->
    ok = rpc:call(Node, riak_core_bucket_type, create, [Type, Props]),
    wait_until_bucket_type_status(Type, ready, Node),
    ok = rpc:call(Node, riak_core_bucket_type, activate, [Type]),
    wait_until_bucket_type_status(Type, active, Node).

wait_until_bucket_type_status(Type, ExpectedStatus, Nodes) when is_list(Nodes) ->
    [wait_until_bucket_type_status(Type, ExpectedStatus, Node) || Node <- Nodes];
wait_until_bucket_type_status(Type, ExpectedStatus, Node) ->
    F = fun() ->
                ActualStatus = rpc:call(Node, riak_core_bucket_type, status, [Type]),
                ExpectedStatus =:= ActualStatus
        end,
    ok = wait_until(F, 30, 100).


%% @doc Retry `Fun' until it returns true.
%% Repeat maximum `Retry' times.
%% Wait `Delay' milliseconds between retries.
wait_until(Fun, Retry, Delay) when Retry > 0 ->
    Res = Fun(),
    case Res of
        true ->
            ok;
        _ when Retry == 1 ->
            {fail, Res};
        _ ->
            timer:sleep(Delay),
            wait_until(Fun, Retry-1, Delay)
    end.


setup_schemas() ->
    do_put_file("/search/schema/vcard", "vcard_search_schema.xml"),
    do_put_file("/search/schema/mam", "mam_search_schema.xml"),
    wait_for_code("/search/schema/vcard", 200),
    wait_for_code("/search/schema/mam", 200),
    ok.

setup_indexes(Prefix) ->
    do_put_json("/search/index/" ++ Prefix ++ "vcard", "{\"schema\":\"vcard\"}"),
    do_put_json("/search/index/" ++ Prefix ++ "mam", "{\"schema\":\"mam\"}"),
    wait_for_code("/search/index/" ++ Prefix ++ "vcard", 200),
    wait_for_code("/search/index/" ++ Prefix ++ "mam", 200),
    ok.

wait_for_code(Path, Code) ->
    F = fun() -> check_result_code(do_get(Path), Code) end,
    ok = wait_until(F, 60, 2000).

do_get(Path) ->
    Url = riak_url(Path),
    Headers = [auth_header()],
    Request = {Url, Headers},
    %% request(Method, Request, HTTPOptions, Options)
    Result = httpc:request(get, Request, http_opts(), []),
    io:format("GET to ~p returns:~n~p~n", [Path, Result]),
    Result.

do_put_file(Path, File) ->
    io:format("~ndo_put_file: ~p~n", [Path]),
    Url = riak_url(Path),
    Headers = [auth_header()],
    ContentType = "application/xml",
    Body = get_body_from_file(File),
    Request = {Url, Headers, ContentType, Body},
    %% request(Method, Request, HTTPOptions, Options)
    Result = httpc:request(put, Request, http_opts(), []),
    io:format("~nPUT to ~p returns:~n~p~n", [Path, Result]),
    Result.

do_put_json(Path, JsonBody) ->
    Url = riak_url(Path),
    Headers = [auth_header()],
    ContentType = "application/json",
    Request = {Url, Headers, ContentType, JsonBody},
    %% request(Method, Request, HTTPOptions, Options)
    Result = httpc:request(put, Request, http_opts(), []),
    io:format("~nPUT to ~p returns:~n~p~n", [Path, Result]),
    Result.

http_opts() ->
    [{timeout, 10000}, {ssl, ssl_opts()}].

ssl_opts() ->
    [{verify, verify_none},
     {ciphers, ["AES256-SHA", "DHE-RSA-AES128-SHA256"]}].

check_result_code({ok, {{_HttpVer, Code, _Reason}, _Headers, _Body}}, Code) ->
    true;
check_result_code(Result, Code) ->
    false.

riak_url(Path) ->
    riak_host() ++ Path.

riak_host() ->
    "https://localhost:8096".
%   "http://localhost:8098".

get_prefix() ->
    case os:getenv("RIAK_PREFIX") of
        false ->
            "";
        Value ->
            Value
    end.

get_body_from_file(File) ->
    case file:read_file(File) of
        {ok, Bin} ->
            binary_to_list(Bin);
        {error, Reason} ->
            error({get_body_from_file_failed, Reason, File})
    end.

auth_header() ->
    User = "ejabberd",
    Password = "mongooseim_secret",
    auth_header(User, Password).

auth_header(User, Password) ->
    Encoded = base64:encode_to_string(lists:append([User,":",Password])),
    {"Authorization","Basic " ++ Encoded}.
