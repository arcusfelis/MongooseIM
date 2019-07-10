%%==============================================================================
%% Copyright 2014 Erlang Solutions Ltd.
%%
%% Licensed under the Apache License, Version 2.0 (the "License");
%% you may not use this file except in compliance with the License.
%% You may obtain a copy of the License at
%%
%% http://www.apache.org/licenses/LICENSE-2.0
%%
%% Unless required by applicable law or agreed to in writing, software
%% distributed under the License is distributed on an "AS IS" BASIS,
%% WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
%% See the License for the specific language governing permissions and
%% limitations under the License.
%%==============================================================================

-module(cowboy_SUITE).
-compile(export_all).

-include_lib("common_test/include/ct.hrl").

-define(SERVER, "http://localhost:8080").

-import(ejabberd_helper, [start_ejabberd/1,
                          stop_ejabberd/0,
                          use_config_file/2,
                          start_ejabberd_with_config/2]).

%%--------------------------------------------------------------------
%% Suite configuration
%%--------------------------------------------------------------------

all() ->
    [{group, routing},
     start_cowboy_returns_error_eaddrinuse,
     conf_reload].

groups() ->
    [{routing, [sequence], [http_requests,
                            ws_request_bad_protocol,
                            ws_requests_xmpp,
                            ws_requests_other,
                            mixed_requests]}].

suite() ->
    [].

%%--------------------------------------------------------------------
%% Init & teardown
%%--------------------------------------------------------------------

-define(APPS, [crypto, ssl, fusco, ranch, cowlib, cowboy]).

init_per_suite(Config) ->
    [application:start(App) || App <- ?APPS],
    {ok, Pid} = create_handlers(),
    spawn_process_monitor(),
    [{meck_pid, Pid}|Config].

end_per_suite(Config) ->
    mnesia:stop(),
    mnesia:delete_schema([node()]),
    remove_handlers(Config),
    stop_process_monitor(),
    Config.

init_per_group(routing, Config) ->
    start_cowboy(),
    Config;
init_per_group(_GroupName, Config) ->
    Config.

end_per_group(routing, Config) ->
    stop_cowboy(),
    Config;
end_per_group(_GroupName, Config) ->
    Config.

init_per_testcase(_CaseName, Config) ->
    Config.

end_per_testcase(_CaseName, Config) ->
    reset_history(),
    reset_process_monitor(),
    Config.

%%--------------------------------------------------------------------
%% Tests
%%--------------------------------------------------------------------
http_requests(Config) ->
    %% Given
    Host = ?SERVER,
    Path = <<"/">>,
    Method = "GET",
    Headers = [],
    Body = [],

    %% When
    Codes = [begin
                Response = execute_request(Host, Path, Method, Headers, Body),
                to_status_code(Response)
            end || _ <- lists:seq(1, 50)],

    %% Then
    ExpectedCodes = lists:duplicate(50, 200), %% 50 times code 200
    case Codes of
        ExpectedCodes ->
            ok;
        _ ->
            ct:fail(#{reason => bad_codes,
                      codes => Codes,
                      expected_codes => ExpectedCodes})
    end,
    assert_cowboy_handler_calls(Config, dummy_http_handler, init, 50),
    wait_for_all_pids_to_finish(dummy_http_handler),
    % Cowboy can shutdown some handlers, that time out without calling terminate function.
    % Reason for termination would be "shutdown", not normal in such case.
    %
    % Shutdown timeout is 5 seconds and eventually would happen, if the test is run enough times.
    % Happens more often on the slow computers.
    %
    % Do debug this, add into group specs {repeat_until_any_fail, 1000}
    ShuttedDownHandlers = get_shutted_down_handlers(),
    assert_cowboy_handler_calls(Config, dummy_http_handler, terminate, 50 - length(ShuttedDownHandlers)),
    ok.

ws_request_bad_protocol(_Config) ->
    %% Given
    Host = ?SERVER,
    Path = <<"/">>,
    Method = "GET",
    Headers = ws_headers(<<"unknown-protocol">>),
    Body = [],

    %% When
    Response = execute_request(Host, Path, Method, Headers, Body),

    %% Then
    assert_status_code(Response, 404).

ws_requests_xmpp(Config) ->
    %% Given
    Host = "localhost",
    Port = 8080,
    Protocol = <<"xmpp">>,
    BinaryPing = ws_tx_frame(<<"ping">>, 2),
    BinaryPong = ws_rx_frame(<<"pong">>, 2),

    %% When
    {ok, Socket} = ws_handshake(Host, Port, Protocol),
    Responses = [begin
                ok = ws_send(Socket, BinaryPing),
                ws_recv(Socket)
            end || _ <- lists:seq(1, 50)],
    ok = gen_tcp:close(Socket),

    %% Then
    %% dummy_ws1_handler:init/2 is not called since mod_cowboy takes over
    Responses = lists:duplicate(50, BinaryPong),
    assert_cowboy_handler_calls(Config, dummy_ws1_handler, websocket_init, 1),
    assert_cowboy_handler_calls(Config, dummy_ws1_handler, websocket_handle, 50),
    ok = meck:wait(dummy_ws1_handler, websocket_terminate, '_', 1000).

ws_requests_other(Config) ->
    %% Given
    Host = "localhost",
    Port = 8080,
    Protocol = <<"other">>,
    TextPing = ws_tx_frame(<<"ping">>, 1),
    TextPong = ws_rx_frame(<<"pong">>, 1),

    %% When
    {ok, Socket} = ws_handshake(Host, Port, Protocol),
    Responses = [begin
            ok = ws_send(Socket, TextPing),
            ws_recv(Socket)
        end || _ <- lists:seq(1, 50)],
    ok = gen_tcp:close(Socket),

    %% Then

    Responses = lists:duplicate(50, TextPong),
    assert_cowboy_handler_calls(Config, dummy_ws2_handler, websocket_init, 1),
    assert_cowboy_handler_calls(Config, dummy_ws2_handler, websocket_handle, 50),
    ok = meck:wait(dummy_ws2_handler, websocket_terminate, '_', 1000).

mixed_requests(_Config) ->
    %% Given
    Protocol1 = <<"xmpp">>,
    Protocol2 = <<"other">>,
    Protocol3 = <<"non-existent">>,

    TextPing = ws_tx_frame(<<"ping">>, 1),
    TextPong = ws_rx_frame(<<"pong">>, 1),

    Host = "localhost",
    Port = 8080,

    HTTPHost = ?SERVER,
    Path = <<"/">>,
    Method = "GET",
    Headers3 = ws_headers(Protocol3),
    Headers4 = [],
    Body = [],

    %% When
    {ok, Socket1} = ws_handshake(Host, Port, Protocol1),
    {ok, Socket2} = ws_handshake(Host, Port, Protocol2),

    Responses = [begin
                ok = ws_send(Socket1, TextPing),
                Resp1 = ws_recv(Socket1),

                Resp2 = execute_request(HTTPHost, Path, Method, Headers4, Body),
                Status2 = is_status_code(Resp2, 200),

                ok = ws_send(Socket2, TextPing),
                Resp3 = ws_recv(Socket2),

                Resp4 = execute_request(HTTPHost, Path, Method, Headers3, Body),
                Status4 = is_status_code(Resp4, 404),

                {Resp1, Status2, Resp3, Status4}
            end || _ <- lists:seq(1, 50)],

    %% Then
    Responses = lists:duplicate(50, {TextPong, true, TextPong, true}).

start_cowboy_returns_error_eaddrinuse(_C) ->
    Opts = [{transport_options, #{socket_opts => [{port, 8088},
                                                  {ip, {127, 0, 0, 1}}]}},
            {modules, []},
            {retries, {2, 10}}],
    {ok, _Pid} = ejabberd_cowboy:start_cowboy(a_ref, Opts),
    Result = ejabberd_cowboy:start_cowboy(a_ref_2, Opts),
    {error, eaddrinuse} = Result.

conf_reload(Config) ->
    %% Given initial configuration
    HTTPHost = "http://localhost:5280",
    Path = <<"/">>,
    Method = "GET",
    Headers1 = [],
    Headers2 = ws_headers(<<"xmpp">>),
    Body = [],

    copy(data(Config, "mongooseim.onlyhttp.cfg"), data(Config, "mongooseim.cfg")),
    start_ejabberd_with_config(Config, "mongooseim.cfg"),

    %% When making requests for http and ws
    Response1 = execute_request(HTTPHost, Path, Method, Headers1, Body),
    Response2 = execute_request(HTTPHost, Path, Method, Headers2, Body),

    %% Then http returns 200 and ws returns 404
    assert_status_code(Response1, 200),
    assert_status_code(Response2, 404),

    %% Given new configuration
    copy(data(Config, "mongooseim.onlyws.cfg"), data(Config, "mongooseim.cfg")),
    ejabberd_config:reload_local(),

    %% When making requests for http and ws
    Response3 = execute_request(HTTPHost, Path, Method, Headers1, Body),

    %% Then http returns 404 and ws works fine
    assert_status_code(Response3, 404),

    ok = stop_ejabberd().

%%--------------------------------------------------------------------
%% Helpers
%%--------------------------------------------------------------------
copy(Src, Dst) ->
    {ok, _} = file:copy(Src, Dst).

data(Config, Path) ->
    Dir = proplists:get_value(data_dir, Config),
    filename:join([Dir, Path]).

start_cowboy() ->
    Dispatch = cowboy_router:compile([
                {'_',
                 [{"/[...]", mod_cowboy,
                   [{http, dummy_http_handler},
                    {ws, xmpp, dummy_ws1_handler},
                    {ws, other, dummy_ws2_handler}
                   ]}]
                }]),
    {ok, _Pid} = cowboy:start_clear(http_listener,
                                    #{num_acceptors => 20,
                                      socket_opts => [{port, 8080}]},
                                    #{env => #{dispatch => Dispatch}}).

stop_cowboy() ->
    ok = cowboy:stop_listener(http_listener).

execute_request(Host, Path, Method, Headers, Body) ->
    {ok, Pid} = fusco:start_link(Host, []),
    Response = fusco:request(Pid, Path, Method, Headers, Body, 5000),
    fusco:disconnect(Pid),
    Response.

assert_status_code(Response, Code) ->
    case is_status_code(Response, Code) of
        true ->
            ok;
        false ->
            ct:fail(#{reason => assert_status_code,
                      response => Response,
                      expected_code => Code})
    end.

is_status_code(Response, Code) ->
    case to_status_code(Response) of
        Code -> true;
        _    -> false
    end.

to_status_code({ok, {{CodeBin, _}, _, _, _, _}}) ->
    binary_to_integer(CodeBin).

ws_send(Socket, Frame) ->
    ok = gen_tcp:send(Socket, Frame).

ws_recv(Socket) ->
    {ok, Packet} = gen_tcp:recv(Socket, 0, 5000),
    Packet.

ws_handshake(Host, Port, Protocol) ->
    {ok, Socket} = gen_tcp:connect(Host, Port, [binary, {packet, raw},
                                                {active, false}]),
    ok = gen_tcp:send(Socket, [
                "GET / HTTP/1.1\r\n"
                "Host: localhost\r\n"
                "Connection: upgrade\r\n"
                "Origin: http://localhost\r\n"
                "Sec-WebSocket-Key: x3JJHMbDL1EzLkh9GBhXDw==\r\n"
                "Sec-WebSocket-Protocol: ", Protocol, "\r\n"
                "Sec-WebSocket-Version: 13\r\n"
                "Upgrade: websocket\r\n"
                "\r\n"]),
    {ok, Handshake} = gen_tcp:recv(Socket, 0, 5000),
    Packet = erlang:decode_packet(http, Handshake, []),
    {ok, {http_response, {1,1}, 101, "Switching Protocols"}, _Rest} = Packet,
    {ok, Socket}.

ws_headers(Protocol) ->
    [{<<"upgrade">>, <<"websocket">>},
     {<<"connection">>, <<"upgrade">>},
     {<<"sec-websocket-key">>, <<"x3JJHMbDL1EzLkh9GBhXDw==">>},
     {<<"sec-websocket-protocol">>, Protocol},
     {<<"sec-websocket-version">>, <<"13">>}].

ws_tx_frame(Payload, Opcode) ->
    Mask = 16#ffffffff,
    Length = byte_size(Payload),
    MaskedPayload = << <<(Byte bxor 16#ff):8>> || <<Byte:8>> <= Payload >>,
    <<1:1, 0:3, Opcode:4, 1:1, Length:7, Mask:32, MaskedPayload/binary>>.

ws_rx_frame(Payload, Opcode) ->
    Length = byte_size(Payload),
    <<1:1, 0:3, Opcode:4, 0:1, Length:7, Payload/binary>>.

%%--------------------------------------------------------------------
%% http/ws handlers mock
%%--------------------------------------------------------------------
create_handlers() ->
    Owner = self(),
    F = fun() ->
            [create_handler(Handler) || Handler <- handlers()],
            Owner ! ok,
            timer:sleep(infinity)
    end,
    Pid = spawn(F),
    receive
        ok ->
            {ok, Pid}
    after 5000 ->
            {error, timeout}
    end.

handlers() ->
    WSFuns = [{init, fun ws_init/2},
              {websocket_init, fun ws_websocket_init/1},
              {websocket_handle, fun ws_websocket_handle/2},
              {websocket_info, fun ws_websocket_info/2},
              {websocket_terminate, fun ws_websocket_terminate/3}],
    [{dummy_http_handler, [{init, fun handler_init/2},
                           {terminate, fun handler_terminate/3}]},
     {dummy_ws1_handler, WSFuns},
     {dummy_ws2_handler, WSFuns}].

create_handler({Name, Functions}) ->
    ok = meck:new(Name, [non_strict]),
    [ok = meck:expect(Name, Function, Fun) || {Function, Fun} <- Functions].

remove_handlers(Config) ->
    [ok = meck:unload(Handler) || {Handler, _} <- handlers()],
    exit(?config(meck_pid, Config), kill).

reset_history() ->
    [ok = meck:reset(Handler) || {Handler, _} <- handlers()].


%% cowboy_http_handler
handler_init(Req, _Opts) ->
    monitor_handler_process(),
    Req1 = cowboy_req:reply(200, Req),
    {ok, Req1, no_state}.

handler_terminate(_Reason, _Req, _State) ->
    ok.

%% cowboy_websocket_handler
ws_init(Req, _Opts) ->
    {cowboy_websocket, Req, no_ws_state}.

ws_websocket_init(no_ws_state) ->
    {ok, no_ws_state}.

ws_websocket_handle({text,<<"ping">>}, no_ws_state) ->
    {reply, {text, <<"pong">>}, no_ws_state};
ws_websocket_handle({binary, <<"ping">>}, no_ws_state) ->
    {reply, {binary, <<"pong">>}, no_ws_state};
ws_websocket_handle(_Other, no_ws_state) ->
    {ok, no_ws_state}.

ws_websocket_info(_Info, no_ws_state) ->
    {ok, no_ws_state}.

ws_websocket_terminate(_Reason, _Req, no_ws_state) ->
    ok.

assert_cowboy_handler_calls(Config, M, F, Num) ->
    Fun = fun() -> meck:num_calls(M, F, '_') end,
    try async_helper:wait_until(Fun, Num, #{time_left => timer:seconds(30)})
    catch Class:Reason ->
              Stacktrace = erlang:get_stacktrace(),
              MeckHistory = meck_proc:get_history(M),
              ct:pal("meck history: ~p", [MeckHistory]),
              write_meck_history(Config, MeckHistory),
              Pids = [element(1, X) || X <- MeckHistory],
              AlivePids = lists:usort([Pid || Pid <- Pids, is_process_alive(Pid)]),
              ct:pal("AlivePids ~p", [AlivePids]),
              PidFun = [{Pid, HistoryFun} || {Pid, {_, HistoryFun, _}, _} <- MeckHistory],
              ct:pal("PidFun ~p", [PidFun]),
              ct:pal("Downs ~p", [get_monitor_state()]),
              erlang:raise(Class, Reason, Stacktrace)
    end.

write_meck_history(Config, MeckHistory) ->
    HistBin = term_to_binary(MeckHistory),
    PrivDir = proplists:get_value(priv_dir, Config),
    Filename = filename:join(PrivDir, "meck_history.bin"),
    ct:pal("write meck history into ~ts", [Filename]),
    file:write_file(Filename, HistBin),
    ok.

%% Return a list of processes, that have called the module.
get_caller_pids(M) ->
    MeckHistory = meck_proc:get_history(M),
    lists:usort([element(1, X) || X <- MeckHistory]).

wait_for_all_pids_to_finish(M) ->
    GetAlivePidsFun = fun() ->
        HandlerPids = get_caller_pids(M),
        lists:usort(lists:filter(fun is_process_alive/1, HandlerPids))
        end,
    async_helper:wait_until(GetAlivePidsFun, [], #{time_left => timer:seconds(30)}).


%% Code for process monitoring.
%% We want to know reason of termination of processes.
%% Call monitor_handler_process() from inside process to monitor.

spawn_process_monitor() ->
    spawn(fun() -> register(process_monitor, self()), process_monitor_loop([]) end).

stop_process_monitor() ->
    process_monitor ! stop.

monitor_handler_process() ->
    process_monitor ! {monitor, self()},
    receive
        process_monitor_ok ->
            ok
    after 5000 ->
           error(monitor_handler_process_timeout)
    end.

reset_process_monitor() ->
    process_monitor ! {reset, self()},
    receive
        reset_ok ->
            ok
    after 5000 ->
              error(reset_process_monitor_timeout)
    end.

get_monitor_state() ->
    process_monitor ! {get_state, self()},
    receive
        {monitor_state, List} ->
            List
    after 5000 ->
              error(reset_process_monitor_timeout)
    end.

process_monitor_loop(List) ->
    receive
        {monitor, Pid} ->
            monitor(process, Pid),
            Pid ! process_monitor_ok,
            process_monitor_loop(List);
        {get_state, Pid} ->
            Pid ! {monitor_state, List},
            process_monitor_loop(List);
        {'DOWN', _, _, _, _} = Down ->
            process_monitor_loop([Down|List]);
        {reset, Pid} ->
            Pid ! reset_ok,
            process_monitor_loop([]);
        stop ->
            stop
    end.

get_shutted_down_handlers() ->
   [Pid || {'DOWN', _, process, Pid, shutdown} <- get_monitor_state()].
