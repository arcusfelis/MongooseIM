-module(mongoose_push_mock).

-export([start/1]).
-export([stop/0]).
-export([port/0]).
-export([init/2]).
-export([subscribe/1]).
-export([wait_for_push_request/1]).

start(Config) ->
    application:ensure_all_started(cowboy),
    CertDir = filename:join(path_helper:test_dir(Config), "priv/ssl"),
    CertPath = path_helper:canonicalize_path(filename:join(CertDir, "cert.pem")),
    KeyPath = path_helper:canonicalize_path(filename:join(CertDir, "key.pem")),

    Dispatch = cowboy_router:compile([{'_', [{<<"/v2/notification/:token">>, ?MODULE, #{}}]}]),
    {ok, Pid} = cowboy:start_tls(mongoose_push_https_mock,
                                 [{certfile, CertPath}, {keyfile, KeyPath}],
                                 #{env => #{dispatch => Dispatch}}),
    ets:new(mongoose_push_mock_subscribers,
            [public, named_table, {heir, Pid, take_care}]),
    wait_for_can_connect_to_port(port()).

port() ->
    ranch:get_port(mongoose_push_https_mock).

stop() ->
    cowboy:stop_listener(mongoose_push_https_mock).

subscribe(Token) ->
    ets:insert(mongoose_push_mock_subscribers, {Token, self()}).

wait_for_push_request(Token) ->
    receive
        {push_request, Token, Body} ->
            Body
    after 10000 ->
              ct:fail("timeout_waiting_for_push_request")
    end.

init(Req, State) ->
    Token = cowboy_req:binding(token, Req),
    {ok, Body, Req2} = cowboy_req:read_body(Req),
    [{_, Subscriber}] = ets:lookup(mongoose_push_mock_subscribers, Token),
    Subscriber ! {push_request, Token, Body},
    Req3 = cowboy_req:reply(204, #{}, <<>>, Req2),
    {ok, Req3, State}.


wait_for_can_connect_to_port(Port) ->
    Opts = #{time_left => timer:seconds(30), sleep_time => 1000, name => {can_connect_to_port, Port}},
    mongoose_helper:wait_until(fun() -> can_connect_to_port(Port) end, true, Opts).

can_connect_to_port(Port) ->
    case gen_tcp:connect("127.0.0.1", Port, []) of
        {ok, Sock} ->
            gen_tcp:close(Sock),
            true;
        Other ->
            ct:pal("can_connect_to_port port=~p result=~p", [Port, Other]),
            false
    end.
