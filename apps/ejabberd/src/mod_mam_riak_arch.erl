-module(mod_mam_riak_arch).
-export([start/1,
         archive_message/6,
         archive_size/2,
         lookup_messages/8,
         remove_user_from_db/2]).

%% UID
-import(mod_mam_utils,
        [encode_compact_uuid/2]).

-include_lib("ejabberd/include/ejabberd.hrl").
-include_lib("ejabberd/include/jlib.hrl").
-include_lib("exml/include/exml.hrl").

-type unix_timestamp() :: non_neg_integer().

start(Host) ->
    application:start(riak_pool),
    F = fun(Conn) ->
            update_bucket(Conn, <<"mam_messages">>, lww_props()),
            update_bucket(Conn, <<"mam_hour_idx">>, [{allow_mult, true}]),
            update_bucket(Conn, <<"mam_usr_msg_cnt">>, [{allow_mult, true}])
        end,
    with_connection(Host, F).

lww_props() ->
    [{allow_mult, false}, {last_write_wins, true}].


archive_size(LServer, LUser) ->
    0.

-spec lookup_messages(UserJID, RSM, Start, End, WithJID, PageSize,
                      LimitPassed, MaxResultLimit) ->
    {ok, {TotalCount, Offset, MessageRows}} | {error, 'policy-violation'}
			     when
    UserJID :: #jid{},
    RSM     :: #rsm_in{} | none,
    Start   :: unix_timestamp() | undefined,
    End     :: unix_timestamp() | undefined,
    PageSize :: non_neg_integer(),
    WithJID :: #jid{} | undefined,
    LimitPassed :: boolean(),
    MaxResultLimit :: non_neg_integer(),
    TotalCount :: non_neg_integer(),
    Offset  :: non_neg_integer(),
    MessageRows :: list(tuple()).
%% Row is `{<<"13663125233">>,<<"bob@localhost">>,<<"res1">>,<<binary>>}'.
lookup_messages(UserJID=#jid{lserver=LServer},
                RSM, Start, End, WithJID,
                PageSize, LimitPassed, MaxResultLimit) ->
%   Filter = prepare_filter(UserJID, Start, End, WithJID),
%   TotalCount = calc_count(LServer, Filter),
%   Offset     = calc_offset(LServer, Filter, PageSize, TotalCount, RSM),
%   %% If a query returns a number of stanzas greater than this limit and the
%   %% client did not specify a limit using RSM then the server should return
%   %% a policy-violation error to the client. 
%   case TotalCount - Offset > MaxResultLimit andalso not LimitPassed of
%       true ->
%           {error, 'policy-violation'};

%       false ->
%           MessageRows = extract_messages(LServer, Filter, Offset, PageSize),
%           {ok, {TotalCount, Offset, MessageRows}}
%   end.
    {ok, {0, 0, []}}.


remove_user_from_db(LServer, LUser) ->
    ok.


archive_message(Id, Dir, _LocJID=#jid{luser=LocLUser, lserver=LocLServer},
                RemJID=#jid{lresource=RemLResource}, SrcJID, Packet) ->
    UserID = mod_mam_cache:user_id(LocLServer, LocLUser),
    Data = term_to_binary(Packet, [compressed]),
    BBareRemJID = jlib:jid_to_binary(
            jlib:jid_tolower(jlib:jid_remove_resource(RemJID))),
    BRemJID = jlib:jid_to_binary(jlib:jid_tolower(RemJID)),
    BID = id_to_binary(Id),
    BUserID = user_id_to_binary(UserID),
    BHourID = hour_id(Id, UserID),
    BRemJIDHourID = remote_jid_hour_id(BHourID, BRemJID),
    BBareRemJIDHourID = remote_jid_hour_id(BHourID, BBareRemJID),
    BShortMessID = short_message_id(Id),
    BUserIDRemJID = remote_jid_message_id(Id, BRemJID),
    BUserIDBareRemJID = remote_jid_message_id(Id, BBareRemJID),
    F = fun(Conn) ->
        %% Request a Riak client's ID from the server.
        {ok, ClientID} = riakc_pb_socket:get_client_id(Conn),
        %% Save message body.
        Obj = riakc_obj:new(<<"mam_messages">>, BID, Data),
        ok = riakc_pb_socket:put(Conn, Obj),
        %% Increment total message count in archive.
        ok = riakc_pb_socket:counter_incr(Conn, <<"mam_usr_msg_cnt">>, BUserID, 1),
        ok = riakc_pb_socket:counter_incr(Conn, <<"mam_usr_msg_cnt">>, BUserIDRemJID, 1),
        ok = riakc_pb_socket:counter_incr(Conn, <<"mam_usr_msg_cnt">>, BUserIDBareRemJID, 1),
        %% Write offset into index.
        update_hour_index(Conn, BHourID, BShortMessID),
        update_hour_index(Conn, BRemJIDHourID, BShortMessID),
        update_hour_index(Conn, BBareRemJIDHourID, BShortMessID),
        ok
    end,
    with_connection(LocLServer, F),
    ok.

with_connection(LocLServer, F) ->
    riak_pool:with_connection(mam_cluster, F).

id_to_binary(Id) ->
    <<Id:64/big>>.

user_id_to_binary(UserID) ->
    <<UserID:64/big>>.

hour_id(MessID, UserID) ->
    {Microseconds, _} = mod_mam_utils:decode_compact_uuid(MessID),
    <<UserID:64/big, (hour(Microseconds)):24/big>>.

%% Transforms `{{2013,7,21},{15,43,36}} => {{2013,7,21},{15,0,0}}'.
hour(Ms) ->
    Ms div microseconds_in_hour().

microseconds_in_hour() ->
    3600000000.

short_message_id(Id) ->
    {Microseconds, NodeID} = mod_mam_utils:decode_compact_uuid(Id),
    Offset = Microseconds rem microseconds_in_hour(),
    <<Offset:32/big, NodeID>>.

%% Add this message into a list of messages with the same hour.
update_hour_index(Conn, BHourID, BShortMessID) ->
    case riakc_pb_socket:get(Conn, <<"mam_hour_idx">>, BHourID) of
        {ok, Obj} ->
            Value = merge_short_message_ids([BShortMessID|riakc_obj:get_values(Obj)]),
            Obj2 = riakc_obj:update_value(Obj, Value),
            Obj3 = fix_metadata(Obj2),
            ok = riakc_pb_socket:put(Conn, Obj3);
        {error, notfound} ->
            ?DEBUG("Index ~p not found, create new one.", [BHourID]),
            Obj = riakc_obj:new(<<"mam_hour_idx">>, BHourID, BShortMessID),
            ok = riakc_pb_socket:put(Conn, Obj)
    end.


%% @doc Resolve metadatas' conflict.
fix_metadata(Obj) ->
    case riakc_obj:get_metadatas(Obj) of
        [_] -> Obj;
        [_|_] = Metas -> riakc_obj:update_metadata(Obj, merge_metadatas(Metas))
    end.

%% @doc Naive merge of metadata proplists.
merge_metadatas(Metas) ->
    lists:ukeysort(1, lists:merge(Metas)).

merge_short_message_ids(GroupedIDS) ->
    IDS = lists:usort([ID || IDS <- GroupedIDS, <<ID:40>> <= IDS]),
    << <<ID:40>> || ID <- IDS>>.

remote_jid_hour_id(BHourID, BJID) ->
    <<BHourID/binary, BJID/binary>>.

remote_jid_message_id(Id, BJID) ->
    <<Id:62/big, BJID/binary>>.


%% @doc Ensure, that `UpdateProps' are set in required position.
update_bucket(Conn, BucketName, UpdateProps) ->
    case riakc_pb_socket:get_bucket(Conn, BucketName) of
        {ok, BucketProps} ->
            ?DEBUG("Properties of the bucket ~p are ~p",
                   [BucketName, BucketProps]),
            BucketProps2 = update_props(BucketProps, UpdateProps),
            case compare_props(BucketProps, BucketProps2) of
                true -> ok;
                false ->
                    riakc_pb_socket:set_bucket(Conn, BucketName, BucketProps2)
            end;
        {error, notfound} ->
            riakc_pb_socket:set_bucket(Conn, BucketName, UpdateProps)
    end.

compare_props(Ps1, Ps2) ->
    orddict:from_list(Ps1) == orddict:from_list(Ps2).

update_props(BucketProps, UpdateProps) ->
    Keys = [K || {K,_} <- UpdateProps],
    UpdateProps ++ lists:foldl(fun proplists:delete/2, BucketProps, Keys).

