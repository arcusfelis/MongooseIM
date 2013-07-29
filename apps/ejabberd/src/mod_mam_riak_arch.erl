-module(mod_mam_riak_arch).
-export([start/1,
         archive_message/6,
         wait_flushing/1,
         archive_size/2,
         lookup_messages/8,
         remove_user_from_db/2]).

%% UID
-import(mod_mam_utils,
        [encode_compact_uuid/2]).

-ifdef(TEST).
-include_lib("eunit/include/eunit.hrl").
-endif.

-include_lib("ejabberd/include/ejabberd.hrl").
-include_lib("ejabberd/include/jlib.hrl").
-include_lib("exml/include/exml.hrl").
-include_lib("riakc/include/riakc.hrl").

-record(index_meta, {
    offset,
    total_count,
    last_hour_keys,
    matched_count
}).

message_bucket() ->
    <<"mam_m">>.

message_count_bucket() ->
    <<"mam_c">>.

index_info_bucket() ->
    <<"mam_i">>.

message_count_by_bare_remote_jid_bucket() ->
    <<"mam_cc">>.

message_count_by_full_remote_jid_bucket() ->
    <<"mam_ccc">>.

user_id_bare_remote_jid_index() ->
    {binary_index, "b"}.

user_id_full_remote_jid_index() ->
    {binary_index, "f"}.

key_index() ->
    <<"$key">>.

-define(MEASURE_TIME_TIMER, 1).
-ifdef(MEASURE_TIME_TIMER).
-define(MEASURE_TIME(Tag, Operation), measure_time((Tag), (fun() -> (Operation) end))). 
measure_time(Tag, F) ->
    {Microseconds, Out} = timer:tc(F),
    io:format("~p ~p~n", [Tag, Microseconds]),
    Out.
-else.
-ifdef(MEASURE_TIME_FOLSOM).
-define(MEASURE_TIME(Tag, Operation), measure_time((Tag), (fun() -> (Operation) end))). 
measure_time(Tag, F) ->
    case folsom_metrics:metric_exists(Tag) of
        true ->
            folsom_metrics:histogram_timed_update(Tag, F);
        false ->
            F()
    end.
-else.
-define(MEASURE_TIME(_, Operation), Operation). 
-endif. % MEASURE_TIME_FOLSOM
-endif. % MEASURE_TIME_TIMER

-type unix_timestamp() :: non_neg_integer().

start(Host) ->
    application:start(riak_pool),
    F = fun(Conn) ->
            update_bucket(Conn, message_bucket(), lww_props()),
            update_bucket(Conn, message_count_bucket(), [{allow_mult, true}]),
            update_bucket(Conn, message_count_by_full_remote_jid_bucket(), [{allow_mult, true}]),
            update_bucket(Conn, message_count_by_bare_remote_jid_bucket(), [{allow_mult, true}])
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
%% `#rsm_in{direction = before | aft, index=int(), id = binary()}'
%% `#rsm_in.id' is not inclusive.
%% `#rsm_in.index' is zero-based offset.
%% `Start', `End' and `WithJID' are filters, they are applied BEFORE paging.
paginate(Keys, none, PageSize) ->
    {0, lists:sublist(Keys, PageSize)};
%% Skip first KeysBeforeCnt keys.
paginate(Keys, #rsm_in{direction = undefined, index = KeysBeforeCnt}, PageSize)
    when is_integer(KeysBeforeCnt), KeysBeforeCnt >= 0 ->
    {KeysBeforeCnt, lists:sublist(Keys, KeysBeforeCnt + 1, PageSize)};
%% Requesting the Last Page in a Result Set
paginate(Keys, #rsm_in{direction = before, id = undefined}, PageSize) ->
    TotalCount = length(Keys),
    KeysBeforeCnt = max(0, TotalCount - PageSize),
    {KeysBeforeCnt, lists:sublist(Keys, KeysBeforeCnt + 1, PageSize)};
paginate(Keys, #rsm_in{direction = before, id = Key}, PageSize) ->
    {_, KeysBefore, _KeysAfter} = split_keys(Key, Keys),
    %% Limit keys.
    KeysBeforeCnt = max(0, length(KeysBefore) - PageSize),
    {KeysBeforeCnt, lists:sublist(KeysBefore, KeysBeforeCnt + 1, PageSize)};
paginate(Keys, #rsm_in{direction = aft, id = Key}, PageSize) ->
    {IsDelimExists, KeysBefore, KeysAfter} = split_keys(Key, Keys),
    KeysBeforeCnt = before_count(IsDelimExists, KeysBefore),
    {KeysBeforeCnt, lists:sublist(KeysAfter, PageSize)};
paginate(Keys, _, PageSize) ->
    paginate(Keys, none, PageSize).

-ifdef(TEST).

paginate_test_() ->
    [{"Zero offset",
      ?_assertEqual({0, [1,2,3]},
                    paginate([1,2,3, 4,5,6, 7,8,9],
                             #rsm_in{index = 0}, 3))},
     {"Before key=7 (6,5,4..)",
      ?_assertEqual({3, [4,5,6]},
                    paginate([1,2,3, 4,5,6, 7,8,9, 10,12,13],
                             #rsm_in{direction = before, id = 7}, 3))},
     {"After key=3 (4,5,6..)",
      ?_assertEqual({3, [4,5,6]},
                    paginate([1,2,3, 4,5,6, 7,8,9, 10,12,13],
                             #rsm_in{direction = aft, id = 3}, 3))},
     {"Offset 3",
      ?_assertEqual({3, [4,5,6]},
                    paginate([1,2,3, 4,5,6, 7,8,9, 10,12,13],
                             #rsm_in{index = 3}, 3))},
     {"Last Page",
      ?_assertEqual({6, [7,8,9]},
                    paginate([1,2,3, 4,5,6, 7,8,9],
                             #rsm_in{direction = before}, 3))}
    ].

-endif.

fix_rsm(_BUserID, none) ->
    none;
fix_rsm(_BUserID, RSM=#rsm_in{id = undefined}) ->
    RSM;
fix_rsm(_BUserID, RSM=#rsm_in{id = <<>>}) ->
    RSM#rsm_in{id = undefined};
fix_rsm(BUserID, RSM=#rsm_in{id = BExtMessID}) when is_binary(BExtMessID) ->
    MessID = mod_mam_utils:external_binary_to_mess_id(BExtMessID),
    BMessID = mess_id_to_binary(MessID),
    Key = message_key(BUserID, BMessID),
    RSM#rsm_in{id = Key}.

before_count(IsDelimExists, KeysBefore) ->
    case IsDelimExists of true -> 1; false -> 0 end + length(KeysBefore).

%% @doc Slit the sorted key using a delimiter.
-spec split_keys(DelimKey, Keys) -> {IsDelimExists, KeysBefore, KeysAfter}
    when DelimKey :: Key,
         Keys :: list(Key),
         IsDelimExists :: boolean(),
         KeysBefore :: Keys,
         KeysAfter :: Keys.
split_keys(DelimKey, Keys) ->
    split_keys(DelimKey, Keys, []).

split_keys(DelimKey, [DelimKey|Keys], Acc) ->
    {true, lists:reverse(Acc), Keys};
split_keys(DelimKey, [Key|Keys], Acc) when Key < DelimKey ->
    split_keys(DelimKey, Keys, [Key|Acc]);
split_keys(_, Keys, Acc) ->
    %% `DelimKey' does not exist.
    {false, lists:reverse(Acc), Keys}.

-ifdef(TEST).

split_keys_test_() ->
    [?_assertEqual({true, [1,2,3], [5,6,7]}, split_keys(4, [1,2,3,4,5,6,7]))
    ].

-endif.

lookup_messages(UserJID=#jid{lserver=LocLServer, luser=LocLUser},
                RSM, Start, End, WithJID,
                PageSize, LimitPassed, MaxResultLimit) ->
    UserID = mod_mam_cache:user_id(LocLServer, LocLUser),
    BUserID = user_id_to_binary(UserID),
    SecIndex = choose_index(WithJID),
    BWithJID = maybe_jid_to_opt_binary(LocLServer, WithJID),
    InfoKey = index_info_key(BUserID, BWithJID),
    MessIdxKeyMaker = mess_index_key_maker(BUserID, BWithJID),
    F = fun(Conn) ->
            case riakc_pb_socket:get(Conn, index_info_bucket(), InfoKey) of
                {error, notfound} ->
%                   MessIdxKeyMaker({lower, Start}),
%                   MessIdxKeyMaker({upper, End}),
                    LBound = MessIdxKeyMaker({lower, Start}),
                    UBound = MessIdxKeyMaker({upper, End}),
                    {ok, ?INDEX_RESULTS{keys=Keys}} =
                    riakc_pb_socket:get_index_range(Conn, message_bucket(), SecIndex, LBound, UBound),
                    TotalCount = length(Keys),
                    {Offset, MatchedKeys} = paginate(Keys, fix_rsm(BUserID, RSM), PageSize),
                    case TotalCount - Offset > MaxResultLimit andalso not LimitPassed of
                        true ->
                            {error, 'policy-violation'};

                        false ->
                            FullMatchedKeys = [{message_bucket(), K} || K <- MatchedKeys],
                            {ok, Values} = to_values(Conn, FullMatchedKeys),
                            MessageRows = deserialize_values(Values),
                            {ok, {TotalCount, Offset, lists:usort(MessageRows)}}
                    end;
                {ok, Info} ->
%                   index_info_max(Info),
%                   MessIdxKeyMaker({upper, undefined}),
                    {ok, {0, 0, []}}
            end
        end,
    with_connection(LocLServer, F).
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

deserialize_values(Values) ->
    [binary_to_term(Value) || Value <- Values].

choose_index(undefined) ->
    key_index();
choose_index(#jid{lresource = <<>>}) ->
    user_id_bare_remote_jid_index();
choose_index(#jid{}) ->
    user_id_full_remote_jid_index().


index_info_key(BUserID, undefined) when is_binary(BUserID) ->
    <<BUserID/binary>>;
index_info_key(BUserID, BWithJID) when is_binary(BUserID), is_binary(BWithJID) ->
    <<BUserID/binary, BWithJID/binary>>.

mess_index_key_maker(BUserID, undefined) when is_binary(BUserID) ->
    fun(TimestampBound) ->
        BMessID = timestamp_bound_to_binary(TimestampBound),
        message_key(BUserID, BMessID)
    end;
mess_index_key_maker(BUserID, BWithJID)
    when is_binary(BUserID), is_binary(BWithJID) ->
    fun(TimestampBound) ->
        BMessID = timestamp_bound_to_binary(TimestampBound),
        user_remote_jid_message_id(BUserID, BMessID, BWithJID)
    end.

timestamp_bound_to_binary({lower, undefined}) ->
    <<>>; %% any(binary()) >= <<>>
timestamp_bound_to_binary({lower, Microseconds}) ->
    mess_id_to_binary(mod_mam_utils:encode_compact_uuid(Microseconds, 0));
timestamp_bound_to_binary({upper, undefined}) ->
    <<-1:64>>; %% set all bits.
timestamp_bound_to_binary({upper, Microseconds}) ->
    mess_id_to_binary(mod_mam_utils:encode_compact_uuid(Microseconds, 255)).

remove_user_from_db(LServer, LUser) ->
    ok.

maybe_jid_to_opt_binary(_, undefined) ->
    undefined;
maybe_jid_to_opt_binary(LServer, JID) ->
    jid_to_opt_binary(LServer, JID).

%% Reordered version.
%% Allows sorting by `LServer'.
%% The server host IS before the username.
jid_to_opt_binary(LServer, #jid{lserver=LServer, luser=LUser, lresource= <<>>}) ->
    %% Both clients are on the same server.
    %% Remote JID is bare.
    %% We cannot use `user_id_to_binary/2', because the remote user can be
    %% deleted from the DB.
    <<$@, LUser/binary>>;
jid_to_opt_binary(LServer, #jid{lserver=LServer, luser=LUser, lresource=LResource}) ->
    %% Both clients are on the same server.
    <<$@, LUser/binary, $/, LResource/binary>>;
jid_to_opt_binary(_, #jid{lserver=LServer, luser=LUser, lresource=LResource}) ->
    <<LServer/binary, $@, LUser/binary, $/, LResource/binary>>.


wait_flushing(LServer) ->
    ok.

archive_message(MessID, Dir, _LocJID=#jid{luser=LocLUser, lserver=LocLServer},
                RemJID=#jid{lresource=RemLResource}, SrcJID, Packet) ->
    UserID = mod_mam_cache:user_id(LocLServer, LocLUser),
    BUserID = user_id_to_binary(UserID),
    BMessID = mess_id_to_binary(MessID),
    BBareRemJID = jid_to_opt_binary(LocLServer, jlib:jid_remove_resource(RemJID)),
    BFullRemJID = jid_to_opt_binary(LocLServer, RemJID),
    BUserIDFullRemJIDMessID = user_remote_jid_message_id(BUserID, BMessID, BFullRemJID),
    BUserIDBareRemJIDMessID = user_remote_jid_message_id(BUserID, BMessID, BBareRemJID),
    Key = message_key(BUserID, BMessID),
    Value = term_to_binary({MessID, SrcJID, Packet}, [compressed]),
    F = fun(Conn) ->
        %% Save message body.
        Obj = riakc_obj:new(message_bucket(), Key, Value),
        Obj1 = set_index(Obj, BUserIDFullRemJIDMessID, BUserIDBareRemJIDMessID),
        %% TODO: handle `{error, Reason}'. Should we reinsert again later?
        ok = ?MEASURE_TIME(put_message, riakc_pb_socket:put(Conn, Obj1)),
        ok
    end,
    with_connection(LocLServer, F),
    ok.

set_index(Obj, BUserIDFullRemJIDMessID, BUserIDBareRemJIDMessID) ->
    MD = riakc_obj:get_metadata(Obj),
    Is = [{user_id_full_remote_jid_index(), [BUserIDFullRemJIDMessID]},
          {user_id_bare_remote_jid_index(), [BUserIDBareRemJIDMessID]}],
    MD1 = riakc_obj:set_secondary_index(MD, Is),
    riakc_obj:update_metadata(Obj, MD1).


with_connection(LocLServer, F) ->
    ?MEASURE_TIME(with_connection, 
        riak_pool:with_connection(mam_cluster, F)).

user_id_to_binary(UserID) ->
    <<UserID:64/big>>.

mess_id_to_binary(MessID) ->
    <<MessID:64/big>>.

hour_id(MessID, UserID) ->
    {Microseconds, _} = mod_mam_utils:decode_compact_uuid(MessID),
    <<UserID:64/big, (hour(Microseconds)):24/big>>.

%% Transforms `{{2013,7,21},{15,43,36}} => {{2013,7,21},{15,0,0}}'.
hour(Ms) ->
    Ms div microseconds_in_hour().

hour_to_microseconds(Hour) when is_integer(Hour) ->
    Hour * microseconds_in_hour().

microseconds_in_hour() ->
    3600000000.

hour_lower_bound(Hour) ->
    hour_to_microseconds(Hour).

hour_upper_bound(Hour) ->
    hour_to_microseconds(Hour+1) - 1.

short_message_id(Id) ->
    {Microseconds, NodeID} = mod_mam_utils:decode_compact_uuid(Id),
    Offset = Microseconds rem microseconds_in_hour(),
    <<Offset:32/big, NodeID>>.

%% Add this message into a list of messages with the same hour.
update_hour_index(Conn, BHourID, BShortMessID)
    when is_binary(BHourID), is_binary(BShortMessID) ->
    ?MEASURE_TIME(update_hour_index, 
    case ?MEASURE_TIME(update_hour_index_get,
                       riakc_pb_socket:get(Conn, <<"mam_hour_idx">>, BHourID)) of
        {ok, Obj} ->
            Value = merge_short_message_ids([BShortMessID|riakc_obj:get_values(Obj)]),
            Obj2 = riakc_obj:update_value(Obj, Value),
            Obj3 = fix_metadata(Obj2),
            ok = ?MEASURE_TIME(update_hour_index_put,
                               riakc_pb_socket:put(Conn, Obj3));
        {error, notfound} ->
            ?DEBUG("Index ~p not found, create new one.", [BHourID]),
            Obj = riakc_obj:new(<<"mam_hour_idx">>, BHourID, BShortMessID),
            ok = ?MEASURE_TIME(update_hour_index_put_new,
                               riakc_pb_socket:put(Conn, Obj))
    end).

part_counter_incr(Conn, ClientID, Key, BHourID, N)
    when is_binary(Key), is_binary(BHourID) ->
    ?MEASURE_TIME(part_counter_incr, 
    case ?MEASURE_TIME(part_counter_incr_get,
                       riakc_pb_socket:get(Conn, <<"mam_usr_msg_cnt">>, Key)) of
        {ok, Obj} ->
            Values = riakc_obj:get_values(Obj),
            case Values of
                [_,_|_] -> %% length(Values) > 1
                    ?DEBUG("Merge ~p siblings.", [length(Values)]);
                _ ->
                    ok
            end,
            MergedValue = part_pb_counter:merge(Values),
            Value = part_pb_counter:increment(BHourID, N, ClientID, MergedValue),
            Obj2 = riakc_obj:update_value(Obj, Value),
            Obj3 = fix_metadata(Obj2),
            ok = ?MEASURE_TIME(part_counter_incr_put,
                               riakc_pb_socket:put(Conn, Obj3));
        {error, notfound} ->
            NewValue = part_pb_counter:new(),
            Value = part_pb_counter:increment(BHourID, N, ClientID, NewValue),
            ?DEBUG("Index ~p not found, create new one.", [Key]),
            Obj = riakc_obj:new(<<"mam_usr_msg_cnt">>, Key, Value),
            ok = ?MEASURE_TIME(part_counter_incr_put_new,
                               riakc_pb_socket:put(Conn, Obj))
    end).

%% @doc Resolve metadatas' conflict.
fix_metadata(Obj) ->
    case riakc_obj:get_metadatas(Obj) of
        [_] -> Obj;
        [_|_] = Metas -> riakc_obj:update_metadata(Obj, merge_metadatas(Metas))
    end.

%% @doc Naive merge of metadata proplists.
-spec merge_metadatas(list(Meta)) -> Meta when Meta :: dict().
merge_metadatas([Meta|_]) ->
    Meta.

merge_short_message_ids(GroupedIDs) ->
    case GroupedIDs of
        [_,_,_|_] -> %% length(GroupedIDs) > 2
            ?DEBUG("Merge ~p siblings.", [length(GroupedIDs)-1]);
        _ ->
            ok
    end,
    SortedLists = ([[ID || <<ID:40>> <= IDs] || IDs <- GroupedIDs]),
    IDs = lists:merge(SortedLists),
    << <<ID:40>> || ID <- IDs>>.

remote_jid_hour_id(BHourID, BJID) ->
    <<BHourID/binary, BJID/binary>>.

message_key(BUserID, BMessID)
    when is_binary(BUserID), is_binary(BMessID) ->
    <<BUserID/binary, BMessID/binary>>.

user_remote_jid_message_id(BUserID, BMessID, BJID)
    when is_binary(BUserID), is_binary(BMessID), is_binary(BJID) ->
    <<BUserID/binary, BJID/binary, 0, BMessID/binary>>.


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


ii_last_hour([_|T]) ->
    ii_last_hour(T);
ii_last_hour([{Hour, _}]) ->
    Hour;
ii_last_hour([]) ->
    undefined.

%% @doc Calc amount of entries before the passed hour (non inclusive).
ii_calc_before(Hour, Info) ->
    ii_calc_before(Hour, Info, 0).

ii_calc_before(Hour, [{Hour, _}|_], Acc) ->
    Acc;
ii_calc_before(Hour, [{_, Cnt}|T], Acc) ->
    ii_calc_before(Hour, T, Acc + Cnt);
ii_calc_before(Hour, _, Acc) ->
    %% There is no information about this hour.
    undefined.

%% @doc Return how many entries are send or received within the passed hour.
ii_calc_volume(Hour, [{Hour, Cnt}|_]) ->
    Cnt;
ii_calc_volume(Hour, [_|T]) ->
    ii_calc_volume(Hour, T);
ii_calc_volume(_, _) ->
    %% There is no information about this hour.
    undefined.


to_values(Conn, Keys) ->
    Ops = [{map, {modfun, riak_kv_mapreduce, map_object_value}, undefined, true}],
    case riakc_pb_socket:mapred(Conn, Keys, Ops) of
        {ok, []} ->
            {ok, []};
        {ok, [{0, Values}]} ->
            {ok, Values};
        {error, Reason} ->
            {error, Reason}
    end.



