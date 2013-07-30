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
    Now = mod_mam_utils:now_to_microseconds(now()),

    QueryAllF = fun(Conn) ->
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
        end
    end,

    AnalyseInfoIndexF = fun(Conn, Info, Index) ->
        %% It is true, if there is an hour, that begins with
        %% the first message of the record set.
        IsEmptyHourOffset = case Start of
            undefined -> false;
            _ -> ii_calc_volume(hour(Start), Info) =:= 0
            end,
        %% Count of messages between the hour beginning and `Start'.
        HourOffset = case IsEmptyHourOffset of
            true -> 0;
            _ ->
                S = MessIdxKeyMaker({lower, hour_to_microseconds(Start)}),
                E = MessIdxKeyMaker({upper, Start}),
                get_entry_count_between(Conn, SecIndex, S, E)
            end,
        %% Skip unused part of the index (it is not a part of RSet).
        Info2 = case Start of
            undefined -> Info;
            _ -> ii_from_hour(hour(Start), Info)
            end,
        %% Skip `HourOffset'.
        %% `Info3' is a beginning of the RSet.
        Info3 = ii_skip_hour_offset(HourOffset, Info2),
        LBoundHour = ii_first_hour(Info3),
        LBound = MessIdxKeyMaker({lower, hour_lower_bound(LBoundHour)}),
        %% First hour to be requested from Riak.
        %% `Index2' is a hour offset.
        {Index2, Info4} = ii_skip_n(Index+PageSize, Info3),
%       Skipped = Index - Index2,
        case ii_is_empty(Info4) of
            %% We will select recent entries.
            true ->
                UBound = MessIdxKeyMaker({upper, undefined}),
                {ok, ?INDEX_RESULTS{keys=Keys}} =
                riakc_pb_socket:get_index_range(Conn, message_bucket(), SecIndex, LBound, UBound),
                MatchedKeys = lists:sublist(Keys, PageSize),
                FullMatchedKeys = [{message_bucket(), K} || K <- MatchedKeys],
                {ok, Values} = to_values(Conn, FullMatchedKeys),
                TotalCount = length(Keys) + Index,
                Offset = Index,
                MessageRows = deserialize_values(Values),
                {ok, {TotalCount, Offset, MessageRows}};
            false ->
                %% Last hour to get.
                UBoundHour = ii_first_hour(Info4),
                UBound = MessIdxKeyMaker({upper, hour_upper_bound(UBoundHour)}),
                %% Values from `Info5' will be never matched.
                Info5 = ii_skip_hour_offset(Index2, Info4),
                %% Get actual recent data.
                LastKnownHour = ii_last_hour(Info5),
                S1 = MessIdxKeyMaker({lower, LastKnownHour+1}),
                E1 = MessIdxKeyMaker({upper, undefined}),
                RecentCnt = get_entry_count_between(Conn, SecIndex, S1, E1),
                %% Request values.
                {ok, ?INDEX_RESULTS{keys=Keys}} =
                riakc_pb_socket:get_index_range(Conn, message_bucket(), SecIndex, LBound, UBound),
                MatchedKeys = lists:sublist(Keys, PageSize),
                FullMatchedKeys = [{message_bucket(), K} || K <- MatchedKeys],
                {ok, Values} = to_values(Conn, FullMatchedKeys),
                TotalCount = length(Keys) + Index + RecentCnt,
                Offset = Index,
                MessageRows = deserialize_values(Values),
                {ok, {TotalCount, Offset, MessageRows}}
        end
    end,

    AnalyseInfoF = fun(Conn, Info) ->
        case RSM of
            #rsm_in{index = Index} when is_integer(Index), Index >= 0 ->
                case ii_calc_volume(hour(Start), Info) of
                    %% Only recent messages match.
                    undefined -> QueryAllF(Conn);
                    _ -> AnalyseInfoIndexF(Conn, Info, Index)
                end;
            _ ->
                QueryAllF(Conn)
        end
    end,

    F = fun(Conn) ->
            IgnoreInfo = case {Start, End} of
                {undefined, undefined} -> false; %% unknown range
                {undefined, _}         -> false;
                {Start,     undefined} -> (Now - Start) < 2; %% looking for recent entries
                {Start,     End}       -> (End - Start) < 2  %% A small range
            end,
            case IgnoreInfo of
                true ->
                    QueryAllF(Conn);
                false ->
                    case riakc_pb_socket:get(Conn, index_info_bucket(), InfoKey) of
                        {error, notfound} ->
                            QueryAllF(Conn);
                        {ok, Info} ->
                            AnalyseInfoF(Conn, Info)
                    end
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


%% `[]' is not allowed.
ii_last_hour([_|T]) ->
    ii_last_hour(T);
ii_last_hour([{Hour, _}]) ->
    Hour.

%% `[]' is not allowed.
ii_first_hour([_|T]) ->
    ii_first_hour(T);
ii_first_hour([{Hour, _}]) ->
    Hour.

%% @doc Calc amount of entries before the passed hour (non inclusive).
ii_calc_before(Hour, Info) ->
    ii_calc_before(Hour, Info, 0).

ii_calc_before(Hour, [{Hour, _}|_], Acc) ->
    Acc;
ii_calc_before(Hour, [{_, Cnt}|T], Acc) ->
    ii_calc_before(Hour, T, Acc + Cnt);
ii_calc_before(_, _, _) ->
    %% There is no information about this hour.
    undefined.

ii_from_hour(Hour, [{CurHour, _}|_]=Info) when Hour >= CurHour ->
    Info;
ii_from_hour(Hour, [{_, _}|T]) ->
    ii_from_hour(Hour, T);
ii_from_hour(_, []) ->
    [].

%% @doc Return how many entries are send or received within the passed hour.
%% Info is sortered by `Hour'.
ii_calc_volume(Hour, [{Hour, Cnt}|_]) ->
    Cnt;
ii_calc_volume(Hour, [{CurHour, _}|T]) when Hour < CurHour ->
    ii_calc_volume(Hour, T);
ii_calc_volume(Hour, [_|_]) ->
    %% The information about this hour is skipped.
    %% There are information after this hour.
    0;
ii_calc_volume(_, []) ->
    %% There is no information about this hour.
    undefined.

ii_skip_n(N, [{Hour, Cnt}|T]) when N >= Cnt ->
    ii_skip_n(N - Cnt, T);
ii_skip_n(N, T) ->
    {N, T}.

ii_skip_hour_offset(HourOffset, [{Hour, Cnt}|T]) when Cnt > HourOffset ->
    [{Hour, Cnt - HourOffset}|T].

ii_is_empty(X) -> X == [].


to_values(Conn, Keys) ->
    [assert_valid_key(Key) || Key <- Keys],
    Ops = [{map, {modfun, riak_kv_mapreduce, map_object_value}, undefined, true}],
    case riakc_pb_socket:mapred(Conn, Keys, Ops) of
        {ok, []} ->
            {ok, []};
        {ok, [{0, Values}]} ->
            {ok, Values};
        {error, Reason} ->
            {error, Reason}
    end.

assert_valid_key({Bucket, Key}) when is_binary(Bucket), is_binary(Key) ->
    ok.

%% TODO: use map-reduce here
get_entry_count_between(Conn, SecIndex, LBound, UBound) ->
    {ok, ?INDEX_RESULTS{keys=Keys}} =
    riakc_pb_socket:get_index_range(Conn, message_bucket(), SecIndex, LBound, UBound),
    length(Keys).


-ifdef(TEST).

meck_test_() ->
    {setup, 
     fun load_mock/0,
     fun unload_mock/1,
     [{timeout, 60, [fun long_case/0]}]}.

load_mock() ->
    SM = riakc_pb_socket, %% webscale is here!
    Tab = ets:new(riak_store, [public, ordered_set]),
    IdxTab = ets:new(riak_index, [public, ordered_set]),
    meck:new(SM),
    meck:expect(SM, get_index_range, fun
        (Conn, Bucket, <<"$key">>, LBound, UBound) ->
            %% fun({{Key, _} = FullKey, _Obj})
            %%     when FullKey >= {Bucket, LBound}
            %%          FullKey =< {Bucket, UBound} -> Key end
            MS = [{
                    {'$1', '_'},
                    [{'>=', '$1', {{Bucket, LBound}}},
                     {'=<', '$1', {{Bucket, UBound}}}],
                    [{element, 2, '$1'}]
                  }],
            Keys = ets:select(Tab, MS),
            {ok, ?INDEX_RESULTS{keys=Keys}};
        (Conn, Bucket, Index, LBound, UBound) ->
            %% fun({SecIndexKey, {Key, _}})
            %%     when SecIndexKey >= {Index, Bucket, LBound}
            %%          SecIndexKey =< {Index, Bucket, UBound} -> Key end
            MS = [{
                    {'$1', '$2'},
                    [{'>=', '$1', {{Index, Bucket, LBound}}},
                     {'=<', '$1', {{Index, Bucket, UBound}}}],
                    [{element, 2, '$2'}]
                  }],
            Keys = ets:select(IdxTab, MS),
            {ok, ?INDEX_RESULTS{keys=Keys}}
        end),
    meck:expect(SM, mapred, fun(Conn, Keys, Ops) ->
        case Ops of
        %% Get values
        %% see `to_values/2'
        [{map, {modfun, riak_kv_mapreduce, map_object_value}, undefined, true}] ->
            case [proplists:get_value(value, Obj)
                 || Key <- Keys,
                    {Key, Obj} <- ets:lookup(Tab, Key)] of
                []     -> {ok, []};
                Values -> {ok, [{0, Values}]}
            end
        end
        end),
    meck:expect(SM, get_bucket, fun(Conn, Bucket) ->
        {ok, []}
        end),
    meck:expect(SM, set_bucket, fun(Conn, Bucket, Opts) ->
        ok
        end),
    meck:expect(SM, put, fun(Conn, Obj) ->
        Key    = proplists:get_value(key, Obj),
        Bucket = proplists:get_value(bucket, Obj),
        Md     = proplists:get_value(metadata, Obj),
        Is     = proplists:get_value(secondary_index, Md, []),
        ets:insert(Tab, {{Bucket, Key}, Obj}),
        ets:insert(IdxTab, [{I, Bucket, Key} || I <- Is]),
        ok
        end),
    meck:expect(SM, get, fun(Conn, Bucket, Key) ->
        case ets:lookup(Tab, {Bucket, Key}) of
            [{_,Obj}] -> {ok, Obj};
            []        -> {error, notfound}
        end
        end),
    OM = riakc_obj,
    meck:new(OM),
    meck:expect(OM, new, fun(Bucket, Key, Value) ->
        [{bucket, Bucket}, {key, Key}, {value, Value}]
        end),
    meck:expect(OM, get_metadata, fun(Obj) ->
        proplists:get_value(metadata, Obj, [])
        end),
    meck:expect(OM, get_metadatas, fun(Obj) ->
        [proplists:get_value(metadata, Obj, [])]
        end),
    meck:expect(OM, update_metadata, fun(Obj, Md) ->
        [{metadata, Md}|Obj]
        end),
    meck:expect(OM, get_value, fun(Obj) ->
        proplists:get_value(value, Obj)
        end),
    meck:expect(OM, get_values, fun(Obj) ->
        [proplists:get_value(value, Obj)]
        end),
    meck:expect(OM, update_value, fun(Obj, Value) ->
        [{value, Value}|Obj]
        end),
    meck:expect(OM, set_secondary_index, fun(Md, Is) ->
        Idx = proplists:get_value(secondary_index, Md, []),
        [{secondary_index, Is ++ Idx}|Md]
        end),
    CM = mod_mam_cache,
    meck:new(CM),
    meck:expect(CM, user_id, fun(LServer, LUser) ->
            user_id(LUser)
        end),
    PM = riak_pool,
    meck:new(PM),
    meck:expect(PM, with_connection, fun(mam_cluster, F) ->
            F(conn)
        end),
    ok.

unload_mock(_) ->
    meck:unload(riakc_pb_socket),
    meck:unload(riakc_obj),
    ok.

long_case() ->
    %% Alice to Cat
    Log1Id = fun(Time) ->
        Date = iolist_to_binary("2000-07-21T" ++ Time ++ "Z"),
        id(Date)
        end,
    Log2Id = fun(Time) ->
        Date = iolist_to_binary("2000-07-22T" ++ Time ++ "Z"),
        id(Date)
        end,
    A2C = fun(Time, Text) ->
        Packet = message(iolist_to_binary(Text)),
        ?MODULE:archive_message(Log1Id(Time), outgoing, alice(), cat(), alice(),
                                Packet)
        end,
    %% Cat to Alice
    C2A = fun(Time, Text) ->
        Packet = message(iolist_to_binary(Text)),
        ?MODULE:archive_message(Log1Id(Time), incoming, alice(), cat(), cat(),
                                Packet)
        end,
    %% Alice to Mad Tea Party
    A2P = fun(Time, Text) ->
        Date = iolist_to_binary("2000-07-22T" ++ Time ++ "Z"),
        Packet = message(iolist_to_binary(Text)),
        ?MODULE:archive_message(id(Date), outgoing, alice(), party(), alice(),
                                Packet)
        end,
    %% March Hare - M, Hatter - H and Dormouse - D
    PM2A = fun(Time, Text) ->
        Date = iolist_to_binary("2000-07-21T" ++ Time ++ "Z"),
        Packet = message(iolist_to_binary(Text)),
        ?MODULE:archive_message(id(Date), incoming, alice(),
                                march_hare_at_party(), march_hare_at_party(),
                                Packet)
        end,
    PH2A = fun(Time, Text) ->
        Date = iolist_to_binary("2000-07-21T" ++ Time ++ "Z"),
        Packet = message(iolist_to_binary(Text)),
        ?MODULE:archive_message(id(Date), incoming, alice(),
                                hatter_at_party(), hatter_at_party(),
                                Packet)
        end,
    PD2A = fun(Time, Text) ->
        Date = iolist_to_binary("2000-07-21T" ++ Time ++ "Z"),
        Packet = message(iolist_to_binary(Text)),
        ?MODULE:archive_message(id(Date), incoming, alice(),
                                dormouse_at_party(), dormouse_at_party(),
                                Packet)
        end,
    %% Credits to http://wiki.answers.com/Q/What_are_some_dialogues_said_by_Alice_in_Alice%27s_Adventures_in_Wonderland
    Log1 = fun(A, C) ->
    %% Alice sends a message to Cheshire Cat.
    A("01:50:00", "Cheshire Puss, would you tell me, please, which way I ought to go from here?"),
    C("01:50:05", "That depends a good deal on where you want to get to."),
    A("01:50:15", "I don't much care where-"),
    C("01:50:16", "Then it doesn't matter which way you go."),
    A("01:50:17", "-so long as I get somewhere."),
    C("01:50:20", "Oh, you're sure to do that, if you only walk long enough."),
    A("01:50:25", "What sort of people live about here?"),
    C("01:50:30", "In THAT direction lives a Hatter: and in THAT direction lives"
                  " a March Hare. Visit either you like: they're both mad."),
    A("01:50:40", "But I don't want to go among mad people."),
    C("01:50:45", "Oh, you can't help that, we're all mad here. I'm mad. You're mad."),
    A("01:50:50", "How do you know I'm mad?"),
    C("01:50:55", "You must be, or you wouldn't have come here."),
    ok
    end,
    Log2 = fun(A, M, H, D) ->
    %% March Hare - M, Hatter - H and Dormouse - D
    M("06:49:00", "No room! No room!"),
    H("06:49:01", "No room! No room!"),
    D("06:49:02", "No room! No room!"),
    A("06:49:05", "There's PLENTY of room!"),
    M("06:49:10", "Have some wine."),
    A("06:49:15", "I don't see any wine."),
    M("06:49:20", "There isn't any."),
    A("06:49:25", "Then it wasn't very civil of you to offer it."),
    M("06:49:30", "It wasn't very civil of you to sit down without being invited."),
    A("06:49:36", "I didn't know it was your table, it's laid for a great many more than three."),
    H("06:49:45", "Your hair wants cutting."),
    A("06:49:50", "You should learn not to make personal remarks, it's very rude."),
    H("06:50:05", "Why is a raven like a writing-desk?"),
    A("06:50:10", "I believe I can guess that."),
    M("06:50:25", "Do you mean that you think you can find out the answer to it?"),
    A("06:50:28", "Exactly so."),
    M("06:50:33", "Then you should say what you mean."),
    A("06:50:40", "I do, at least--at least I mean what I say--that's the same thing, you know."),
    H("06:50:50", "Not the same thing a bit! You might just as well say that "
                  "\"I see what I eat\" is the same thing as \"I eat what I see\"!"),
    M("06:51:00", "You might just as well say that \"I like what I get\" "
                  "is the same thing as \"I get what I like\"!"),
    D("06:51:10", "You might just as well say that \"I breathe when I sleep\" "
                  "is the same thing as \"I sleep when I breathe\"!"),
    H("06:51:15", "It IS the same thing with you. "),
    ok
    end,
    Log1(A2C, C2A),
    Log2(A2P, PM2A, PH2A, PD2A),
    
    %% lookup_messages(UserJID, RSM, Start, End, WithJID,
    %%                 PageSize, LimitPassed, MaxResultLimit) ->
    %% {ok, {TotalCount, Offset, MessageRows}}
    %% First 5.
    
    assert_keys(34, 0,
                [join_date_time("2000-07-21", Time)
                 || Time <- ["01:50:00", "01:50:05", "01:50:15",
                             "01:50:16", "01:50:17"]],
                lookup_messages(alice(),
                    none, undefined, undefined, undefined,
                    5, true, 5)),
    ok.

assert_keys(ExpectedTotalCount, ExpectedOffset, DateTimes,
            {ok, {TotalCount, Offset, MessageRows}}) ->
    
    ?assertEqual(ExpectedTotalCount, TotalCount),
    ?assertEqual(ExpectedOffset, Offset),
    ?assertEqual(DateTimes,
        [mess_id_to_iso_date_time(MessID) || {MessID, _, _} <- MessageRows]).

join_date_time(Date, Time) ->
    Date ++ "T" ++ Time.

mess_id_to_iso_date_time(MessID) ->
    {Microseconds, _} = mod_mam_utils:decode_compact_uuid(MessID),
    TimeStamp = mod_mam_utils:microseconds_to_now(Microseconds),
    {{Year, Month, Day}, {Hour, Minute, Second}} =
    calendar:now_to_universal_time(TimeStamp),
    lists:flatten(
      io_lib:format("~4..0w-~2..0w-~2..0wT~2..0w:~2..0w:~2..0w",
                    [Year, Month, Day, Hour, Minute, Second])).


alice() ->
    #jid{luser = <<"alice">>, lserver = <<"wonderland">>, lresource = <<>>,
          user = <<"alice">>,  server = <<"wonderland">>,  resource = <<>>}.

cat() ->
    #jid{luser = <<"cat">>, lserver = <<"wonderland">>, lresource = <<>>,
          user = <<"cat">>,  server = <<"wonderland">>,  resource = <<>>}.

party() ->
    #jid{luser = <<"party">>, lserver = <<"wonderland">>, lresource = <<>>,
          user = <<"party">>,  server = <<"wonderland">>,  resource = <<>>}.


march_hare_at_party() ->
    #jid{luser = <<"party">>, lserver = <<"wonderland">>, lresource = <<"march_hare">>,
          user = <<"party">>,  server = <<"wonderland">>,  resource = <<"march_hare">>}.

hatter_at_party() ->
    #jid{luser = <<"party">>, lserver = <<"wonderland">>, lresource = <<"hatter">>,
          user = <<"party">>,  server = <<"wonderland">>,  resource = <<"hatter">>}.
dormouse_at_party() ->
    #jid{luser = <<"party">>, lserver = <<"wonderland">>, lresource = <<"dormouse">>,
          user = <<"party">>,  server = <<"wonderland">>,  resource = <<"dormouse">>}.

user_id(<<"alice">>) -> 1.

message(Text) when is_binary(Text) ->
    #xmlel{
        name = <<"message">>,
        children = [
            #xmlel{
                name = <<"body">>,
                children = #xmlcdata{content = Text}
            }]
    }.

id(Date) ->
    Microseconds = mod_mam_utils:maybe_microseconds(Date),
    encode_compact_uuid(Microseconds, 1).


-endif.
