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

-type message_row() :: tuple().

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

digest_bucket() ->
    <<"mam_d">>.

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

digest_creation_threshold(Host) ->
    gen_mod:get_module_opt(Host, mod_mam, digest_creation_threshold, 100).


lookup_messages(UserJID=#jid{lserver=LocLServer, luser=LocLUser},
                RSM, Start, End, WithJID,
                PageSize, LimitPassed, MaxResultLimit) ->
    UserID = mod_mam_cache:user_id(LocLServer, LocLUser),
    BUserID = user_id_to_binary(UserID),
    SecIndex = choose_index(WithJID),
    BWithJID = maybe_jid_to_opt_binary(LocLServer, WithJID),
    DigestKey = digest_key(BUserID, BWithJID),
    MessIdxKeyMaker = mess_index_key_maker(BUserID, BWithJID),
    Now = mod_mam_utils:now_to_microseconds(now()),

    QueryAllF = fun(Conn) ->
        LBound = MessIdxKeyMaker({lower, Start}),
        UBound = MessIdxKeyMaker({upper, End}),
        Keys = get_key_range(Conn, SecIndex, LBound, UBound),
        TotalCount = length(Keys),
        {Offset, MatchedKeys} = paginate(Keys, fix_rsm(BUserID, RSM), PageSize),
        case TotalCount - Offset > MaxResultLimit andalso not LimitPassed of
            true ->
                {error, 'policy-violation'};

            false ->
                MessageRows = get_message_rows(Conn, MatchedKeys),
                {ok, {TotalCount, Offset, MessageRows}}
        end
    end,


    AnalyseDigestIndexF = fun(Conn, Digest, Offset) ->
        %% It is true, when there is an hour, that begins with
        %% the first message of the record set.
        IsEmptyHourOffset = case Start of
            undefined -> true;
            _ -> dig_calc_volume(hour(Start), Digest) =:= 0
            end,
        %% Count of messages from the hour beginning to `Start'.
        HourOffset = case IsEmptyHourOffset of
            true -> 0;
            false ->
                S = MessIdxKeyMaker({lower, hour_lower_bound(hour(Start))}),
                E = MessIdxKeyMaker({upper, Start}),
                get_entry_count_between(Conn, SecIndex, S, E)
            end,
        %% Skip unused part of the index (it is not a part of RSet).
        Digest2 = case is_defined(Start) of
            false -> Digest;
            true  -> dig_from_hour(hour(Start), Digest)
            end,
        %% Skip `HourOffset'.
        %% `Digest3' is a beginning of the RSet.
        Digest3 = dig_skip_hour_offset(HourOffset, Digest2),
        %% Looking for the erliest hour, that should be loaded from Riak.
        %% `LeftOffset' is how many etries left to skip.
        {LeftOffset, Digest4} = case is_defined(End) of
            true  -> dig_skip_n_with_border(Offset, hour(End), Digest3);
            false -> dig_skip_n(Offset, Digest3)
            end,
        LBoundHour = dig_first_hour(Digest4),
        LBound = MessIdxKeyMaker({lower, hour_lower_bound(LBoundHour)}),
        %% `Left' is how many records are left to fill a full page.
        {Left, Digest5} = case is_defined(End) of
            true  -> dig_skip_n_with_border(LeftOffset+PageSize, hour(End), Digest4);
            false -> dig_skip_n(LeftOffset+PageSize, Digest4)
            end,
%       Skipped = Offset - Left,
        Strategy = case dig_is_empty(Digest5) of
            true  -> get_whole_range;
            false ->
                case is_defined(End) andalso dig_first_hour(Digest5) >= hour(End) of
                    true  -> handle_border;
                    false -> handle_page_limit
                end
            end,
        case Strategy of
            %% We will select recent entries only.
            get_whole_range ->
                UBound = MessIdxKeyMaker({upper, End}),
                Keys = get_key_range(Conn, SecIndex, LBound, UBound),
                %% Tip: `LeftOffset' is zero-based, but `sublist' expects offset from `1'.
                MatchedKeys = lists:sublist(Keys, LeftOffset+1, PageSize),
                TotalCount = length(Keys) + Offset - LeftOffset,
                MessageRows = get_message_rows(Conn, MatchedKeys),
                {ok, {TotalCount, Offset, MessageRows}};
            handle_border ->
                UBound = MessIdxKeyMaker({upper, End}),
                %% Request values.
                Keys = get_key_range(Conn, SecIndex, LBound, UBound),
                MatchedKeys = lists:sublist(Keys, LeftOffset+1, PageSize),
                TotalCount = length(Keys) + Offset - LeftOffset,
                MessageRows = get_message_rows(Conn, MatchedKeys),
                {ok, {TotalCount, Offset, MessageRows}};
            %% Not all index digest was traversed, stop here.
            handle_page_limit ->
                %% `UBoundHour' is a last hour in the range to get.
                UBoundHour = dig_first_hour(Digest5),
                UBound = MessIdxKeyMaker({upper, hour_upper_bound(UBoundHour)}),
                %% Get actual recent data.
                LastKnownHour = dig_last_hour(Digest5),
                %% Values from `DigestX' will be never matched.
%               DigestX = dig_skip_hour_offset(Left, Digest5),
                %% Values from `Digest6' will not be extracted.
                Digest6 = dig_tail(Digest5),
                DigestLeft = case is_defined(End) of
                    true  -> dig_calc_before(hour(End), Digest6);
                    false -> dig_total(Digest6)
                    end,
                S1 = MessIdxKeyMaker({lower, hour_lower_bound(LastKnownHour+1)}),
                E1 = MessIdxKeyMaker({upper, End}),
                RecentCnt = get_entry_count_between(Conn, SecIndex, S1, E1),
                %% Request values.
                Keys = get_key_range(Conn, SecIndex, LBound, UBound),
                MatchedKeys = lists:sublist(Keys, LeftOffset+1, PageSize),
                TotalCount = length(Keys) + Offset - LeftOffset + DigestLeft + RecentCnt,
                MessageRows = get_message_rows(Conn, MatchedKeys),
                {ok, {TotalCount, Offset, MessageRows}}
        end
    end,

    AnalyseDigestLastPageF = fun(Conn, Digest) ->
        %% Get actual recent data.
        %% Where the lower bound is located: before, inside or after the index digest.
        StartPos = dig_position_start(Start, Digest),
        EndPos = dig_position_end(End, Digest),
        %% There are impossible cases due to `Start =< End'.
        Strategy =
        case {StartPos, EndPos} of
            {before,   before} -> empty;
            {before,   inside} -> upper_bounded; % digest only
            {before,  'after'} -> whole_digest_and_recent;
          % {inside,   before} -> impossible;
            {inside,   inside} -> bounded; % digest only
            {inside,  'after'} -> lower_bounded;
          % {'after',  before} -> impossible;
          % {'after',  inside} -> impossible;
            {'after', 'after'} -> recent_only
        end,
        HasLBound = case Strategy of
            bounded       -> true;
            lower_bounded -> true;
            _             -> false
        end,
        case Strategy of
            empty -> {ok, {0, 0, []}};
            recent_only -> QueryAllF(Conn);
            whole_digest_and_recent ->
                Keys = get_minimum_key_range_before(
                    Conn, MessIdxKeyMaker, SecIndex, End, PageSize, Digest),
                RecentCnt = filter_and_count_recent_keys(Keys, Digest),
                TotalCount = dig_total(Digest) + RecentCnt,
                MatchedKeys = sublist_r(Keys, PageSize),
                MessageRows = get_message_rows(Conn, MatchedKeys),
                Offset = TotalCount - length(MatchedKeys),
                {ok, {TotalCount, Offset, MessageRows}};
            lower_bounded ->
                %% Contains records, that will be in RSet for sure
                %% Digest2 does not contain hour(Start).
                Digest2 = dig_after_hour(hour(Start), Digest),
                case dig_total(Digest2) < PageSize of
                true -> QueryAllF(Conn); %% It is a small range
                false -> 
                    StartHourCnt = case dig_is_skipped(hour(Start), Digest) of
                        true -> 0; 
                        false -> get_hour_entry_count_after(
                            Conn, MessIdxKeyMaker, SecIndex, Start)
                        end,
                    Keys = get_minimum_key_range_before(
                        Conn, MessIdxKeyMaker, SecIndex, End, PageSize, Digest2),
                    RecentCnt = filter_and_count_recent_keys(Keys, Digest2),
                    TotalCount = StartHourCnt + dig_total(Digest2) + RecentCnt,
                    MatchedKeys = sublist_r(Keys, PageSize),
                    MessageRows = get_message_rows(Conn, MatchedKeys),
                    Offset = TotalCount - length(MatchedKeys),
                    {ok, {TotalCount, Offset, MessageRows}}
                end;
            upper_bounded ->
                Digest2 = dig_before_hour(hour(End), Digest),
                case dig_total(Digest2) < PageSize of
                true -> QueryAllF(Conn); %% It is a small range
                false -> 
                    Keys = get_minimum_key_range_before(
                        Conn, MessIdxKeyMaker, SecIndex, End, PageSize, Digest2),
                    LastHourCnt = filter_and_count_recent_keys(Keys, Digest2),
                    TotalCount = dig_total(Digest2) + LastHourCnt,
                    MatchedKeys = sublist_r(Keys, PageSize),
                    MessageRows = get_message_rows(Conn, MatchedKeys),
                    Offset = TotalCount - length(MatchedKeys),
                    {ok, {TotalCount, Offset, MessageRows}}
                end;
            bounded ->
                Digest2 = dig_before_hour(hour(End),
                    dig_after_hour(hour(Start), Digest)),
                case dig_total(Digest2) < PageSize of
                true -> QueryAllF(Conn); %% It is a small range
                false -> 
                    StartHourCnt = case dig_is_skipped(hour(Start), Digest) of
                        true -> 0; 
                        false -> get_hour_entry_count_after(
                            Conn, MessIdxKeyMaker, SecIndex, Start)
                        end,
                    Keys = get_minimum_key_range_before(
                        Conn, MessIdxKeyMaker, SecIndex, End, PageSize, Digest2),
                    LastHourCnt = filter_and_count_recent_keys(Keys, Digest2),
                    TotalCount = StartHourCnt + dig_total(Digest2) + LastHourCnt,
                    MatchedKeys = sublist_r(Keys, PageSize),
                    MessageRows = get_message_rows(Conn, MatchedKeys),
                    Offset = TotalCount - length(MatchedKeys),
                    {ok, {TotalCount, Offset, MessageRows}}
                end;
            _ -> QueryAllF(Conn)
        end
    end,

    AnalyseDigestF = fun(Conn, Digest) ->
        case RSM of
            #rsm_in{index = Index} when is_integer(Index), Index >= 0 ->
                %% If `Start' is not recorded, that requested result
                %% set contains new messages only
                case is_defined(Start) andalso not dig_is_recorded(hour(Start), Digest) of
                    %% Match only recent messages.
                    true -> QueryAllF(Conn);
                    false -> AnalyseDigestIndexF(Conn, Digest, Index)
                end;
            %% Last page.
            #rsm_in{direction = before, id = undefined} ->
                AnalyseDigestLastPageF(Conn, Digest);
            #rsm_in{direction = before, id = Id} ->
                QueryAllF(Conn);
            #rsm_in{direction = aft, id = Id} ->
                QueryAllF(Conn);
            _ ->
                QueryAllF(Conn)
        end
    end,

    F = fun(Conn) ->
            case is_short_range(Now, Start, End) of
                true ->
                    %% ignore index digest.
                    QueryAllF(Conn);
                false ->
                    case get_actual_digest(
                        Conn, DigestKey, Now, SecIndex, MessIdxKeyMaker) of
                        {error, notfound} ->
                            Result = QueryAllF(Conn),
                            should_create_digest(LocLServer, Result)
                            andalso
                            create_digest(Conn, DigestKey,
                                fetch_all_keys(Conn, SecIndex, MessIdxKeyMaker)),
                            Result;
                            
                        {ok, Digest} ->
                            AnalyseDigestF(Conn, Digest)
                    end
            end
        end,
    with_connection(LocLServer, F).

should_create_digest(LocLServer, {ok, {TotalCount, _, _}}) ->
    TotalCount >= digest_creation_threshold(LocLServer);
should_create_digest(_, _) ->
    false.

create_digest(Conn, DigestKey, Keys) ->
    Digest = dig_new(Keys),
    Value = term_to_binary(Digest),
    DigestObj = riakc_obj:new(digest_bucket(), DigestKey, Value),
    riakc_pb_socket:put(Conn, DigestObj).

get_actual_digest(Conn, DigestKey, Now, SecIndex, MessIdxKeyMaker) ->
    case riakc_pb_socket:get(Conn, digest_bucket(), DigestKey) of
        {error, notfound} ->
            {error, notfound};
        {ok, DigestObj} ->
            DigestObj2 = merge_siblings(DigestObj),
            Digest2 = binary_to_term(riakc_obj:get_value(DigestObj2)),
            Last = last_modified(DigestObj2),
            case hour(Last) =:= hour(Now) of
                %% Digest is actual.
                true ->
                    %% Write the merged version.
                    has_siblings(DigestObj) andalso riakc_pb_socket:put(Conn, DigestObj2),
                    {ok, Digest2};
                false ->
                    Digest3 = update_digest(Conn, Last, Now, Digest2, SecIndex, MessIdxKeyMaker),
                    DigestObj3 = riakc_obj:update_value(term_to_binary(Digest3), DigestObj2),
                    riakc_pb_socket:put(Conn, DigestObj3),
                    {ok, Digest3}
            end
    end.

has_siblings(Obj) ->
    length(riakc_obj:get_values(Obj)) > 1.

merge_siblings(DigestObj) ->
    case has_siblings(DigestObj) of
        true ->
            DigestObj2 = fix_metadata(DigestObj),
            %% TODO: write me
            riakc_obj:update_value(error(todo), DigestObj2);
        false ->
            DigestObj
    end.
            

%% @doc Request new entries from the server.
%% `Last' and `Now' are microseconds.
update_digest(Conn, Last, Now, Digest, SecIndex, MessIdxKeyMaker) ->
    LBound = MessIdxKeyMaker({lower, hour_upper_bound(hour(Last))}),
    UBound = MessIdxKeyMaker({upper, hour_upper_bound(hour(Now) - 1)}),
    {ok, ?INDEX_RESULTS{keys=Keys}} =
    riakc_pb_socket:get_index_range(Conn, message_bucket(), SecIndex, LBound, UBound),
    dig_add_keys(Keys, Digest).
    

%% @doc Return mtime of the object in microseconds.
last_modified(Obj) ->
    MD = riakc_obj:get_metadata(Obj),
    TimeStamp = proplists:get_value(<<"X-Riak-Last-Modified">>, MD),
    mod_mam_utils:now_to_microseconds(TimeStamp).

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


digest_key(BUserID, undefined) when is_binary(BUserID) ->
    <<BUserID/binary>>;
digest_key(BUserID, BWithJID) when is_binary(BUserID), is_binary(BWithJID) ->
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
    mess_id_to_binary(encode_compact_uuid(Microseconds, 0));
timestamp_bound_to_binary({upper, undefined}) ->
    <<-1:64>>; %% set all bits.
timestamp_bound_to_binary({upper, Microseconds}) ->
    mess_id_to_binary(encode_compact_uuid(Microseconds, 255)).

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
hour(Ms) when is_integer(Ms) ->
    Ms div microseconds_in_hour().

hour_to_microseconds(Hour) when is_integer(Hour) ->
    Hour * microseconds_in_hour().

microseconds_in_hour() ->
    3600000000.

hour_lower_bound(Hour) ->
    hour_to_microseconds(Hour).

hour_upper_bound(Hour) ->
    hour_to_microseconds(Hour+1) - 1.

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


message_key(BUserID, BMessID)
    when is_binary(BUserID), is_binary(BMessID) ->
    <<BUserID/binary, BMessID/binary>>.

user_remote_jid_message_id(BUserID, BMessID, BJID)
    when is_binary(BUserID), is_binary(BMessID), is_binary(BJID) ->
    <<BUserID/binary, BJID/binary, 0, BMessID/binary>>.


key_to_microseconds(Key) when is_binary(Key) ->
    {Microseconds, _} = mod_mam_utils:decode_compact_uuid(key_to_mess_id(Key)),
    Microseconds.

key_to_mess_id(Key) ->
    %% Get 8 bytes.
    <<MessID:64/big>> = binary:part(Key, byte_size(Key), -8),
    MessID.


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

is_defined(X) -> X =/= undefined.

%% `[]' is not allowed.
dig_last_hour([_|[_|_]=T]) ->
    dig_last_hour(T);
dig_last_hour([{Hour, _}]) ->
    Hour.


%% `[]' is not allowed.
dig_first_hour([{Hour,_}|_]) ->
    Hour.

%% @doc Calc amount of entries before the passed hour (non inclusive).
dig_calc_before(Hour, Digest) ->
    dig_calc_before(Hour, Digest, 0).

dig_calc_before(Hour, [{CurHour, _}|_], Acc) when CurHour >= Hour ->
    Acc;
dig_calc_before(Hour, [{_, Cnt}|T], Acc) ->
    dig_calc_before(Hour, T, Acc + Cnt);
dig_calc_before(_, _, _) ->
    %% There is no information about this hour.
    undefined.


dig_from_hour(Hour, [{CurHour, _}|T]) when CurHour < Hour ->
    dig_from_hour(Hour, T);
dig_from_hour(_, T) ->
    T.

%% @doc Returns entries for hours higher than `Hour'.
dig_after_hour(Hour, [{CurHour, _}|T]) when CurHour =< Hour ->
    dig_after_hour(Hour, T);
dig_after_hour(_, T) ->
    T.

dig_to_hour(Hour, [{CurHour, _}=H|T]) when CurHour =< Hour ->
    [H|dig_to_hour(Hour, T)];
dig_to_hour(_, _) ->
    [].

%% @doc Returns entries for hours lower than `Hour'.
dig_before_hour(Hour, [{CurHour, _}=H|T]) when CurHour < Hour ->
    [H|dig_before_hour(Hour, T)];
dig_before_hour(_, _) ->
    [].

%% @doc Return how many entries are send or received within the passed hour.
%% Digest is sortered by `Hour'.
dig_calc_volume(Hour, [{Hour, Cnt}|_]) ->
    Cnt;
dig_calc_volume(Hour, [{CurHour, _}|T]) when Hour < CurHour ->
    dig_calc_volume(Hour, T);
dig_calc_volume(_, [_|_]) ->
    %% No messages was archived during this hour,
    %% but there are records after this hour.
    0;
dig_calc_volume(_, []) ->
    %% There is no information about this hour yet (but it can be in the future).
    undefined.

dig_total(Digest) ->
    dig_total(Digest, 0).

dig_total([{_, Cnt}|T], Acc) ->
    dig_total(T, Acc + Cnt);
dig_total([], Acc) ->
    Acc.

%% @doc Return length of the key list ignoring keys from digest.
%% Note: Digest must be older, than keys.
filter_and_count_recent_keys(Keys, Digest) ->
    LastHour = dig_last_hour(Digest),
    dig_total(dig_after_hour(LastHour, dig_new(Keys))).
    

dig_is_recorded(Hour, Digest) ->
    %% Tip: if no information exists after specified hour,
    %%      `dig_calc_volume' returns `undefined'.
    is_defined(dig_calc_volume(hour(Hour), Digest)).

dig_is_skipped(Hour, Digest) ->
    dig_calc_volume(hour(Hour), Digest) =:= 0.
                

dig_skip_n(N, [{_, Cnt}|T]) when N >= Cnt ->
    dig_skip_n(N - Cnt, T);
dig_skip_n(N, T) ->
    {N, T}.

dig_skip_nr(N, Digest) ->
    {N1, T} = dig_skip_n(N, lists:reverse(Digest)),
    {N1, lists:reverse(T)}.

dig_skip_n_with_border(N, BorderHour, [{Hour, _}|_]=T) when Hour >= BorderHour ->
    {N, T}; %% out of the border
dig_skip_n_with_border(N, BorderHour, [{_, Cnt}|T]) when N >= Cnt ->
    dig_skip_n_with_border(N - Cnt, BorderHour, T);
dig_skip_n_with_border(N, _, T) ->
    {N, T}.

dig_skip_hour_offset(HourOffset, [{Hour, Cnt}|T]) when Cnt > HourOffset ->
    [{Hour, Cnt - HourOffset}|T].

dig_tail([_|T]) -> T.

dig_is_empty(X) -> X == [].

dig_add_keys(Keys, Digest) ->
    %% Sorted.
    Digest ++ dig_new(Keys).

%% Where `Hour' is located: before, inside or after the index digest.
dig_position(Hour, Digest) ->
    FirstKnownHour = dig_first_hour(Digest),
    LastKnownHour  = dig_last_hour(Digest),
    case Hour < FirstKnownHour of
    true -> before;
    false ->
        case Hour > LastKnownHour of
        true -> 'after';
        false -> inside
        end
    end.

dig_position_start(Start, Digest) ->
    case is_defined(Start) of
    false -> before;
    true  -> dig_position(hour(Start), Digest)
    end.

dig_position_end(End, Digest) ->
    case is_defined(End) of
    false -> 'after';
    true  -> dig_position(hour(End), Digest)
    end.

dig_new(Keys) ->
    frequency([hour(key_to_microseconds(Key)) || Key <- Keys]).

%% Returns `[{Hour, MessageCount}]'.
frequency([H|T]) ->
    frequency_1(T, H, 1);
frequency([]) ->
    [].
    
frequency_1([H|T], H, N) ->
    frequency_1(T, H, N+1);
frequency_1(T, H, N) ->
    [{H, N}|frequency(T)].


to_values(Conn, Keys) ->
    [assert_valid_key(Key) || Key <- Keys],
    Ops = [{map, {modfun, riak_kv_mapreduce, map_object_value}, undefined, true}],
    case riakc_pb_socket:mapred(Conn, Keys, Ops) of
        {ok, []} ->
            [];
        {ok, [{0, Values}]} ->
            Values
    end.

assert_valid_key({Bucket, Key}) when is_binary(Bucket), is_binary(Key) ->
    ok.

%% @doc Count entries (bounds are inclusive).
%% TODO: use map-reduce here
get_entry_count_between(Conn, SecIndex, LBound, UBound) ->
    {ok, ?INDEX_RESULTS{keys=Keys}} =
    riakc_pb_socket:get_index_range(Conn, message_bucket(), SecIndex, LBound, UBound),
    length(Keys).

get_hour_entry_count_after(Conn, MessIdxKeyMaker, SecIndex, Start) ->
    LBound = MessIdxKeyMaker({lower, Start}),
    UBound = MessIdxKeyMaker({upper, hour_upper_bound(hour(Start))}),
    get_entry_count_between(Conn, SecIndex, LBound, UBound).

get_key_range(Conn, SecIndex, LBound, UBound)
    when is_binary(LBound), is_binary(UBound) ->
    {ok, ?INDEX_RESULTS{keys=Keys}} =
    riakc_pb_socket:get_index_range(Conn, message_bucket(), SecIndex, LBound, UBound),
    Keys = [K || K <- Keys, LBound =< K, K =< UBound],
    Keys.

get_minimum_key_range_before(Conn, MessIdxKeyMaker, SecIndex, Before, PageSize, Digest) ->
    {_, MinDigest} = dig_skip_nr(PageSize, Digest),
    %% Expected start hour of the page
    ExpectedFirstHour = case dig_is_empty(MinDigest) of
        true -> dig_first_hour(Digest);
        false -> dig_last_hour(MinDigest)
    end,
    LBound = MessIdxKeyMaker({lower, hour_lower_bound(ExpectedFirstHour)}),
    UBound = MessIdxKeyMaker({upper, Before}),
    get_key_range(Conn, SecIndex, LBound, UBound).
    

-spec get_message_rows(term(), [binary()]) -> list(message_row()).
get_message_rows(Conn, Keys) ->
    FullKeys = [{message_bucket(), K} || K <- Keys],
    Values = to_values(Conn, FullKeys),
    MessageRows = deserialize_values(Values),
    lists:usort(MessageRows).

fetch_all_keys(Conn, SecIndex, MessIdxKeyMaker) ->
    LBound = MessIdxKeyMaker({lower, undefined}),
    UBound = MessIdxKeyMaker({upper, undefined}),
    get_key_range(Conn, SecIndex, LBound, UBound).


-ifdef(TEST).

meck_test_() ->
    [{"Without index digest.",
      {setup, 
       fun() -> load_mock(100), load_data() end,
       fun(_) -> unload_mock() end,
       {timeout, 60, {spawn, {generator, fun long_case/0 }}}}},
     {"With index digest.",
      {setup, 
       fun() -> load_mock(0), load_data() end,
       fun(_) -> unload_mock() end,
       {timeout, 60, {spawn, {generator, fun long_case/0}}}}},
     {"Without index digest.",
      {setup, 
       fun() -> load_mock(100), load_data2() end,
       fun(_) -> unload_mock() end,
       {timeout, 60, {spawn, {generator, fun short_case/0}}}}},
     {"With index digest.",
      {setup, 
       fun() -> load_mock(0), load_data2() end,
       fun(_) -> unload_mock() end,
       {timeout, 60, {spawn, {generator, fun short_case/0}}}}}
    ].

load_mock(DigestThreshold) ->
    GM = gen_mod,
    meck:new(GM),
    meck:expect(GM, get_module_opt, fun(_, mod_mam, digest_creation_threshold, _) ->
        DigestThreshold
        end),
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
        Md     = proplists:get_value(metadata, Obj, []),
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
        Md = [{<<"X-Riak-Last-Modified">>, now()}],
        [{bucket, Bucket}, {key, Key}, {value, Value}, {metadata, Md}]
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
    PM = riak_pool,
    meck:new(PM),
    meck:expect(PM, with_connection, fun(mam_cluster, F) ->
            F(conn)
        end),
    CM = mod_mam_cache,
    meck:new(CM),
    meck:expect(CM, user_id, fun(LServer, LUser) ->
            user_id(LUser)
        end),
    ok.

unload_mock() ->
    meck:unload(gen_mod),
    meck:unload(riakc_pb_socket),
    meck:unload(riakc_obj),
    meck:unload(riak_pool),
    meck:unload(mod_mam_cache),
    ok.

load_data() ->
    Log1Id = fun(Time) ->
        Date = iolist_to_binary("2000-07-21T" ++ Time ++ "Z"),
        id(Date)
        end,
    Log2Id = fun(Time) ->
        Date = iolist_to_binary("2000-07-22T" ++ Time ++ "Z"),
        id(Date)
        end,
    %% Alice to Cat
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
        Packet = message(iolist_to_binary(Text)),
        ?MODULE:archive_message(Log2Id(Time), outgoing, alice(), party(), alice(),
                                Packet)
        end,
    %% March Hare - M, Hatter - H and Dormouse - D
    PM2A = fun(Time, Text) ->
        Packet = message(iolist_to_binary(Text)),
        ?MODULE:archive_message(Log2Id(Time), incoming, alice(),
                                march_hare_at_party(), march_hare_at_party(),
                                Packet)
        end,
    PH2A = fun(Time, Text) ->
        Packet = message(iolist_to_binary(Text)),
        ?MODULE:archive_message(Log2Id(Time), incoming, alice(),
                                hatter_at_party(), hatter_at_party(),
                                Packet)
        end,
    PD2A = fun(Time, Text) ->
        Packet = message(iolist_to_binary(Text)),
        ?MODULE:archive_message(Log2Id(Time), incoming, alice(),
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
    ok.

long_case() ->
    %% lookup_messages(UserJID, RSM, Start, End, WithJID,
    %%                 PageSize, LimitPassed, MaxResultLimit) ->
    %% {ok, {TotalCount, Offset, MessageRows}}
    
    [
    {"First 5.",
    assert_keys(34, 0,
                [join_date_time("2000-07-21", Time)
                 || Time <- ["01:50:00", "01:50:05", "01:50:15",
                             "01:50:16", "01:50:17"]],
                lookup_messages(alice(),
                    none, undefined, undefined, undefined,
                    5, true, 5))},

    {"Last 5.",
    assert_keys(34, 29,
                [join_date_time("2000-07-22", Time)
                 || Time <- ["06:50:40", "06:50:50", "06:51:00",
                             "06:51:10", "06:51:15"]],
                lookup_messages(alice(),
                    #rsm_in{direction = before},
                    undefined, undefined, undefined,
                    5, true, 5))},

    {"Last 5 with a lower bound.",
    %% lower_bounded
    assert_keys(12, 7,
                [join_date_time("2000-07-22", Time)
                 || Time <- ["06:50:40", "06:50:50", "06:51:00",
                             "06:51:10", "06:51:15"]],
                lookup_messages(alice(),
                    #rsm_in{direction = before},
                    to_microseconds("2000-07-22", "06:49:45"),
                    undefined, undefined,
                    5, true, 5))},

    {"Last 5 from both conversations with a lower bound.",
    %% lower_bounded
    assert_keys(24, 19,
                [join_date_time("2000-07-22", Time)
                 || Time <- ["06:50:40", "06:50:50", "06:51:00",
                             "06:51:10", "06:51:15"]],
                lookup_messages(alice(),
                    #rsm_in{direction = before},
                    to_microseconds("2000-07-21", "01:50:50"),
                    undefined, undefined,
                    5, true, 5))},

    %% upper_bounded
    {"Last 5 from both conversations with an upper bound.",
    %% lower_bounded
    assert_keys(28, 23,
                [join_date_time("2000-07-22", Time)
                 || Time <- ["06:49:50", "06:50:05", "06:50:10",
                             "06:50:25", "06:50:28"]],
                lookup_messages(alice(),
                    #rsm_in{direction = before}, undefined,
                    to_microseconds("2000-07-22", "06:50:29"), undefined,
                    5, true, 5))},

    {"Index 3.",
    assert_keys(34, 3,
                [join_date_time("2000-07-21", Time)
                 || Time <- ["01:50:16", "01:50:17", "01:50:20",
                             "01:50:25", "01:50:30"]],
                lookup_messages(alice(),
                    #rsm_in{index = 3},
                    undefined, undefined, undefined,
                    5, true, 5))},

    {"Last 5 from the range.",
    %% bounded
    assert_keys(6, 1,
                [join_date_time("2000-07-22", Time)

                 || Time <- ["06:49:50", "06:50:05", "06:50:10",
                             "06:50:25", "06:50:28"]],
                lookup_messages(alice(),
                    #rsm_in{direction = before},
                    to_microseconds("2000-07-22", "06:49:45"),
                    to_microseconds("2000-07-22", "06:50:28"), undefined,
                    5, true, 5))}].

%% Short dialogs
load_data2() ->
    Log1Id = fun(Time) ->
        Date = iolist_to_binary("2000-07-21T" ++ Time ++ "Z"),
        id(Date)
        end,
    Log2Id = fun(Time) ->
        Date = iolist_to_binary("2000-07-22T" ++ Time ++ "Z"),
        id(Date)
        end,
    Log3Id = fun(Time) ->
        Date = iolist_to_binary("2000-07-23T" ++ Time ++ "Z"),
        id(Date)
        end,
    A2H = fun(Time, Text) ->
        Packet = message(iolist_to_binary(Text)),
        ?MODULE:archive_message(Log1Id(Time), outgoing, alice(), hatter(), alice(),
                                Packet)
        end,
    H2A = fun(Time, Text) ->
        Packet = message(iolist_to_binary(Text)),
        ?MODULE:archive_message(Log1Id(Time), incoming, alice(), hatter(), hatter(),
                                Packet)
        end,
    A2D = fun(Time, Text) ->
        Packet = message(iolist_to_binary(Text)),
        ?MODULE:archive_message(Log2Id(Time), outgoing, alice(), duchess(), alice(),
                                Packet)
        end,
    D2A = fun(Time, Text) ->
        Packet = message(iolist_to_binary(Text)),
        ?MODULE:archive_message(Log2Id(Time), incoming, alice(), duchess(), duchess(),
                                Packet)
        end,
    A2M = fun(Time, Text) ->
        Packet = message(iolist_to_binary(Text)),
        ?MODULE:archive_message(Log3Id(Time), outgoing, alice(), mouse(), alice(),
                                Packet)
        end,
    M2A = fun(Time, Text) ->
        Packet = message(iolist_to_binary(Text)),
        ?MODULE:archive_message(Log3Id(Time), incoming, alice(), mouse(), mouse(),
                                Packet)
        end,
    
    %% Credits to http://quotations.about.com/od/moretypes/a/alice4.htm
    % Alice and The Hatter from Alice in the Wonderland
    Log1 = fun(A, H) ->
        A("13:10:03", "What a funny watch! It tells the day of the month, "
                      "and it doesn't tell what o'clock it is!"),
        H("13:10:10", "Why should it? Does your watch tell you what year it is?"),
        A("13:10:15", "Of course not, but that's because it stays the same year"
                      " for such a long time together."),
        H("13:10:16", "…which is just the case with mine."),
        ok
        end,

% Alice and the Duchess from Alice in Wonderland
    Log2 = fun(A, D) ->
        A("15:59:59", "I didn't know that Cheshire cats always grinned; "
                      "in fact, I didn't know that cats could grin."),
        D("16:00:05", "You don't know much; and that's a fact."),
        ok
        end,

% Alice and the Dormouse from Alice in Wonderland
    Log3 = fun(A, M) ->
        M("10:34:04", "You've got no right to grow here."),
        A("10:34:20", "Don't talk nonsense. You know you're growing too."),
        M("10:34:23", "Yes, but I grow at a reasonable pace, "
                      "not in that ridiculous fashion."),
        ok
        end,
    Log1(A2H, H2A),
    Log2(A2D, D2A),
    Log3(A2M, M2A),
    ok.


short_case() ->
    [{"Last 2 from the range.",
    %% bounded
    assert_keys(2, 0,
                [join_date_time("2000-07-22", Time)

                 || Time <- ["15:59:59", "16:00:05"]],
                lookup_messages(alice(),
                    #rsm_in{direction = before},
                    to_microseconds("2000-07-21", "13:30:00"),
                    to_microseconds("2000-07-23", "10:30:00"), undefined,
                    2, true, 2))}].


to_microseconds(Date, Time) ->
    %% Reuse the library function.
    assert_defined(mod_mam_utils:maybe_microseconds(
        list_to_binary(join_date_time_z(Date, Time)))).
    
assert_defined(X) when X =/= undefined -> X.

assert_keys(ExpectedTotalCount, ExpectedOffset, DateTimes,
            {ok, {TotalCount, Offset, MessageRows}}) ->
    [?_assertEqual(ExpectedTotalCount, TotalCount),
     ?_assertEqual(ExpectedOffset, Offset),
     ?_assertEqual(DateTimes,
        [mess_id_to_iso_date_time(MessID) || {MessID, _, _} <- MessageRows])].

join_date_time(Date, Time) ->
    Date ++ "T" ++ Time.

join_date_time_z(Date, Time) ->
    Date ++ "T" ++ Time ++ "Z".

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

hatter() ->
    #jid{luser = <<"hatter">>, lserver = <<"wonderland">>, lresource = <<>>,
          user = <<"hatter">>,  server = <<"wonderland">>,  resource = <<>>}.

duchess() ->
    #jid{luser = <<"duchess">>, lserver = <<"wonderland">>, lresource = <<>>,
          user = <<"duchess">>,  server = <<"wonderland">>,  resource = <<>>}.

mouse() ->
    #jid{luser = <<"dormouse">>, lserver = <<"wonderland">>, lresource = <<>>,
          user = <<"dormouse">>,  server = <<"wonderland">>,  resource = <<>>}.

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

sublist_r_test_() ->
    [?_assertEqual([4,5], sublist_r([1,2,3,4,5], 2)),
     ?_assertEqual([1,2,3,4,5], sublist_r([1,2,3,4,5], 5))].

dig_before_hour_test_() ->
    [?_assertEqual([{2, 10}, {3, 15}], dig_before_hour(5, [{2, 10}, {3, 15}])),
     ?_assertEqual([], dig_before_hour(5, [{5, 10}])),
     ?_assertEqual([], dig_before_hour(5, [{6, 10}]))
    ].

dig_position_test_() ->
    Digest = [{3, 25}, {6, 15}],
    
    [?_assertEqual(before, dig_position(2, Digest)),
     ?_assertEqual(inside, dig_position(3, Digest)),
     ?_assertEqual(inside, dig_position(5, Digest)),
     ?_assertEqual(inside, dig_position(6, Digest)),
     ?_assertEqual('after', dig_position(8, Digest))].

frequency_test_() ->
    [?_assertEqual([{a, 2}, {b, 3}], frequency([a, a, b, b, b]))].

-endif.

is_short_range(Now, Start, End) ->
    case calc_range(Now, Start, End) of
        undefined -> false;
        Range -> Range < 2
    end.

calc_range(_,   undefined, _)     -> undefined;
calc_range(Now, Start, undefined) -> Now - Start; 
calc_range(_,   Start, End)       -> End - Start.

%% Returns N elements from behind.
sublist_r(Keys, N) ->
    lists:sublist(Keys, max(0, length(Keys) - N) + 1, N).


