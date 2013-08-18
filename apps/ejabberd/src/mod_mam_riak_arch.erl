-module(mod_mam_riak_arch).
-export([start/1,
         archive_message/6,
         wait_flushing/1,
         archive_size/2,
         lookup_messages/9,
         remove_user_from_db/2,
         purge_single_message/3,
         purge_multiple_messages/5]).

%% UID
-import(mod_mam_utils,
        [encode_compact_uuid/2]).

-type message_row() :: tuple(). % {MessID, SrcJID, Packet}

-ifdef(TEST).
-export([load_mock/1,
         unload_mock/0,
         reset_mock/0,
         set_now/1]).

%% Time
-import(mod_mam_utils,
        [datetime_to_microseconds/1]).

%% Lazy calculation
-define(_assertKeys(TotalCount, Offset, Keys, Result),
        {generator,
         fun() -> assert_keys((TotalCount), (Offset), (Keys), (Result)) end}).

-include_lib("eunit/include/eunit.hrl").
-endif.

-include_lib("ejabberd/include/ejabberd.hrl").
-include_lib("ejabberd/include/jlib.hrl").
-include_lib("exml/include/exml.hrl").
-include_lib("riakc/include/riakc.hrl").

-type message_id() :: non_neg_integer().
-type maybe_message_id() :: message_id() | undefined.

-record(r_query, {
        connection, 
        l_bound :: maybe_message_id(),
        u_bound :: maybe_message_id(),
        b_user_id,
        b_with_jid,
        sec_index
}).

make_query(Conn, BUserID, BWithJID, WithJID, LBound, UBound) ->
    SecIndex = choose_index(WithJID),
    #r_query{
        connection = Conn,
        b_user_id = BUserID,
        b_with_jid = BWithJID,
        l_bound = LBound,
        u_bound = UBound,
        sec_index = SecIndex
    }.

make_query(Conn, DigestObj) ->
    DigestKey = riakc_obj:key(DigestObj),
    {BUserID, BWithJID} = decode_digest_key(DigestKey),
    SecIndex = choose_index_bin(BWithJID),
    #r_query{
        connection = Conn,
        b_user_id = BUserID,
        b_with_jid = BWithJID,
        sec_index = SecIndex
    }.

set_bounds(LBound, UBound, RQ=#r_query{}) ->
    RQ#r_query{
        l_bound = LBound,
        u_bound = UBound
    }.

request_keys(RQ=#r_query{}) ->
    #r_query{
        connection = Conn,
        b_user_id = BUserID,
        b_with_jid = BWithJID,
        l_bound = LBound,
        u_bound = UBound,
        sec_index = SecIndex
    } = RQ,
    LBoundKey = maybe_mess_id_to_lower_bound_binary(LBound),
    UBoundKey = maybe_mess_id_to_upper_bound_binary(UBound),
    LBoundSecKey = secondary_key(BUserID, LBoundKey, BWithJID),
    UBoundSecKey = secondary_key(BUserID, UBoundKey, BWithJID),
    get_key_range(Conn, SecIndex, LBoundSecKey, UBoundSecKey).


request_key_count(RQ=#r_query{}) ->
    #r_query{
        connection = Conn,
        b_user_id = BUserID,
        b_with_jid = BWithJID,
        l_bound = LBound,
        u_bound = UBound,
        sec_index = SecIndex
    } = RQ,
    LBoundKey = maybe_mess_id_to_lower_bound_binary(LBound),
    UBoundKey = maybe_mess_id_to_upper_bound_binary(UBound),
    LBoundSecKey = secondary_key(BUserID, LBoundKey, BWithJID),
    UBoundSecKey = secondary_key(BUserID, UBoundKey, BWithJID),
    get_key_count_between(Conn, SecIndex, LBoundSecKey, UBoundSecKey).

message_bucket() ->
    <<"mam_m">>.

digest_bucket() ->
    <<"mam_d">>.

user_id_bare_remote_jid_index() ->
    {binary_index, "b"}.

user_id_full_remote_jid_index() ->
    {binary_index, "f"}.

key_index() ->
    <<"$key">>.

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
            update_bucket(Conn, digest_bucket(), [{allow_mult, true}])
        end,
    with_connection(Host, F).

lww_props() ->
    [{allow_mult, false}, {last_write_wins, true}].


archive_size(LServer, LUser) ->
    Now = mod_mam_utils:now_to_microseconds(now()),
    {ok, {TotalCount, 0, []}} = lookup_messages(
            jlib:make_jid(LUser, LServer, <<>>),
            undefined, undefined, undefined, Now, undefined, 0, true, 0),
    TotalCount.

%% Row is `{<<"13663125233">>,<<"bob@localhost">>,<<"res1">>,<<binary>>}'.
%% `#rsm_in{direction = before | aft, index=int(), id = binary()}'
%% `#rsm_in.id' is not inclusive.
%% `#rsm_in.index' is zero-based offset.
%% `Start', `End' and `WithJID' are filters, they are applied BEFORE paging.
paginate(Keys, undefined, PageSize) ->
    {0, lists:sublist(Keys, PageSize)};
%% Skip first KeysBeforeCnt keys.
paginate(Keys, #rsm_in{direction = undefined, index = KeysBeforeCnt}, PageSize)
    when is_integer(KeysBeforeCnt), KeysBeforeCnt >= 0 ->
    {KeysBeforeCnt, safe_sublist(Keys, KeysBeforeCnt + 1, PageSize)};
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
    paginate(Keys, undefined, PageSize).

-ifdef(TEST).

sublist_r_test_() ->
    [?_assertEqual([], sublist_r([], 1)),
     ?_assertEqual([1], sublist_r([1], 1)),
     ?_assertEqual([4,5], sublist_r([1,2,3,4,5], 2)),
     ?_assertEqual([1,2,3], sublist_r([1,2,3], 5)),
     ?_assertEqual([1,2,3,4,5], sublist_r([1,2,3,4,5], 5))
    ].

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


page_minimum_hour_test_() ->
    [?_assertEqual(3, page_minimum_hour( 5, [{1, 6}, {2, 5}, {3, 8}])),
     ?_assertEqual(2, page_minimum_hour(10, [{1, 6}, {2, 5}, {3, 8}])),
     ?_assertEqual(1, page_minimum_hour(20, [{1, 6}, {2, 5}, {3, 8}]))
    ].

safe_sublist_test_() ->
    [?_assertEqual([1,2,3], safe_sublist([1,2,3], 1, 10))
    ,?_assertEqual([2,3],   safe_sublist([1,2,3], 2, 10))
    ,?_assertEqual([3],     safe_sublist([1,2,3], 3, 10))
    ,?_assertEqual([],      safe_sublist([1,2,3], 4, 10))
    ,?_assertEqual([],      safe_sublist([], 1, 1))
    ,?_assertEqual([],      safe_sublist([], 1, 2))
    ].

dig_decrease_test_() ->
    [?_assertEqual([{262969,1}], dig_decrease([{262969,1},{262970,1}], 262970, 1))
    ].

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
     {"Index 0, TotalCount 2, PageSize 5.",
      ?_assertEqual({0, [1,2]},
                    paginate([1,2],
                             #rsm_in{index = 0}, 5))},
     {"Index 1, TotalCount 2, PageSize 5.",
      ?_assertEqual({1, [2]},
                    paginate([1,2],
                             #rsm_in{index = 1}, 5))},
     {"Last Page",
      ?_assertEqual({6, [7,8,9]},
                    paginate([1,2,3, 4,5,6, 7,8,9],
                             #rsm_in{direction = before}, 3))},
     {"Index 6, TotalCount 4.",
      ?_assertEqual({6, []},
                    paginate([1,2,3,4],
                             #rsm_in{index = 6}, 4))}
    ].

-endif.

fix_rsm(_BUserID, undefined) ->
    undefined;
fix_rsm(_BUserID, RSM=#rsm_in{id = undefined}) ->
    RSM;
fix_rsm(BUserID, RSM=#rsm_in{id = MessID}) when is_integer(MessID) ->
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


-spec lookup_messages(UserJID, RSM, Start, End, Now, WithJID, PageSize,
                      LimitPassed, MaxResultLimit) ->
    {ok, {TotalCount, Offset, MessageRows}} | {error, 'policy-violation'}
			     when
    UserJID :: #jid{},
    RSM     :: #rsm_in{} | undefined,
    Start   :: unix_timestamp() | undefined,
    End     :: unix_timestamp() | undefined,
    Now     :: unix_timestamp(),
    PageSize :: non_neg_integer(),
    WithJID :: #jid{} | undefined,
    LimitPassed :: boolean(),
    MaxResultLimit :: non_neg_integer(),
    TotalCount :: non_neg_integer(),
    Offset  :: non_neg_integer(),
    MessageRows :: list(tuple()).
lookup_messages(_UserJID=#jid{lserver=LocLServer, luser=LocLUser},
                RSM, Start, End, Now, WithJID,
                PageSize, LimitPassed, MaxResultLimit) when is_integer(Now) ->
    UserID = mod_mam_cache:user_id(LocLServer, LocLUser),
    BUserID = user_id_to_binary(UserID),
    BWithJID = maybe_jid_to_opt_binary(LocLServer, WithJID),
    DigestKey = digest_key(BUserID, BWithJID),
    RSM1 = fix_rsm(BUserID, RSM),
    F = fun(Conn) ->
        LBound = maybe_microseconds_to_min_mess_id(Start),
        UBound = maybe_microseconds_to_max_mess_id(End),
        RQ = make_query(Conn, BUserID, BWithJID, WithJID, LBound, UBound),
        check_result_and_return(Conn, MaxResultLimit, LimitPassed,
        case is_short_range(Now, Start, End) of
        true ->
            %% ignore index digest.
            query_all(RQ, RSM1, PageSize);
        false ->
            case get_actual_digest(Conn, RQ, DigestKey, Now) of
            {error, notfound} ->
                Result = query_all(RQ, RSM1, PageSize),
                should_create_digest(LocLServer, Result)
                andalso
                create_non_empty_digest(Conn, DigestKey,
                    get_old_keys(RQ, Now)),
                Result;
            {ok, Digest} ->
                analyse_digest(RQ, RSM1, Start, End, PageSize, Digest)
            end
        end)
        end,
    with_connection(LocLServer, F).


return_matched_keys(TotalCount, Offset, MatchedKeys) ->
    {ok, {TotalCount, Offset, MatchedKeys}}.

check_result_and_return(Conn, MaxResultLimit, LimitPassed,
                        {ok, {TotalCount, Offset, MatchedKeys}}) ->
    check_and_return(
        Conn, MaxResultLimit, LimitPassed, MatchedKeys, TotalCount, Offset);
check_result_and_return(_, _, _, Result) ->
    Result.

check_and_return(
    Conn, MaxResultLimit, LimitPassed, MatchedKeys, TotalCount, Offset) ->
    case is_policy_violation(TotalCount, Offset, MaxResultLimit, LimitPassed) of
    true -> return_policy_violation();
    false ->
        MessageRows = get_message_rows(Conn, MatchedKeys),
        {ok, {TotalCount, Offset, MessageRows}}
    end.

%% If a query returns a number of stanzas greater than this limit and the
%% client did not specify a limit using RSM then the server should return
%% a policy-violation error to the client.
is_policy_violation(TotalCount, Offset, MaxResultLimit, LimitPassed) ->
    TotalCount - Offset > MaxResultLimit andalso not LimitPassed.

return_policy_violation() ->
    {error, 'policy-violation'}.


analyse_digest(RQ, RSM, Start, End, PageSize, Digest) ->
    case RSM of
        #rsm_in{index = Offset} when is_integer(Offset), Offset >= 0 ->
            %% If `Start' is not recorded, that requested result
            %% set contains new messages only
            case is_defined(Start) andalso not dig_is_recorded(hour(Start), Digest) of
                %% `Start' is after the digist.
                %% Match only recent messages.
                true -> query_all(RQ, RSM, PageSize);
                false ->
                    analyse_digest_index(RQ, RSM,
                        Offset, Start, End, PageSize, Digest)
            end;
        %% Last page.
        #rsm_in{direction = before, id = undefined} ->
            analyse_digest_last_page(RQ, RSM, Start, End, PageSize, Digest);
        #rsm_in{direction = before, id = Id} ->
            analyse_digest_before(RQ, RSM, Start, End, PageSize, Id, Digest);
        #rsm_in{direction = aft, id = Id} ->
            analyse_digest_after(RQ, RSM, Start, End, PageSize, Id, Digest);
        _ ->
            query_all(RQ, RSM, PageSize)
    end.

analyse_digest_index(RQ, RSM, Offset, Start, End, PageSize, Digest) ->
    Strategy = choose_strategy(Start, End, Digest),
    case Strategy of
    empty -> return_empty();
    recent_only -> query_all(RQ, RSM, PageSize);
    whole_digest_and_recent ->
        DigestCnt = dig_total(Digest),
        Strategy2 =
        %% When Offset == DigestCnt, PageDigest is empty.
        case {PageSize, Offset >= DigestCnt, Offset + PageSize >= DigestCnt} of
        %% Index is too high. Get recent keys.
        {0, _, _} -> index_all_count_only;
        {_, true, _} -> nothing_from_digest;
        {_, false, true} -> part_from_digest;
        {_, false, false} -> only_from_digest
        end,
        case Strategy2 of
        index_all_count_only ->
            RecentCnt = get_recent_key_count(RQ, Digest, End),
            TotalCount = dig_total(Digest) + RecentCnt,
            return_matched_keys(TotalCount, Offset, []);
        nothing_from_digest ->
            %% Left to skip
            LeftOffset = Offset - DigestCnt,
            Keys = get_recent_keys(RQ, Digest, End),
            MatchedKeys = safe_sublist(Keys, LeftOffset+1, PageSize),
            TotalCount = DigestCnt + length(Keys),
            return_matched_keys(TotalCount, Offset, MatchedKeys);
        part_from_digest ->
            %% Head of  `PageDigest' contains last entries of the previous
            %% page and starting messages of the requsted page.
            %% `FirstHourOffset' is how many entries to skip in `PageDigest'.
            {FirstHourOffset, PageDigest} = dig_skip_n(Offset, Digest),
            LHour = dig_first_hour(PageDigest),
            LBound = hour_to_min_mess_id(LHour),
            UBound = maybe_microseconds_to_max_mess_id(End),
            Keys = request_keys(set_bounds(LBound, UBound, RQ)),
            MatchedKeys = safe_sublist(Keys, FirstHourOffset+1, PageSize),
            TotalCount = Offset + length(Keys) - FirstHourOffset,
            return_matched_keys(TotalCount, Offset, MatchedKeys);
        only_from_digest ->
            %% Head of  `PageDigest' contains last entries of the previous
            %% page and starting messages of the requsted page.
            %% `FirstHourOffset' is how many entries to skip in `PageDigest'.
            {FirstHourOffset, PageDigest} = dig_skip_n(Offset, Digest),
            {LastHourOffset, LeftDigest} = dig_skip_n(
                FirstHourOffset + PageSize, PageDigest),
            %% Calc minimum and maximum hours of the page
            LHour = dig_first_hour(PageDigest),
            UHour = case LastHourOffset of
                0 -> dig_first_hour(LeftDigest) - 1; %% no need for this hour
                _ -> dig_first_hour(LeftDigest) %% get this hour
                end,
            LBound = hour_to_min_mess_id(LHour),
            UBound = hour_to_max_mess_id(UHour),
            Keys = request_keys(set_bounds(LBound, UBound, RQ)),
            MatchedKeys = safe_sublist(Keys, FirstHourOffset+1, PageSize),
            RecentCnt = get_recent_key_count(RQ, Digest, End),
            TotalCount = DigestCnt + RecentCnt,
            return_matched_keys(TotalCount, Offset, MatchedKeys)
        end;
    lower_bounded ->
        %% RSet includes a start hour
        RSetDigest = dig_from_hour(hour(Start), Digest),
        RSetDigestCnt = dig_total(RSetDigest),
        StartHourOffset = get_hour_key_count_before(RQ, Digest, Start),
        RSetOffset = StartHourOffset + Offset,
        LBoundPos = dig_nth_pos(RSetOffset, RSetDigest),
        UBoundPos = dig_nth_pos(RSetOffset + PageSize, RSetDigest),
        LHour = case LBoundPos of
            inside  -> dig_nth(RSetOffset, RSetDigest);
            'after' -> dig_last_hour(RSetDigest)
            end,
        LBound = hour_to_min_mess_id(LHour),
        UBound = case UBoundPos of
            inside ->
                UHour = dig_nth(RSetOffset + PageSize, RSetDigest),
                hour_to_max_mess_id(UHour);
            'after' -> undefined
            end,
        Keys = request_keys(set_bounds(LBound, UBound, RQ)),
        %% How many entries in `RSetDigest' before `Keys'.
        SkippedCnt = dig_total(dig_before_hour(LHour, RSetDigest)),
        %% How many unwanted keys were extracted.
        PageOffset = RSetOffset - SkippedCnt,
        MatchedKeys = safe_sublist(Keys, PageOffset+1, PageSize),
        RecentCnt = case UBoundPos of
            inside ->  get_recent_key_count(RQ, Digest, End);
            'after' -> filter_and_count_keys_before_digest(Keys, Digest)
            end,
        TotalCount = RSetDigestCnt - StartHourOffset + RecentCnt,
        return_matched_keys(TotalCount, Offset, MatchedKeys);
    upper_bounded ->
        %% End hour included.
        RSetDigest = dig_to_hour(hour(End), Digest),
        RSetDigestCnt = dig_total(RSetDigest),
        Strategy2 =
        case Offset + PageSize > RSetDigestCnt of
        true -> count_only;
        false ->
            UHour1 = dig_nth(Offset + PageSize, RSetDigest),
            case End < hour_to_max_microseconds(UHour1) of
            true -> index_upper_bounded_last_page;
            false -> middle_page
            end
        end,
        case Strategy2 of
        count_only ->
            AfterHourCnt = get_hour_key_count_after(RQ, Digest, End),
            TotalCount = RSetDigestCnt - AfterHourCnt,
            return_matched_keys(TotalCount, Offset, []);
        middle_page ->
            LHour = dig_nth(Offset, RSetDigest),
            UHour = dig_nth(Offset + PageSize, RSetDigest),
            LBound = hour_to_min_mess_id(LHour),
            UBound = hour_to_max_mess_id(UHour),
            Keys = request_keys(set_bounds(LBound, UBound, RQ)),
            PageOffset = dig_total(dig_before_hour(LHour, RSetDigest)),
            MatchedKeys = safe_sublist(Keys, PageOffset+1, PageSize),
            AfterHourCnt = get_hour_key_count_after(RQ, Digest, End),
            TotalCount = RSetDigestCnt - AfterHourCnt,
            return_matched_keys(TotalCount, Offset, MatchedKeys);
        index_upper_bounded_last_page ->
            %% Get entries only from digest before `End'.
            LHour = dig_nth(Offset, RSetDigest),
            LBound = hour_to_min_mess_id(LHour),
            UBound = microseconds_to_max_mess_id(End),
            Keys = request_keys(set_bounds(LBound, UBound, RQ)),
            BeforeCnt = dig_total(dig_before_hour(LHour, RSetDigest)),
            PageOffset = Offset - BeforeCnt,
            MatchedKeys = safe_sublist(Keys, PageOffset+1, PageSize),
            EndHourCnt = filter_and_count_hour_keys(hour(End), Keys),
            AfterHourCnt = dig_calc_volume(hour(End), RSetDigest) - EndHourCnt,
            TotalCount = RSetDigestCnt - AfterHourCnt,
            return_matched_keys(TotalCount, Offset, MatchedKeys)
        end;
    bounded ->
        %% `Start' and `End' hours are included.
        RSetDigest = dig_to_hour(hour(End), dig_from_hour(hour(Start), Digest)),
        %% Expected size of the Result Set in best-cast scenario (maximim)
        RSetDigestCnt = dig_total(RSetDigest),
        HeadCnt = dig_first_count(RSetDigest),
        LastCnt = dig_last_count(RSetDigest),
        MinRSetDigestCnt = RSetDigestCnt - LastCnt - HeadCnt,
        Strategy2 =
        case {Offset + PageSize > RSetDigestCnt,
              Offset < HeadCnt,
              Offset + PageSize > MinRSetDigestCnt} of
        {true, _, _} -> bounded_count_only;
        {false, true, true} -> bounded_query_all;
        {false, true, false} -> bounded_first_page;
        {false, false, true} -> bounded_last_page;
        {false, false, false} -> bounded_middle_page
        end,
        case Strategy2 of
        bounded_query_all -> query_all(RQ, RSM, PageSize);
        bounded_count_only ->
            StartHourOffset = get_hour_key_count_before(RQ, Digest, Start),
            AfterHourCnt = get_hour_key_count_after(RQ, Digest, End),
            TotalCount = RSetDigestCnt - StartHourOffset - AfterHourCnt,
            return_matched_keys(TotalCount, Offset, []);
        bounded_first_page ->
            UHour = dig_nth(HeadCnt + Offset + PageSize, RSetDigest),
            LBound = microseconds_to_min_mess_id(Start),
            UBound = hour_to_max_mess_id(UHour),
            Keys = request_keys(set_bounds(LBound, UBound, RQ)),
            MatchedKeys = safe_sublist(Keys, Offset+1, PageSize),
            StartHourCnt = filter_and_count_hour_keys(hour(Start), Keys),
            AfterHourCnt = get_hour_key_count_after(RQ, Digest, End),
            StartHourOffset = HeadCnt - StartHourCnt,
            TotalCount = RSetDigestCnt - StartHourOffset - AfterHourCnt,
            return_matched_keys(TotalCount, Offset, MatchedKeys);
        bounded_last_page ->
            StartHourOffset = get_hour_key_count_before(RQ, Digest, Start),
            RSetOffset = HeadCnt + Offset - StartHourOffset,
            RSetDigestCnt = dig_total(RSetDigest),
            case RSetOffset >= RSetDigestCnt of
            true -> 
%               true = hour(Start) =/= hour(End),
                AfterHourCnt = get_hour_key_count_after(RQ, Digest, End),
                TotalCount = RSetDigestCnt - StartHourOffset - AfterHourCnt,
                {ok, {TotalCount, Offset, []}};
            false ->
                LHour = dig_nth(RSetOffset, RSetDigest),
                LBound = hour_to_min_mess_id(LHour),
                UBound = microseconds_to_max_mess_id(End),
                Keys = request_keys(set_bounds(LBound, UBound, RQ)),
                PageOffset = RSetOffset + dig_total(dig_before_hour(LHour, RSetDigest)),
                MatchedKeys = safe_sublist(Keys, PageOffset+1, PageSize),
                LastHourCnt = filter_and_count_hour_keys(hour(End), Keys),
                %% expected - real
                AfterHourCnt = LastCnt - LastHourCnt,
                TotalCount = RSetDigestCnt - StartHourOffset - AfterHourCnt,
                return_matched_keys(TotalCount, Offset, MatchedKeys)
            end;
        bounded_middle_page ->
            AfterHourCnt = get_hour_key_count_after(RQ, Digest, End),
            StartHourOffset = get_hour_key_count_before(RQ, Digest, Start),
            TotalCount = RSetDigestCnt - StartHourOffset - AfterHourCnt,
            RSetOffset = HeadCnt + Offset - StartHourOffset,
            LHour = dig_nth(RSetOffset, RSetDigest),
            UHour = dig_nth(RSetOffset + PageSize, RSetDigest),
            LBound = hour_to_min_mess_id(LHour),
            UBound = hour_to_max_mess_id(UHour),
            Keys = request_keys(set_bounds(LBound, UBound, RQ)),
            PageOffset = RSetOffset + dig_total(dig_before_hour(LHour, RSetDigest)),
            MatchedKeys = safe_sublist(Keys, PageOffset+1, PageSize),
            return_matched_keys(TotalCount, Offset, MatchedKeys)
        end
    end.


%% Is the key `Key' before, `after' or inside of the result set?
key_position(Key, Start, End, Digest) when is_binary(Key) ->
    MessID = key_to_mess_id(Key),
    StartMessID = maybe_microseconds_to_min_mess_id(Start),
    EndMessID   = maybe_microseconds_to_max_mess_id(End),
    MinMessID   = maybe_dig_minimum_mess_id(Digest),
    MaxMessID   = maybe_dig_maximum_mess_id(Digest),
    LBoundMessID = maybe_max(MinMessID, StartMessID),
    UBoundMessID = maybe_min(MaxMessID, EndMessID),
    if MessID < LBoundMessID -> before;
       MessID > UBoundMessID -> 'after';
       true -> inside
    end.

maybe_dig_minimum_mess_id([]) -> undefined;
maybe_dig_minimum_mess_id(Digest) -> dig_minimum_mess_id(Digest).

maybe_dig_maximum_mess_id([]) -> undefined;
maybe_dig_maximum_mess_id(Digest) -> dig_maximum_mess_id(Digest).

dig_minimum_mess_id(Digest) ->
    hour_to_min_mess_id(dig_first_hour(Digest)).

dig_maximum_mess_id(Digest) ->
    hour_to_max_mess_id(dig_last_hour(Digest)).

maybe_mess_id_to_lower_bound_binary(undefined) ->
    min_binary_mess_id();
maybe_mess_id_to_lower_bound_binary(MessID) ->
    mess_id_to_binary(MessID).

maybe_mess_id_to_upper_bound_binary(undefined) ->
    max_binary_mess_id();
maybe_mess_id_to_upper_bound_binary(MessID) ->
    mess_id_to_binary(MessID).


analyse_digest_before(RQ, RSM, Start, End, PageSize, BeforeKey, Digest) ->
    %% Any value from `RSetDigest' is inside the result set.
    %% `RSetDigest' does not contain `hour(Start)' and `hour(End)'.
    RSetDigest = dig_between(Start, End, Digest),
    %% Hour in which a message with the passed id was send.
    BeforeHour = key_to_hour(BeforeKey),
    %% Top values from `PageDigest' will be on the page.
    %% `BeforeHour' is not included.
    PageDigest = dig_before_hour(BeforeHour, RSetDigest),
    KeyPos = key_position(BeforeKey, Start, End, Digest),
    Strategy = case {dig_is_empty(RSetDigest),
                     is_single_page(PageSize, PageDigest), KeyPos} of
        {true, _, _} -> query_all;
        {false, _, before} -> count_only;
        {false, true, _} -> first_page;
        {false, false, inside} -> middle_page;
        {false, _, 'after'} -> last_page %% or near
        end,
    %% Place different points on the timeline:
    %% B - before, S - start, E - end, N - now
    case Strategy of
    query_all -> query_all(RQ, RSM, PageSize);
    %% B S E
    count_only -> %% return a total count only
        StartHourCnt = maybe_get_hour_key_count_after(RQ, Digest, Start),
        RecentCnt = get_recent_key_count(RQ, RSetDigest, End),
        RSetDigestCnt = dig_total(RSetDigest),
        TotalCount = StartHourCnt + RSetDigestCnt + RecentCnt,
        return_matched_keys(TotalCount, 0, []);
    %% S B E
    first_page ->
        %% Page is in the beginning of the RSet.
        %% `PageDigest' is empty.
        BeforeMessID = key_to_mess_id(BeforeKey),
        LBound = maybe_microseconds_to_min_mess_id(Start),
        UBound = previous_mess_id(BeforeMessID),
        Keys = request_keys(set_bounds(LBound, UBound, RQ)),
        StartHourCnt = filter_and_count_keys_before_digest(Keys, RSetDigest),
        MatchedKeys = sublist_r(Keys, PageSize),
        Offset = length(Keys) - length(MatchedKeys),
        RecentCnt = get_recent_key_count(RQ, RSetDigest, End),
        RSetDigestCnt = dig_total(RSetDigest),
        TotalCount = StartHourCnt + RSetDigestCnt + RecentCnt,
        return_matched_keys(TotalCount, Offset, MatchedKeys);
    %% S B E
    middle_page ->
        MinHour = page_minimum_hour(PageSize, PageDigest),
        BeforeMessID = key_to_mess_id(BeforeKey),
        LBound = hour_to_min_mess_id(MinHour),
        UBound = previous_mess_id(BeforeMessID),
        Keys = request_keys(set_bounds(LBound, UBound, RQ)),
        MatchedKeys = sublist_r(Keys, PageSize),
        StartHourCnt = maybe_get_hour_key_count_after(RQ, Digest, Start),
        RecentCnt = get_recent_key_count(RQ, RSetDigest, End),
        RSetDigestCnt = dig_total(RSetDigest),
        TotalCount = StartHourCnt + RSetDigestCnt + RecentCnt,
        %% Count of messages between `PageDigest' and `BeforeKey'.
        BeforeOffset = filter_and_count_recent_keys(Keys, PageDigest),
        LastMatchedOffset = StartHourCnt + dig_total(PageDigest) + BeforeOffset,
        Offset = LastMatchedOffset - length(MatchedKeys),
        return_matched_keys(TotalCount, Offset, MatchedKeys);
    %% S E B
    %% S E N B
    %% S E B N
    last_page ->
        MinHour = page_minimum_hour(PageSize, PageDigest),
        LBound = hour_to_min_mess_id(MinHour),
        BeforeMessID = key_to_mess_id(BeforeKey),
        EndMessID = maybe_microseconds_to_max_mess_id(End),
        UBound = maybe_min(previous_mess_id(BeforeMessID), EndMessID),
        Keys = request_keys(set_bounds(LBound, UBound, RQ)),
        MatchedKeys = sublist_r(Keys, PageSize),
        StartHourCnt = maybe_get_hour_key_count_after(RQ, Digest, Start),
        %% Count of messages between `PageDigest' and `BeforeKey'.
        BeforeOffset = filter_and_count_recent_keys(Keys, PageDigest),
        LastMatchedOffset = StartHourCnt + dig_total(PageDigest) + BeforeOffset,
        RecentCnt = request_key_count(set_bounds(BeforeMessID, EndMessID, RQ)),
        TotalCount = LastMatchedOffset + RecentCnt,
        Offset = LastMatchedOffset - length(MatchedKeys),
        return_matched_keys(TotalCount, Offset, MatchedKeys)
    end.


analyse_digest_after(RQ, RSM, Start, End, PageSize, AfterKey, Digest) ->
    %% Any value from `RSetDigest' is inside the result set.
    %% `RSetDigest' does not contain `hour(Start)' and `hour(End)'.
    RSetDigest = dig_between(Start, End, Digest),
    %% Hour in which a message with the passed id was send.
    AfterHour = key_to_hour(AfterKey),
    %% Middle values from `PageDigest' will be on the page.
    %% `AfterHour' is not included.
    PageDigest = dig_after_hour(AfterHour, RSetDigest),
    KeyPos = key_position(AfterKey, Start, End, RSetDigest),
    Strategy = case {dig_is_empty(RSetDigest),
                     dig_is_empty(PageDigest),
                     is_single_page(PageSize, PageDigest), KeyPos} of
        {true,  _,     _,     _      } -> after_query_all;
        {false, _,     _,     before } -> after_first_page;
        {false, _,     true,  inside } -> after_last_page;
        {false, true,  true,  'after'} -> after_recent_only;
        {false, false, false, inside } -> after_middle_page
        end,
    %% Place different points on the timeline:
    %% A - after, S - start, E - end, N - now
    case Strategy of
    after_query_all -> query_all(RQ, RSM, PageSize);
    %% S E A
    after_count_only -> %% return a total count only
        StartHourCnt = maybe_get_hour_key_count_after(RQ, Digest, Start),
        RecentCnt = get_recent_key_count(RQ, RSetDigest, End),
        RSetDigestCnt = dig_total(RSetDigest),
        TotalCount = StartHourCnt + RSetDigestCnt + RecentCnt,
        return_matched_keys(TotalCount, 0, []);
    %% A S E
    after_first_page ->
        %% Page is in the beginning of the RSet.
        analyse_digest_index(RQ, RSM, 0, Start, End, PageSize, Digest);
    %% S A E
    after_middle_page ->
        StartHourCnt = maybe_get_hour_key_count_after(RQ, Digest, Start),
        RecentCnt = get_recent_key_count(RQ, RSetDigest, End),
        RSetDigestCnt = dig_total(RSetDigest),
        TotalCount = StartHourCnt + RSetDigestCnt + RecentCnt,
        UHour = page_maximum_hour(PageSize, PageDigest),
        %% Next message id.
        LBound = next_mess_id(key_to_mess_id(AfterKey)),
        UBound = hour_to_max_mess_id(UHour),
        Keys = request_keys(set_bounds(LBound, UBound, RQ)),
        MatchedKeys = lists:sublist(Keys, PageSize),
        %% Position in `RSetDigestCnt' of the last selected key.
        LastKeyOffset = dig_total(dig_to_hour(UHour, RSetDigest)),
        Offset = StartHourCnt + LastKeyOffset - length(Keys),
        return_matched_keys(TotalCount, Offset, MatchedKeys);
    %% S A E
    after_last_page ->
        %% Request keys between `PageDigest' and the end of the RSet.
        LBound = next_mess_id(key_to_mess_id(AfterKey)),
        UBound = maybe_microseconds_to_max_mess_id(End),
        Keys = request_keys(set_bounds(LBound, UBound, RQ)),
        %% Keys in the tail of the digest
        DigestTailCnt = filter_and_count_digest_keys(Keys, RSetDigest),
        MatchedKeys = lists:sublist(Keys, PageSize),
        StartHourCnt = maybe_get_hour_key_count_after(RQ, Digest, Start),
        RSetDigestCnt = dig_total(RSetDigest),
        %% Count of messages between `PageDigest' and `AfterKey'.
        Offset = StartHourCnt + RSetDigestCnt - DigestTailCnt,
        TotalCount = Offset + length(Keys),
        return_matched_keys(TotalCount, Offset, MatchedKeys);
    after_recent_only ->
        %% assert
        true  = dig_is_empty(PageDigest),
        false = dig_is_empty(RSetDigest),

        %% Get recent keys after `RSetDigest'.
        Keys = get_recent_keys(RQ, RSetDigest, End),
        AfterKeys = filter_after(AfterKey, Keys),
        MatchedKeys = lists:sublist(AfterKeys, PageSize),
        StartHourCnt = maybe_get_hour_key_count_after(RQ, Digest, Start),
        RSetDigestCnt = dig_total(RSetDigest),
        %% Count of messages between `PageDigest' and `AfterKey'.
        AfterOffset = length(Keys) - length(AfterKeys),
        Offset = StartHourCnt + RSetDigestCnt + AfterOffset,
        TotalCount = Offset + length(AfterKeys),
        return_matched_keys(TotalCount, Offset, MatchedKeys)
    end.

dig_between(Start, End, Digest) ->
    Strategy = choose_strategy(Start, End, Digest),
    case Strategy of
        empty -> dig_empty();
        recent_only -> dig_empty();
        whole_digest_and_recent -> Digest;
        lower_bounded -> dig_after_hour(hour(Start), Digest);
        upper_bounded -> dig_before_hour(hour(End), Digest);
        bounded -> dig_before_hour(hour(End), dig_after_hour(hour(Start), Digest));
        _ -> Digest
    end.

filter_and_count_keys_before_digest(Keys, RSetDigest) ->
    RSetDigestMinHour = dig_first_hour(RSetDigest),
    length([K || K <- Keys, key_to_hour(K) < RSetDigestMinHour]).

choose_strategy(Start, End, Digest) ->
    %% Where the lower bound is located: before, inside or after the index digest.
    StartPos = dig_position_start(Start, Digest),
    EndPos = dig_position_end(End, Digest),
    %% There are impossible cases due to `Start =< End'.
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
    end.

choose_strategy2(Start, End, Digest) ->
    Strategy = choose_strategy(Start, End, Digest),
    case Strategy of
        inside ->
            %% Maximum RSet
            RSetDigest = dig_to_hour(hour(End), dig_from_hour(hour(Start), Digest)),
            case dig_is_empty(RSetDigest) of
                true -> empty;
                false -> inside
            end;
        _ -> Strategy
    end.

analyse_digest_last_page(RQ, RSM, Start, End, PageSize, Digest) ->
    Strategy = choose_strategy2(Start, End, Digest),
    case Strategy of
        empty -> return_empty();
        recent_only -> query_all(RQ, RSM, PageSize);
        whole_digest_and_recent ->
            Keys = get_minimum_key_range_before(RQ, Digest, End, PageSize),
            RecentCnt = filter_and_count_recent_keys(Keys, Digest),
            TotalCount = dig_total(Digest) + RecentCnt,
            MatchedKeys = sublist_r(Keys, PageSize),
            Offset = TotalCount - length(MatchedKeys),
            return_matched_keys(TotalCount, Offset, MatchedKeys);
        lower_bounded ->
            %% Contains records, that will be in RSet for sure
            %% Digest2 does not contain hour(Start).
            Digest2 = dig_after_hour(hour(Start), Digest),
            case dig_total(Digest2) =< PageSize of
            true -> query_all(RQ, RSM, PageSize); %% It is a small range
            false ->
                StartHourCnt = get_hour_key_count_from(RQ, Digest, Start),
                Keys = get_minimum_key_range_before(RQ, Digest2, End, PageSize),
                RecentCnt = filter_and_count_recent_keys(Keys, Digest2),
                TotalCount = StartHourCnt + dig_total(Digest2) + RecentCnt,
                MatchedKeys = sublist_r(Keys, PageSize),
                Offset = TotalCount - length(MatchedKeys),
                return_matched_keys(TotalCount, Offset, MatchedKeys)
            end;
        upper_bounded ->
            Digest2 = dig_before_hour(hour(End), Digest),
            case dig_total(Digest2) =< PageSize of
            true -> query_all(RQ, RSM, PageSize); %% It is a small range
            false ->
                Keys = get_minimum_key_range_before(RQ, Digest2, End, PageSize),
                LastHourCnt = filter_and_count_recent_keys(Keys, Digest2),
                TotalCount = dig_total(Digest2) + LastHourCnt,
                MatchedKeys = sublist_r(Keys, PageSize),
                Offset = TotalCount - length(MatchedKeys),
                return_matched_keys(TotalCount, Offset, MatchedKeys)
            end;
        bounded ->
            Digest2 = dig_before_hour(hour(End),
                dig_after_hour(hour(Start), Digest)),
            case dig_total(Digest2) =< PageSize of
            true -> query_all(RQ, RSM, PageSize); %% It is a small range
            false ->
                StartHourCnt = get_hour_key_count_from(RQ, Digest, Start),
                Keys = get_minimum_key_range_before(RQ, Digest2, End, PageSize),
                LastHourCnt = filter_and_count_recent_keys(Keys, Digest2),
                TotalCount = StartHourCnt + dig_total(Digest2) + LastHourCnt,
                MatchedKeys = sublist_r(Keys, PageSize),
                Offset = TotalCount - length(MatchedKeys),
                return_matched_keys(TotalCount, Offset, MatchedKeys)
            end;
        _ -> query_all(RQ, RSM, PageSize)
    end.

return_empty() ->
    {ok, {0, 0, []}}.


query_all(RQ, RSM, PageSize) ->
    Keys = request_keys(RQ),
    TotalCount = length(Keys),
    {Offset, MatchedKeys} = paginate(Keys, RSM, PageSize),
    return_matched_keys(TotalCount, Offset, MatchedKeys).



should_create_digest(LocLServer, {ok, {TotalCount, _, _}}) ->
    TotalCount >= digest_creation_threshold(LocLServer);
should_create_digest(_, _) ->
    false.

create_non_empty_digest(_Conn, _DigestKey, []) ->
    ok; %% empty
create_non_empty_digest(Conn, DigestKey, Keys) ->
    create_digest(Conn, DigestKey, Keys).

create_digest(Conn, DigestKey, Keys) ->
    lager:debug("Creating a digest with ~p keys.", [length(Keys)]),
    Digest = dig_new(Keys),
    Value = digest_to_binary(Digest),
    DigestObj = riakc_obj:new(digest_bucket(), DigestKey, Value),
    riakc_pb_socket:put(Conn, DigestObj).

is_empty_digest_obj(Obj) ->
    riakc_obj:get_value(Obj) =:= <<>>.

digest_to_binary([]) ->
    <<>>;
digest_to_binary(Digest) ->
    term_to_binary(Digest).

binary_to_digest(<<>>) ->
    [];
binary_to_digest(Bin) ->
    binary_to_term(Bin).

get_actual_digest(Conn, RQ, DigestKey, Now) ->
    case riakc_pb_socket:get(Conn, digest_bucket(), DigestKey) of
        {error, notfound} ->
            {error, notfound};
        {ok, DigestObj} ->
            DigestObj2 = merge_siblings(RQ, DigestObj),
            Digest2 = binary_to_digest(riakc_obj:get_value(DigestObj2)),
            Last = last_modified(DigestObj2),
            case hour(Last) =:= hour(Now) of
                %% Digest is actual.
                true ->
                    %% Write the merged version.
                    has_siblings(DigestObj) andalso riakc_pb_socket:put(Conn, DigestObj2),
                    {ok, Digest2};
                false ->
                    Digest3 = update_digest(RQ, Now, Digest2),
                    DigestObj3 = riakc_obj:update_value(DigestObj2, digest_to_binary(Digest3)),
                    riakc_pb_socket:put(Conn, DigestObj3),
                    {ok, Digest3}
            end
    end.

has_siblings(Obj) ->
    length(riakc_obj:get_values(Obj)) > 1.

merge_siblings(RQ, DigestObj) ->
    case has_siblings(DigestObj) of
        true ->
            DigestObj2 = fix_metadata(DigestObj),
            Values = riakc_obj:get_values(DigestObj),
            NewDigest = dig_merge(RQ, deserialize_values(Values)),
            riakc_obj:update_value(DigestObj2, digest_to_binary(NewDigest));
        false ->
            DigestObj
    end.

%% @doc Request new entries from the server.
%% `Now' are microseconds.
update_digest(RQ, Now, Digest) ->
    LHour = dig_last_hour(Digest) + 1,
    UHour = hour(Now) - 1,
    LBound = hour_to_min_mess_id(LHour),
    UBound = hour_to_max_mess_id(UHour),
    Keys = request_keys(set_bounds(LBound, UBound, RQ)),
    dig_add_keys(Keys, Digest).


%% @doc Return mtime of the object in microseconds.
last_modified(Obj) ->
    MD = riakc_obj:get_metadata(Obj),
    TimeStamp = proplists:get_value(<<"X-Riak-Last-Modified">>, MD),
    mod_mam_utils:now_to_microseconds(TimeStamp).


deserialize_values(Values) ->
    [binary_to_term(Value) || Value <- Values].

choose_index(undefined) ->
    key_index();
choose_index(#jid{lresource = <<>>}) ->
    user_id_bare_remote_jid_index();
choose_index(#jid{}) ->
    user_id_full_remote_jid_index().

choose_index_bin(undefined) ->
    key_index();
choose_index_bin(BWithJID) ->
    case has_resource(BWithJID) of
        true -> 
            user_id_full_remote_jid_index();
        false ->
            user_id_bare_remote_jid_index()
    end.

secondary_key(BUserID, BMessID, undefined) ->
    message_key(BUserID, BMessID);
secondary_key(BUserID, BMessID, BWithJID) ->
    user_remote_jid_message_id(BUserID, BMessID, BWithJID).

min_digest_key(BUserID) ->
    digest_key(BUserID, undefined).

max_digest_key(BUserID) ->
    %% JID cannot contain 255
    digest_key(BUserID, <<255>>).

%% @see jid_to_opt_binary
min_full_jid_digest_key(BUserID, BWithBareJID) ->
    <<BUserID/binary, BWithBareJID/binary, $/>>.

max_full_jid_digest_key(BUserID, BWithBareJID) ->
    <<BUserID/binary, BWithBareJID/binary, $/, 255>>.

digest_key(BUserID, undefined) when is_binary(BUserID) ->
    <<BUserID/binary>>;
digest_key(BUserID, BWithJID) when is_binary(BUserID), is_binary(BWithJID) ->
    <<BUserID/binary, BWithJID/binary>>.

%% @see user_id_to_binary/1
%% @see jid_to_opt_binary
decode_digest_key(<<BUserID:8/binary, BWithJID/binary>>) ->
    {BUserID, case BWithJID of <<>> -> undefined; _ -> BWithJID end}.
    

max_binary_mess_id() ->
    <<-1:64>>. %% set all bits.

min_binary_mess_id() ->
    <<>>. %% any(binary()) >= <<>>


maybe_microseconds_to_min_mess_id(undefined) ->
    undefined;
maybe_microseconds_to_min_mess_id(Microseconds) ->
    microseconds_to_min_mess_id(Microseconds).

microseconds_to_min_mess_id(Microseconds) ->
    encode_compact_uuid(Microseconds, 0).


maybe_microseconds_to_max_mess_id(undefined) ->
    undefined;
maybe_microseconds_to_max_mess_id(Microseconds) ->
    microseconds_to_max_mess_id(Microseconds).

microseconds_to_max_mess_id(Microseconds) ->
    encode_compact_uuid(Microseconds, 255).


remove_user_from_db(LServer, LUser) ->
    F = fun(Conn) ->
        delete_digests(Conn, LServer, LUser),
        delete_messages(Conn, LServer, LUser)
        end,
    with_connection(LServer, F),
    ok.

-spec purge_single_message(UserJID, MessID, Now) ->
    ok | {error, 'not-allowed' | 'not-found'} when
    UserJID :: #jid{},
    MessID :: message_id(),
    Now :: unix_timestamp().
purge_single_message(UserJID=#jid{lserver = LServer}, MessID, Now)
    when is_integer(MessID), is_integer(Now) ->
    with_connection(LServer, fun(Conn) ->
        purge_single_message(Conn, UserJID, MessID, Now)
        end).

purge_single_message(Conn, #jid{lserver = LServer, luser = LUser}, MessID, Now) ->
    UserID = mod_mam_cache:user_id(LServer, LUser),
    BUserID = user_id_to_binary(UserID),
    BMessID = mess_id_to_binary(MessID),
    Key = message_key(BUserID, BMessID),
    OnlyRecentMessages = hour(Now) =:= hour(mess_id_to_microseconds(MessID)),
    case riakc_pb_socket:get(Conn, message_bucket(), Key) of
        {error, notfound} -> {error, 'not-found'};
        {ok, _} when OnlyRecentMessages ->
            riakc_pb_socket:delete(Conn, message_bucket(), Key),
            ok;
        {ok, MessObj} ->
            {BBareRemJID, BFullRemJID} = extract_bin_remote_jids(MessObj),
            UserDigestKey = digest_key(BUserID, undefined),
            BareDigestKey = digest_key(BUserID, BBareRemJID),
            FullDigestKey = digest_key(BUserID, BFullRemJID),
            Updated = single_purge(Conn, MessID, UserDigestKey)
                   ++ single_purge(Conn, MessID, BareDigestKey)
                   ++ single_purge(Conn, MessID, FullDigestKey),
            save_updated_digests(Conn, Updated),
            riakc_pb_socket:delete(Conn, message_bucket(), Key),
            ok
    end.

-spec purge_multiple_messages(UserJID, Start, End, Now, WithJID) ->
    ok | {error, 'not-allowed'} when
    UserJID :: #jid{},
    Start   :: unix_timestamp() | undefined,
    End     :: unix_timestamp() | undefined,
    Now     :: unix_timestamp(),
    WithJID :: #jid{} | undefined.
purge_multiple_messages(UserJID = #jid{lserver=LServer}, Start, End, Now, WithJID) ->
    with_connection(LServer, fun(Conn) ->
        purge_multiple_messages(Conn, UserJID, Start, End, Now, WithJID)
        end).

purge_multiple_messages(Conn, #jid{lserver=LServer, luser=LUser}, Start, End,
                        Now, WithJID) ->
    UserID = mod_mam_cache:user_id(LServer, LUser),
    BUserID = user_id_to_binary(UserID),
    BWithJID = maybe_jid_to_opt_binary(LServer, WithJID),
    SecIndex = choose_index(WithJID),
    LBound = maybe_microseconds_to_min_mess_id(Start),
    UBound = maybe_microseconds_to_max_mess_id(End),
    LBoundKey = maybe_mess_id_to_lower_bound_binary(LBound),
    UBoundKey = maybe_mess_id_to_upper_bound_binary(UBound),
    LBoundSecKey = secondary_key(BUserID, LBoundKey, BWithJID),
    UBoundSecKey = secondary_key(BUserID, UBoundKey, BWithJID),
    Keys = get_key_range(Conn, SecIndex, LBoundSecKey, UBoundSecKey),
    [riakc_pb_socket:delete(Conn, message_bucket(), Key)
    || Key <- Keys],
    OnlyRecentMessages = is_defined(Start) andalso hour(Now) =:= hour(Start),
    DoNothing = Keys =:= [],
    Strategy =
       case {DoNothing or OnlyRecentMessages, WithJID} of
        {true,  _                     } -> skip_digest_update;
        {false, undefined             } -> undef_jid;
        {false, #jid{lresource = <<>>}} -> bare_jid;
        {false, #jid{}                } -> full_jid
    end,
    case Strategy of
    skip_digest_update ->
        ok;
    undef_jid ->
        %% Delete information about records from the range from all digests.
        OldDigestObjects = get_all_digest_objects(Conn, LServer, LUser),
        NewDigestObjects = merge_and_purge_digest_objects(Conn, Start, End, OldDigestObjects),
        Updated = filter_midified_objects(OldDigestObjects, NewDigestObjects),
        save_updated_digests(Conn, Updated),
        ok;
    bare_jid ->
        %% Delete information about records from the range from per JID digests.
        %% Selectively delete info from the main user digest.
        UserDigestKey = digest_key(BUserID, undefined),
        BareDigestKey = digest_key(BUserID, BWithJID),
        FullDigestKeys = get_full_jid_digest_keys(Conn, LServer, LUser, BWithJID),
        %% Delete all entries in the range for next digests.
        DigestKeys = [BareDigestKey | FullDigestKeys],
        OldDigestObjects = to_digest_objects(Conn, DigestKeys),
        NewDigestObjects = merge_and_purge_digest_objects(Conn, Start, End, OldDigestObjects),
        Updated = sparse_purge(Conn, Keys, UserDigestKey)
               ++ filter_midified_objects(OldDigestObjects, NewDigestObjects),
        save_updated_digests(Conn, Updated),
        ok;
    full_jid ->
        %% We only need to change 3 digests.
        %% Delete information about records from the full JID digests.
        %% Selectively delete info from the bare JID digest.
        %% Selectively delete info from the main user digest.
        BWithBareJID = jid_to_opt_binary(LServer, jlib:jid_remove_resource(WithJID)),
        BWithFullJID = jid_to_opt_binary(LServer, WithJID),
        UserDigestKey = digest_key(BUserID, undefined),
        BareDigestKey = digest_key(BUserID, BWithBareJID),
        FullDigestKey = digest_key(BUserID, BWithFullJID),
        Updated = sparse_purge(Conn, Keys, UserDigestKey)
               ++ sparse_purge(Conn, Keys, BareDigestKey)
               ++ dense_purge(Conn, Start, End, FullDigestKey),
        save_updated_digests(Conn, Updated),
        ok
    end.


delete_digests(Conn, LServer, LUser) ->
    Keys = get_all_digest_keys(Conn, LServer, LUser),
    [riakc_pb_socket:delete(Conn, digest_bucket(), Key)
    || Key <- Keys],
    ok.

%% @doc Get keys for full JIDs with the same bare JID.
get_full_jid_digest_keys(Conn, LServer, LUser, BWithBareJID) ->
    %% jid_to_opt_binary
    UserID = mod_mam_cache:user_id(LServer, LUser),
    BUserID = user_id_to_binary(UserID),
    LBound = min_full_jid_digest_key(BUserID, BWithBareJID),
    UBound = max_full_jid_digest_key(BUserID, BWithBareJID),
    get_key_range(Conn, digest_bucket(), key_index(), LBound, UBound).

get_all_digest_keys(Conn, LServer, LUser) ->
    UserID = mod_mam_cache:user_id(LServer, LUser),
    BUserID = user_id_to_binary(UserID),
    LBound = min_digest_key(BUserID),
    UBound = max_digest_key(BUserID),
    get_key_range(Conn, digest_bucket(), key_index(), LBound, UBound).

get_all_digest_objects(Conn, LServer, LUser) ->
    UserID = mod_mam_cache:user_id(LServer, LUser),
    BUserID = user_id_to_binary(UserID),
    LBound = min_digest_key(BUserID),
    UBound = max_digest_key(BUserID),
    get_object_range(Conn, digest_bucket(), key_index(), LBound, UBound).

delete_messages(Conn, LServer, LUser) ->
    Keys = get_all_message_keys(Conn, LServer, LUser),
    [riakc_pb_socket:delete(Conn, message_bucket(), Key)
    || Key <- Keys],
    ok.

get_all_message_keys(Conn, LServer, LUser) ->
    UserID = mod_mam_cache:user_id(LServer, LUser),
    BUserID = user_id_to_binary(UserID),
    LBound = message_key(BUserID, min_binary_mess_id()),
    UBound = message_key(BUserID, max_binary_mess_id()),
    get_key_range(Conn, message_bucket(), key_index(), LBound, UBound).

get_hour_keys_before(Conn, BUserID, Start) ->
    LMessID = hour_to_min_mess_id(hour(Start)),
    UMessID = previous_mess_id(microseconds_to_max_mess_id(Start)),
    LBound = message_key(BUserID, LMessID),
    UBound = message_key(BUserID, UMessID),
    get_key_range(Conn, message_bucket(), key_index(), LBound, UBound).

get_hour_keys_after(Conn, BUserID, End) ->
    LMessID = next_mess_id(microseconds_to_min_mess_id(End)),
    UMessID = hour_to_max_mess_id(hour(End)),
    LBound = message_key(BUserID, LMessID),
    UBound = message_key(BUserID, UMessID),
    get_key_range(Conn, message_bucket(), key_index(), LBound, UBound).

dig_compare_volume(Hour, DeletedDigest, UserDigest) ->
    dig_calc_volume(Hour, DeletedDigest) =:=
    dig_calc_volume(Hour, UserDigest).

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

has_resource(BWithJID) ->
    binary:match(BWithJID, <<"/">>) =/= nomatch.

wait_flushing(_LServer) ->
    ok.

archive_message(MessID, _Dir, _LocJID=#jid{luser=LocLUser, lserver=LocLServer},
                RemJID=#jid{}, SrcJID, Packet) ->
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

extract_bin_remote_jids(Obj) ->
    MD = riakc_obj:get_metadata(Obj),
    [BareSecIdx] = riakc_obj:get_secondary_index(MD, user_id_bare_remote_jid_index()),
    [FullSecIdx] = riakc_obj:get_secondary_index(MD, user_id_full_remote_jid_index()),
    BBareJID = user_remote_jid_message_id_to_bjid(BareSecIdx),
    BFullJID = user_remote_jid_message_id_to_bjid(FullSecIdx),
    {BBareJID, BFullJID}.


with_connection(_LocLServer, F) ->
    ?MEASURE_TIME(with_connection,
        riak_pool:with_connection(mam_cluster, F)).

user_id_to_binary(UserID) when is_integer(UserID) ->
    <<UserID:64/big>>.

mess_id_to_binary(MessID) when is_integer(MessID) ->
    <<MessID:64/big>>.

%% @doc Transforms microseconds to hours.
hour(Ms) when is_integer(Ms) ->
    Ms div microseconds_in_hour().

mess_id_to_hour(MessID) when is_integer(MessID) ->
    hour(mess_id_to_microseconds(MessID)).

hour_to_microseconds(Hour) when is_integer(Hour) ->
    Hour * microseconds_in_hour().

microseconds_in_hour() ->
    3600000000.

hour_to_min_microseconds(Hour) ->
    hour_to_microseconds(Hour).

hour_to_max_microseconds(Hour) ->
    hour_to_microseconds(Hour+1) - 1.

hour_to_min_mess_id(Hour) ->
    microseconds_to_min_mess_id(hour_to_min_microseconds(Hour)).

hour_to_max_mess_id(Hour) ->
    microseconds_to_max_mess_id(hour_to_max_microseconds(Hour)).

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

user_remote_jid_message_id_to_bjid(SecIndex) when is_binary(SecIndex) ->
    %% `Delete' `BUserID', `0' and `BMessID'.
    binary:part(SecIndex, 8, byte_size(SecIndex) - 17).
    

key_to_hour(Key) when is_binary(Key) ->
    hour(key_to_microseconds(Key)).

key_to_microseconds(Key) when is_binary(Key) ->
    mess_id_to_microseconds(key_to_mess_id(Key)).

mess_id_to_microseconds(MessID) ->
    {Microseconds, _} = mod_mam_utils:decode_compact_uuid(MessID),
    Microseconds.

key_to_mess_id(Key) ->
    %% Get 8 bytes.
    <<MessID:64/big>> = binary:part(Key, byte_size(Key), -8),
    MessID.

next_mess_id(MessID) -> MessID + 1.

%% This function is used to create a message id, that will be lower that `MessID',
%% but greater or equal, than any message id before `MessID'.
previous_mess_id(MessID) -> MessID - 1.

replace_mess_id(NewMessID, SrcKey) ->
    %% Delete old message id.
    <<Prefix/binary>> = binary:part(SrcKey, 0, byte_size(SrcKey) - 8),
    <<Prefix/binary, NewMessID:64/big>>.


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

dig_first_count([{_,Cnt}|_]) ->
    Cnt.

dig_last_count([{_,Cnt}]) ->
    Cnt;
dig_last_count([_|T]) ->
    dig_last_count(T).


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

dig_total_tail([]) ->
    0;
dig_total_tail(Digest) ->
    dig_total(dig_tail(Digest)).

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
%% Digest should be not empty.
filter_and_count_recent_keys(Keys, Digest) ->
    LastHour = dig_last_hour(Digest),
    dig_total(dig_after_hour(LastHour, dig_new(Keys))).

%% @doc Drop all keys outside of `Digest' and count them.
filter_and_count_digest_keys(Keys, Digest) ->
    length(Keys) - filter_and_count_recent_keys(Keys, Digest).


%% @doc Drop keys before or equal `AfterKey'.
filter_after(AfterKey, [Key|Keys]) when Key =< AfterKey ->
    filter_after(AfterKey, Keys);
filter_after(_, Keys) -> Keys.

filter_and_count_hour_keys(Hour, Keys) ->
    length(filter_hour_keys(Hour, Keys)).

filter_hour_keys(Hour, Keys) ->
    [Key || Key <- Keys, key_to_hour(Key) =:= Hour].

dig_is_recorded(Hour, Digest) ->
    %% Tip: if no information exists after specified hour,
    %%      `dig_calc_volume' returns `undefined'.
    is_defined(dig_calc_volume(Hour, Digest)).

dig_is_skipped(Hour, Digest) ->
    dig_calc_volume(hour(Hour), Digest) =:= 0.


dig_skip_n(N, [{_, Cnt}|T]) when N >= Cnt ->
    dig_skip_n(N - Cnt, T);
dig_skip_n(N, T) ->
    {N, T}.


dig_nth(N, [{_, Cnt}|T]) when N > Cnt ->
    dig_nth(N - Cnt, T);
dig_nth(_, [{Hour, _}|_]) ->
    Hour.

dig_is_nth_defined(N, Digest) ->
    N =< dig_total(Digest).

dig_nth_pos(N, Digest) ->
    case dig_is_nth_defined(N, Digest) of
        true -> inside;
        false -> 'after'
    end.


dig_skip_nr(N, Digest) ->
    {N1, T} = dig_skip_n(N, lists:reverse(Digest)),
    {N1, lists:reverse(T)}.

dig_tail([_|T]) -> T.

dig_is_empty(X) -> X == [].

dig_add_keys(Keys, Digest) ->
    %% Sorted.
    Digest ++ dig_new(Keys).

dig_merge(RQ, Digests) ->
    SiblingCnt = length(Digests),
    F1 = fun({Hour, Cnt}, Dict) -> dict:append(Hour, Cnt, Dict) end,
    F2 = fun(Digest, Dict) -> lists:foldl(F1, Dict, Digest) end,
    Hour2Cnts = dict:to_list(lists:foldl(F2, dict:new(), Digests)),
    [{NewHour, NewCnt} ||
        {Hour, Cnts} <- Hour2Cnts,
        {NewHour, NewCnt} <- merge_digest(
                RQ, Hour, Cnts, SiblingCnt)].

dig_concat(Digests) ->
    [Hour2Count || Digest <- Digests, Hour2Count <- Digest].

merge_digest(RQ, Hour, Cnts, SiblingCnt) ->
    case all_equal(Cnts) andalso length(Cnts) =:= SiblingCnt of
        true -> [{Hour, hd(Cnts)}]; %% good value
        false ->
            %% collision
            case get_hour_key_count(RQ, Hour) of
                0 -> [];
                NewCnt -> [{Hour, NewCnt}]
            end
    end.

all_equal([H|T]) ->
    all_equal(H, T).

all_equal(H, [H|T]) -> all_equal(H, T);
all_equal(_, [_|_]) -> false;
all_equal(_, []) -> true.

%% @doc Subtract `Subtract' from `Digest' for each hour.
%% `Digest' should contain all keys from `DeletedDigest'.
-spec dig_subtract(Digest, SubDigest) -> Digest when
    Digest :: list(),
    SubDigest :: list().
dig_subtract([H|T1], [H|T2]) ->
    dig_subtract(T1, T2);
dig_subtract([{Hour, Cnt}|T1], [{Hour, SubCnt}|T2]) when Cnt > SubCnt ->
    [{Hour, Cnt-SubCnt}|dig_subtract(T1, T2)];
dig_subtract([{Hour, Cnt}|T1], [{Hour, SubCnt}|T2]) when Cnt < SubCnt ->
    %% To much to delete.
    %% unexpected, ignore
    dig_subtract(T1, T2);
dig_subtract([{Hour1, _}=H1|T1], [{Hour2, _}|_]=T2) when Hour1 < Hour2 ->
    %% Hour1 | ? |   ?   |
    %% none  | ? | Hour2 |
    [H1|dig_subtract(T1, T2)];
dig_subtract([{Hour1, _}=H1|T1], [{Hour2, _}|_]=T2) when Hour1 < Hour2 ->
    %% No entries about this hour.
    %% none  | ? |   ?   |
    %% Hour2 | ? | Hour2 |
    %% unexpected, ignore
    [H1|dig_subtract(T1, T2)];
dig_subtract([], []) ->
    [].

%% @doc Decrease a counter for a specified hour.
dig_decrease([{Hour, Cnt}|T], Hour, Cnt) ->
    T;
dig_decrease([{Hour, Cnt}|T], Hour, SubCnt) ->
    [{Hour, Cnt - SubCnt}|T];
dig_decrease([{Hour, Cnt}|T], SubHour, SubCnt) when Hour < SubHour ->
    [{Hour, Cnt}|dig_decrease(T, SubHour, SubCnt)];
dig_decrease(T, _, _) ->
    T.

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

dig_empty() ->
    [].

%% Returns `[{Hour, MessageCount}]'.
frequency([H|T]) ->
    frequency_1(T, H, 1);
frequency([]) ->
    [].

frequency_1([H|T], H, N) ->
    frequency_1(T, H, N+1);
frequency_1(T, H, N) ->
    [{H, N}|frequency(T)].


%% @doc Return sorted keys.
to_values(Conn, Keys) ->
    [assert_valid_key(Key) || Key <- Keys],
    Ops = [{map, {modfun, riak_kv_mapreduce, map_object_value}, undefined, true}],
    case riakc_pb_socket:mapred(Conn, Keys, Ops) of
        {ok, []} ->
            [];
        {ok, [{0, Values}]} ->
            Values
    end.

to_digest_objects(Conn, Keys) ->
    FullKeys = [{digest_bucket(), Key} || Key <- Keys],
    to_objects(Conn, FullKeys).

to_objects(Conn, Keys) ->
    [assert_valid_key(Key) || Key <- Keys],
    Ops = [{map, {modfun, riak_kv_mapreduce, map_identity}, undefined, true}],
    case riakc_pb_socket:mapred(Conn, Keys, Ops) of
        {ok, []} ->
            [];
        {ok, [{0, Objects}]} ->
            Objects
    end.

assert_valid_key({Bucket, Key}) when is_binary(Bucket), is_binary(Key) ->
    ok.

%% @doc Count entries (bounds are inclusive).
%% TODO: use map-reduce here
get_key_count_between(Conn, SecIndex, LBound, UBound) when LBound =< UBound ->
    {ok, ?INDEX_RESULTS{keys=Keys}} =
    riakc_pb_socket:get_index_range(Conn, message_bucket(), SecIndex, LBound, UBound),
    length(Keys);
get_key_count_between(_, _, _, _) ->
    0.


maybe_get_hour_key_count_from(_RQ, _Digest, undefined) ->
    0;
maybe_get_hour_key_count_from(RQ, Digest, Microseconds) ->
    get_hour_key_count_from(RQ, Digest, Microseconds).

get_hour_key_count_from(RQ, Digest, Microseconds) ->
    case dig_is_skipped(hour(Microseconds), Digest) of
    true -> 0;
    false -> request_hour_key_count_from(RQ, Microseconds)
    end.

request_hour_key_count_from(RQ, Microseconds) ->
    LBound = microseconds_to_min_mess_id(Microseconds),
    UBound = hour_to_max_mess_id(hour(Microseconds)),
    request_key_count(set_bounds(LBound, UBound, RQ)).

get_hour_key_count_to(RQ, Digest, Microseconds) ->
    case dig_is_skipped(hour(Microseconds), Digest) of
    true -> 0;
    false -> request_hour_key_count_to(RQ, Microseconds)
    end.

request_hour_key_count_to(RQ, Microseconds) ->
    LBound = hour_to_min_mess_id(hour(Microseconds)),
    UBound = microseconds_to_max_mess_id(Microseconds),
    request_key_count(set_bounds(LBound, UBound, RQ)).


maybe_get_hour_key_count_after(_RQ, _Digest, undefined) ->
    0;
maybe_get_hour_key_count_after(RQ, Digest, Microseconds) ->
    get_hour_key_count_after(RQ, Digest, Microseconds).

get_hour_key_count_after(RQ, Digest, Microseconds) ->
    case dig_is_skipped(hour(Microseconds), Digest) of
    true -> 0;
    false -> request_hour_key_count_after(RQ, Microseconds)
    end.

request_hour_key_count_after(RQ, Microseconds) ->
    case hour(Microseconds) =:= hour(Microseconds+1) of
    %% Microseconds has a zero hour offset.
    false -> 0;
    true ->
        LBound = microseconds_to_min_mess_id(Microseconds+1),
        UBound = hour_to_max_mess_id(hour(Microseconds)),
        request_key_count(set_bounds(LBound, UBound, RQ))
    end.

get_hour_key_count_before(RQ, Digest, Microseconds) ->
    case dig_is_skipped(hour(Microseconds), Digest) of
    true -> 0;
    false -> request_hour_key_count_before(RQ, Microseconds)
    end.

request_hour_key_count_before(RQ, Microseconds) ->
    case hour(Microseconds) =:= hour(Microseconds-1) of
    %% Microseconds has a zero hour offset.
    false -> 0;
    true ->
        LBound = hour_to_min_mess_id(hour(Microseconds)),
        UBound = microseconds_to_max_mess_id(Microseconds-1),
        request_key_count(set_bounds(LBound, UBound, RQ))
    end.

get_hour_key_count(RQ, Hour) ->
    LBound = hour_to_min_mess_id(Hour),
    UBound = hour_to_max_mess_id(Hour),
    request_key_count(set_bounds(LBound, UBound, RQ)).

get_recent_key_count(RQ, Digest, End) ->
    LastKnownHour = dig_last_hour(Digest),
    LBound = hour_to_min_mess_id(LastKnownHour + 1),
    UBound = maybe_microseconds_to_max_mess_id(End),
    request_key_count(set_bounds(LBound, UBound, RQ)).

get_recent_keys(RQ, Digest, End) ->
    LastKnownHour = dig_last_hour(Digest),
    LBound = hour_to_min_mess_id(LastKnownHour + 1),
    UBound = maybe_microseconds_to_max_mess_id(End),
    request_keys(set_bounds(LBound, UBound, RQ)).

get_old_keys(RQ, Now) ->
    UHour = hour(Now) - 1,
    LBound = undefined_bound(),
    UBound = hour_to_max_mess_id(UHour),
    request_keys(set_bounds(LBound, UBound, RQ)).

get_key_range(Conn, SecIndex, LBound, UBound)
    when is_binary(LBound), is_binary(UBound) ->
    get_key_range(Conn, message_bucket(), SecIndex, LBound, UBound).

get_key_range(Conn, Bucket, SecIndex, LBound, UBound)
    when is_binary(LBound), is_binary(UBound) ->
    {ok, ?INDEX_RESULTS{keys=Keys}} =
    riakc_pb_socket:get_index_range(Conn, Bucket, SecIndex, LBound, UBound),
    Keys.

get_object_range(Conn, Bucket, SecIndex, LBound, UBound)
    when is_binary(LBound), is_binary(UBound) ->
    Input = {index,Bucket,SecIndex,LBound,UBound},
    Ops = [{map, {modfun, riak_kv_mapreduce, map_identity}, undefined, true}],
    case riakc_pb_socket:mapred(Conn, Input, Ops) of
        {ok, []} ->
            [];
        {ok, [{0, Objects}]} ->
            Objects
    end.


get_minimum_key_range_before(RQ, Digest, Before, PageSize) ->
    MinHour = page_minimum_hour(PageSize, Digest),
    LBound = hour_to_min_mess_id(MinHour),
    UBound = maybe_microseconds_to_max_mess_id(Before),
    request_keys(set_bounds(LBound, UBound, RQ)).

%% Expected start hour of the page
page_minimum_hour(PageSize, Digest) ->
    %% Skip `PageSize' elements from the end.
    {_, MinDigest} = dig_skip_nr(PageSize, Digest),
    case dig_is_empty(MinDigest) of
        true -> dig_first_hour(Digest);
        false -> dig_last_hour(MinDigest)
    end.

%% Expected end hour of the page
%% This hour will be the end of the page in the worst-case scenario.
page_maximum_hour(PageSize, [{_, Cnt}|Digest = [_|_]]) when PageSize > Cnt ->
    page_maximum_hour(PageSize - Cnt, Digest);
page_maximum_hour(_, [{Hour, _}|_]) ->
    %% Tail is empty or PageSize =< Hour
    Hour.

%% @doc Skip 1 hour and evaluate `page_maximum_hour'.
tail_page_maximum_hour(PageSize, [_,_|_] = PageDigest) ->
    page_maximum_hour(PageSize, dig_tail(PageDigest));
tail_page_maximum_hour(_PageSize, [{Hour, _}]) ->
    Hour.

-spec get_message_rows(term(), [binary()]) -> list(message_row()).
get_message_rows(Conn, Keys) ->
    FullKeys = [{message_bucket(), K} || K <- Keys],
    Values = to_values(Conn, FullKeys),
    MessageRows = deserialize_values(Values),
    lists:usort(MessageRows).



-ifdef(TEST).

%% + 2 hours
test_now() -> mod_mam_utils:now_to_microseconds(now()) + 7200 * 1000000.


%% Virtual timer
get_now() ->
    case ets:lookup(test_vars, mocked_now) of
        [] -> mod_mam_utils:now_to_microseconds(now());
        [{_, V}] -> V
    end.

set_now(Microseconds) when is_integer(Microseconds) ->
    ets:insert(test_vars, {mocked_now, Microseconds}).

reset_now() ->
    ets:delete(test_vars, mocked_now).


meck_test_() ->
    {inorder,
    [{"Checking, that digest update will be triggered.",
      {setup,
       fun() -> load_mock(0) end,
       fun(_) -> unload_mock() end,
       safe_generator(fun digest_update_case/0)}},

     {"With recent messages by index.",
      {setup,
       fun() -> load_mock(0) end,
       fun(_) -> unload_mock() end,
       safe_generator(fun index_nothing_from_digest_case/0)}},

     {"Paginate by index without digest (it is not yet generated).",
      {setup,
       fun() -> load_mock(0) end,
       fun(_) -> unload_mock() end,
       safe_generator(fun index_pagination_case/0)}},

     {"Paginate by index. All entries are in digest. "
      "PageDigest begins in the middle of an hour.",
      {setup,
       fun() -> load_mock(0) end,
       fun(_) -> unload_mock() end,
       safe_generator(fun only_from_digest_non_empty_hour_offset_case/0)}},

     {"From proper.",
      {setup,
       fun() -> load_mock(0) end,
       fun(_) -> unload_mock() end,
       safe_generator(fun proper_case/0)}},

     {"Try to purge multiple messages (lower bounded).",
      {setup,
       fun() -> load_mock(0) end,
       fun(_) -> unload_mock() end,
       safe_generator(fun purge_multiple_messages_lower_bounded_case/0)}},

     {"Try to purge multiple messages (upper bounded).",
      {setup,
       fun() -> load_mock(0) end,
       fun(_) -> unload_mock() end,
       safe_generator(fun purge_multiple_messages_upper_bounded_case/0)}},

     {"Index=6, PageSize=1, Start and End defined.",
      {setup,
       fun() -> load_mock(0) end,
       fun(_) -> unload_mock() end,
       safe_generator(fun index_bounded_last_page_case/0)}},

     {"Index = 0, PageSize = 0, Start and End are defined.",
      {setup,
       fun() -> load_mock(0) end,
       fun(_) -> unload_mock() end,
       safe_generator(fun bounded_first_page_case/0)}},

     {"Index=0, PageSize=1, End is defined.",
      {setup,
       fun() -> load_mock(0) end,
       fun(_) -> unload_mock() end,
       safe_generator(fun index_upper_bounded_last_page_case/0)}},

     {"Index = 2, PageSize = 1, Start is defined.",
      {setup,
       fun() -> load_mock(0) end,
       fun(_) -> unload_mock() end,
       safe_generator(fun index_upper_bounded_last_page2_case/0)}},

     {"Selecting messages from a tiny date range. RSet is empty.",
      {setup,
       fun() -> load_mock(0) end,
       fun(_) -> unload_mock() end,
       safe_generator(fun tiny_empty_date_range_lookup_case/0)}},

     {"Selecting messages from a tiny date range. RSet is empty (2).",
      {setup,
       fun() -> load_mock(0) end,
       fun(_) -> unload_mock() end,
       safe_generator(fun tiny_empty_date_range_lookup2_case/0)}},

     {"Selecting messages from a tiny date range. RSet is not empty (3).",
      {setup,
       fun() -> load_mock(0) end,
       fun(_) -> unload_mock() end,
       safe_generator(fun tiny_not_empty_date_range_lookup_case/0)}},

     {"Purge a single message.",
      {setup,
       fun() -> load_mock(0) end,
       fun(_) -> unload_mock() end,
       safe_generator(fun purge_single_message_case/0)}},

     {"Purge a single message and erase its digest.",
      {setup,
       fun() -> load_mock(0) end,
       fun(_) -> unload_mock() end,
       safe_generator(fun purge_single_message_digest_case/0)}},

     {"Purge a single message and erase its digest. PageSize = 0.",
      {setup,
       fun() -> load_mock(0) end,
       fun(_) -> unload_mock() end,
       safe_generator(fun purge_single_message_digest_zero_page_size_case/0)}},

     {"RSetDigest is not empty, PageDigest is empty, KeyPos is inside.",
      {setup,
       fun() -> load_mock(0) end,
       fun(_) -> unload_mock() end,
       safe_generator(fun after_last_page_case/0)}},

     {"Without index digest.",
      {setup,
       fun() -> load_mock(10000), load_data() end,
       fun(_) -> unload_mock() end,
       {timeout, 60, safe_generator(fun long_case/0)}}},
     {"With index digest.",
      {setup,
       fun() -> load_mock(0), load_data() end,
       fun(_) -> unload_mock() end,
       {timeout, 60, safe_generator(fun long_case/0)}}},
     {"Without index digest.",
      {setup,
       fun() -> load_mock(10000), load_data2() end,
       fun(_) -> unload_mock() end,
       {timeout, 60, safe_generator(fun short_case/0)}}},
     {"With index digest.",
      {setup,
       fun() -> load_mock(0), load_data2() end,
       fun(_) -> unload_mock() end,
       {timeout, 60, safe_generator(fun short_case/0)}}},
     {"Without index digest.",
      {setup,
       fun() -> load_mock(10000), load_data3() end,
       fun(_) -> unload_mock() end,
       {timeout, 60, safe_generator(fun incremental_pagination_case/0)}}},
     {"With index digest.",
      {setup,
       fun() -> load_mock(0), load_data3() end,
       fun(_) -> unload_mock() end,
       {timeout, 60, safe_generator(fun incremental_pagination_case/0)}}},
     {"Without index digest.",
      {setup,
       fun() -> load_mock(10000), load_data3() end,
       fun(_) -> unload_mock() end,
       {timeout, 60, safe_generator(fun decremental_pagination_case/0)}}},
     {"With index digest.",
      {setup,
       fun() -> load_mock(0), load_data3() end,
       fun(_) -> unload_mock() end,
       {timeout, 60, safe_generator(fun decremental_pagination_case/0)}}}
    ]}.

all_keys(Bucket) ->
    MS = [{
            {{Bucket, '$1'}, '_'},
            [],
            ['$1']
          }],
    ets:select(riak_store, MS).

load_mock(DigestThreshold) ->
    GM = gen_mod,
    meck:new(GM),
    meck:expect(GM, get_module_opt, fun(_, mod_mam, digest_creation_threshold, _) ->
        DigestThreshold
        end),
    SM = riakc_pb_socket, %% webscale is here!
    Tab = ets:new(riak_store, [public, ordered_set, named_table]),
    IdxTab = ets:new(riak_index, [public, ordered_set, named_table]),
    ets:new(test_vars, [public, ordered_set, named_table]),
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
        (Conn, Bucket, Index, LBound, UBound) when is_tuple(Index) ->
            %% fun({SecIndexKey, {_, Key}})
            %%     when SecIndexKey >= {Index, Bucket, LBound}
            %%          SecIndexKey =< {Index, Bucket, UBound} -> Key end
            MS = [{
                    {'$1', {'_', '$2'}},
                    [{'>=', '$1', {{{Index}, Bucket, LBound}}},
                     {'=<', '$1', {{{Index}, Bucket, UBound}}}],
                    ['$2']
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
            end;
        [{map, {modfun, riak_kv_mapreduce, map_identity}, undefined, true}] ->
            case [Obj
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
    meck:expect(SM, put, fun(Conn, Obj) when is_list(Obj) ->
        Key    = proplists:get_value(key, Obj),
        Bucket = proplists:get_value(bucket, Obj),
        Md     = proplists:get_value(metadata, Obj, []),
        Md1    = proplists:delete(<<"X-Riak-Last-Modified">>, Md),
        Now    = mod_mam_utils:microseconds_to_now(get_now()),
        Md2    = [{<<"X-Riak-Last-Modified">>, Now}|Md1],
        Is     = proplists:get_value(secondary_index, Md, []),
        Obj1   = [{metadata, Md2}|proplists:delete(metadata, Obj)],
        ets:insert(Tab, {{Bucket, Key}, Obj1}),
        ets:insert(IdxTab, [{{IdxName, Bucket, SecKey}, {Bucket, Key}}
                            || {IdxName, SecKeys} <- Is, SecKey <- SecKeys]),
        ok
        end),
    meck:expect(SM, get, fun(Conn, Bucket, Key) ->
        case ets:lookup(Tab, {Bucket, Key}) of
            [{_,Obj}] -> {ok, Obj};
            []        -> {error, notfound}
        end
        end),
    meck:expect(SM, delete, fun(Conn, Bucket, Key) ->
        ets:delete(Tab, {Bucket, Key}),
        ets:match_delete(IdxTab, {'_', {Bucket, Key}}),
        ok
        end),
    meck:expect(SM, delete_obj, fun(Conn, Obj) ->
        Key = proplists:get_value(key, Obj),
        Bucket = proplists:get_value(bucket, Obj),
        ets:delete(Tab, {Bucket, Key}),
        ets:match_delete(IdxTab, {'_', {Bucket, Key}}),
        ok
        end),
    OM = riakc_obj,
    meck:new(OM),
    meck:expect(OM, new, fun(Bucket, Key, Value) ->
        Md = [],
        [{bucket, Bucket}, {key, Key}, {value, Value}, {metadata, Md}]
        end),
    meck:expect(OM, key, fun(Obj) ->
        proplists:get_value(key, Obj, [])
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
    meck:expect(OM, get_secondary_index, fun(Md, IdxName) ->
        Idx = proplists:get_value(secondary_index, Md, []),
        proplists:get_value(IdxName, Idx)
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
    reset_now(),
    catch ets:delete(test_vars),
    catch ets:delete(riak_store),
    catch ets:delete(riak_index),
    meck:unload(gen_mod),
    meck:unload(riakc_pb_socket),
    meck:unload(riakc_obj),
    meck:unload(riak_pool),
    meck:unload(mod_mam_cache),
    ok.

reset_mock() ->
    reset_now(),
    ets:match_delete(test_vars, '_'),
    ets:match_delete(riak_store, '_'),
    ets:match_delete(riak_index, '_'),
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
    Log1Id = fun(Time) ->
        Date = iolist_to_binary("2000-07-21T" ++ Time ++ "Z"),
        id(Date)
        end,
    Log2Id = fun(Time) ->
        Date = iolist_to_binary("2000-07-22T" ++ Time ++ "Z"),
        id(Date)
        end,

    %% lookup_messages(UserJID, RSM, Start, End, Now, WithJID,
    %%                 PageSize, LimitPassed, MaxResultLimit) ->
    %% {ok, {TotalCount, Offset, MessageRows}}

    [
    {"Trigger digest creation.",
    ?_assertKeys(34, 0, [],
                lookup_messages(alice(), undefined,
                    undefined, undefined, test_now(), undefined,
                    0, true, 0))},

    {"First 5.",
    ?_assertKeys(34, 0,
                [join_date_time("2000-07-21", Time)
                 || Time <- ["01:50:00", "01:50:05", "01:50:15",
                             "01:50:16", "01:50:17"]],
                lookup_messages(alice(),
                    undefined, undefined, undefined, test_now(), undefined,
                    5, true, 5))},

    {"Last 5.",
    ?_assertKeys(34, 29,
                [join_date_time("2000-07-22", Time)
                 || Time <- ["06:50:40", "06:50:50", "06:51:00",
                             "06:51:10", "06:51:15"]],
                lookup_messages(alice(),
                    #rsm_in{direction = before},
                    undefined, undefined, test_now(), undefined,
                    5, true, 5))},

    {"Last 5 with a lower bound.",
    %% lower_bounded
    ?_assertKeys(12, 7,
                [join_date_time("2000-07-22", Time)
                 || Time <- ["06:50:40", "06:50:50", "06:51:00",
                             "06:51:10", "06:51:15"]],
                lookup_messages(alice(),
                    #rsm_in{direction = before},
                    to_microseconds("2000-07-22", "06:49:45"),
                    undefined, test_now(), undefined,
                    5, true, 5))},

    {"Last 5 from both conversations with a lower bound.",
    %% lower_bounded
    ?_assertKeys(24, 19,
                [join_date_time("2000-07-22", Time)
                 || Time <- ["06:50:40", "06:50:50", "06:51:00",
                             "06:51:10", "06:51:15"]],
                lookup_messages(alice(),
                    #rsm_in{direction = before},
                    to_microseconds("2000-07-21", "01:50:50"),
                    undefined, test_now(), undefined,
                    5, true, 5))},

    {"Last 5 from both conversations with an upper bound.",
    %% upper_bounded
    ?_assertKeys(28, 23,
                [join_date_time("2000-07-22", Time)
                 || Time <- ["06:49:50", "06:50:05", "06:50:10",
                             "06:50:25", "06:50:28"]],
                lookup_messages(alice(),
                    #rsm_in{direction = before}, undefined,
                    to_microseconds("2000-07-22", "06:50:29"),
                    test_now(), undefined,
                    5, true, 5))},

    {"Index 3.",
    ?_assertKeys(34, 3,
                [join_date_time("2000-07-21", Time)
                 || Time <- ["01:50:16", "01:50:17", "01:50:20",
                             "01:50:25", "01:50:30"]],
                lookup_messages(alice(),
                    #rsm_in{index = 3},
                    undefined, undefined, test_now(), undefined,
                    5, true, 5))},

    {"Index 30 (at the end).",
    ?_assertKeys(34, 30,
                [join_date_time("2000-07-22", Time)
                 || Time <- ["06:50:50", "06:51:00",
                             "06:51:10", "06:51:15"]],
                lookup_messages(alice(),
                    #rsm_in{index = 30},
                    undefined, undefined, test_now(), undefined,
                    5, true, 5))},

    %% RSet timestamps:
    %% "06:49:05", "06:49:10", "06:49:15", "06:49:20", "06:49:25",
    %% "06:49:30", "06:49:36", "06:49:45", "06:49:50", "06:50:05"
    {"Index 0 in the range (HourOffset is not empty).",
    ?_assertKeys(10, 0,
                [join_date_time("2000-07-22", Time)
                 || Time <- ["06:49:05", "06:49:10", "06:49:15",
                             "06:49:20", "06:49:25"]],
                lookup_messages(alice(),
                    #rsm_in{index = 0},
                    to_microseconds("2000-07-22", "06:49:05"),
                    to_microseconds("2000-07-22", "06:50:05"),
                    test_now(),
                    undefined, 5, true, 5))},

    {"Index 3 in the range (HourOffset is not empty).",
    ?_assertKeys(10, 3,
                [join_date_time("2000-07-22", Time)
                 || Time <- ["06:49:20", "06:49:25",
                             "06:49:30", "06:49:36", "06:49:45"]],
                lookup_messages(alice(),
                    #rsm_in{index = 3},
                    to_microseconds("2000-07-22", "06:49:05"),
                    to_microseconds("2000-07-22", "06:50:05"),
                    test_now(),
                    undefined, 5, true, 5))},

    {"Index 7 in the range (HourOffset is not empty). "
     "This page is last and not full.",
    ?_assertKeys(10, 7,
                [join_date_time("2000-07-22", Time)
                 || Time <- ["06:49:45", "06:49:50", "06:50:05"]],
                lookup_messages(alice(),
                    #rsm_in{index = 7},
                    to_microseconds("2000-07-22", "06:49:05"),
                    to_microseconds("2000-07-22", "06:50:05"),
                    test_now(),
                    undefined, 5, true, 5))},

    {"Last 5 from the range.",
    %% bounded
    ?_assertKeys(6, 1,
                [join_date_time("2000-07-22", Time)
                 || Time <- ["06:49:50", "06:50:05", "06:50:10",
                             "06:50:25", "06:50:28"]],
                lookup_messages(alice(),
                    #rsm_in{direction = before},
                    to_microseconds("2000-07-22", "06:49:45"),
                    to_microseconds("2000-07-22", "06:50:28"),
                    test_now(), undefined,
                    5, true, 5))},

   {"Return a page of 5 elements before N from the range.",
    %% S B E, RSetDigest is empty, PageDigest is empty.
    ?_assertKeys(13, 5,
                [join_date_time("2000-07-22", Time)
                 || Time <- ["06:49:30", "06:49:36", "06:49:45",
                             "06:49:50", "06:50:05"]],
                lookup_messages(alice(),
                    #rsm_in{direction = before, id = Log2Id("06:50:10")},
                    to_microseconds("2000-07-22", "06:49:05"),
                    to_microseconds("2000-07-22", "06:50:28"),
                    test_now(), undefined,
                    5, true, 5))},

   {"Return a page of 5 elements before N from the range "
    "filtered by jid with resource.",
    %% S B E, RSetDigest is empty, PageDigest is empty.
    ?_assertKeys(2, 0,
                [join_date_time("2000-07-22", Time)
                 %% "06:49:30" is skipped (it is from Dormouse).
                 %% All messages from Alice will be skipped
                 %% (they are addressed to the whole room).
                 || Time <- ["06:49:45", "06:50:05"]],
                lookup_messages(alice(),
                    #rsm_in{direction = before, id = Log2Id("06:50:10")},
                    to_microseconds("2000-07-22", "06:49:05"),
                    to_microseconds("2000-07-22", "06:50:28"),
                    test_now(), hatter_at_party(),
                    5, true, 5))}
    ].

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
    Id = fun(Date, Time) ->
        id(iolist_to_binary(join_date_time_z(Date, Time)))
        end,
    %% The key is shrinked here (empty user-id).
    Key = fun(Date, Time) -> mess_id_to_binary(Id(Date, Time)) end,
    WholeDigest = [
        {date_to_hour("2000-07-21", "13:00:00"), 4},
        {date_to_hour("2000-07-22", "15:00:00"), 1},
        {date_to_hour("2000-07-22", "16:00:00"), 1},
        {date_to_hour("2000-07-23", "10:00:00"), 2}
    ],
    [{"Trigger digest creation.",
    ?_assertKeys(9, 0, [],
                lookup_messages(alice(), undefined,
                    undefined, undefined, test_now(), undefined,
                    0, true, 0))},

    {"Index 0, PageSize 0.",
    %% count_only
    ?_assertKeys(9, 0, [],
                lookup_messages(alice(),
                    #rsm_in{index = 0},
                    undefined, undefined, test_now(), undefined,
                    0, true, 0))},

    {"Index 0.",
    %% only_from_digest
    ?_assertKeys(9, 0,
                ["2000-07-21T13:10:03", "2000-07-21T13:10:10",
                 "2000-07-21T13:10:15", "2000-07-21T13:10:16",
                 "2000-07-22T15:59:59"],
                lookup_messages(alice(),
                    #rsm_in{index = 0},
                    undefined, undefined, test_now(), undefined,
                    5, true, 5))},

    {"Index = TotalCount.",
    %% nothing_from_digest
    ?_assertKeys(9, 9, [],
                lookup_messages(alice(),
                    #rsm_in{index = 9},
                    undefined, undefined, test_now(), undefined,
                    5, true, 5))},

    {"Offset > TotalCount.",
    %% nothing_from_digest
    ?_assertKeys(9, 100, [],
                lookup_messages(alice(),
                    #rsm_in{index = 100},
                    undefined, undefined, test_now(), undefined,
                    5, true, 5))},

    {"Last 2 from the range.",
    %% bounded
    ?_assertKeys(2, 0,
                [join_date_time("2000-07-22", Time)

                 || Time <- ["15:59:59", "16:00:05"]],
                lookup_messages(alice(),
                    #rsm_in{direction = before},
                    to_microseconds("2000-07-21", "13:30:00"),
                    to_microseconds("2000-07-23", "10:30:00"),
                    test_now(), undefined,
                    2, true, 2))},
    {"Partically full page from the range.",
    %% bounded
    ?_assertKeys(3, 0,
                ["2000-07-21T13:10:16",
                 "2000-07-22T15:59:59",
                 "2000-07-22T16:00:05"],
                lookup_messages(alice(),
                    #rsm_in{direction = before},
                    to_microseconds("2000-07-21", "13:10:16"),
                    to_microseconds("2000-07-23", "10:30:00"),
                    test_now(), undefined,
                    5, true, 5))},


   {"Return a page of 2 elements before N from the range.",
    %% S B E, RSetDigest is not empty, PageDigest is not empty.
    %% first_page
    ?_assertKeys(7, 2,
                ["2000-07-21T13:10:16",
                 "2000-07-22T15:59:59"],
                lookup_messages(alice(),
                    #rsm_in{direction = before,
                            id = Id("2000-07-22", "16:00:05")},
                    to_microseconds("2000-07-21", "13:10:10"),
                    to_microseconds("2000-07-23", "10:34:20"),
                    test_now(), undefined,
                    2, true, 2))},

   {"Return a page of 2 elements before N from the range (empty page digest).",
    %% first_page
    %% S B E, RSetDigest is not empty, PageDigest is empty.
    %% "2000-07-21T13:10:10", "2000-07-21T13:10:15", "2000-07-22T13:10:16",
    %% "2000-07-22T15:59:59", "2000-07-23T16:00:05", "2000-07-23T10:34:04",
    %% "2000-07-23T10:34:20"
    ?_assertKeys(7, 1,
                ["2000-07-21T13:10:15",
                 "2000-07-21T13:10:16"],
                lookup_messages(alice(),
                    #rsm_in{direction = before,
                            id = Id("2000-07-22", "15:59:59")},
                    to_microseconds("2000-07-21", "13:10:10"),
                    to_microseconds("2000-07-23", "10:34:20"),
                    test_now(), undefined,
                    2, true, 2))},

    {"Return a last page before a known entry " %% it looks impossible.
     "(B > E. There are no entries after B or E).",
    %% last_page
    ?_assertKeys(9, 4,
                ["2000-07-22T15:59:59", "2000-07-22T16:00:05",
                 "2000-07-23T10:34:04", "2000-07-23T10:34:20",
                 "2000-07-23T10:34:23"],
                lookup_messages(alice(),
                    #rsm_in{direction = before,
                            id = Id("2000-07-28", "00:00:00")},
                    undefined,
                    to_microseconds("2000-07-27", "00:00:00"),
                    test_now(), undefined,
                    5, true, 5))},

    {"Return an empty page before a known entry. B < S.",
    %% count_only
    ?_assertKeys(9, 0,
                [],
                lookup_messages(alice(),
                    #rsm_in{direction = before,
                            id = Id("2000-07-20", "00:00:00")},
                    undefined, undefined, test_now(), undefined,
                    5, true, 5))},


    {"After id (last page, part of messages are in RSetDigest).",
    % key_position(Key, Start, End, Digest)
    [?_assertEqual(inside, key_position(Key("2000-07-22", "15:59:59"),
                     undefined, undefined, WholeDigest)),
    %% after_last_page
    ?_assertKeys(9, 5,
                ["2000-07-22T16:00:05", "2000-07-23T10:34:04",
                 "2000-07-23T10:34:20", "2000-07-23T10:34:23"],
                lookup_messages(alice(),
                    #rsm_in{direction = aft,
                            id = Id("2000-07-22", "15:59:59")},
                    undefined, undefined, test_now(), undefined,
                    5, true, 5))]},

    {"After id (last page, only recent entries).",
    [?_assertEqual(inside, key_position(Key("2000-07-23", "10:34:23"),
                     undefined, undefined, WholeDigest)),
    %% after_recent_only
    ?_assertKeys(9, 7,
                ["2000-07-23T10:34:20", "2000-07-23T10:34:23"],
                lookup_messages(alice(),
                    #rsm_in{direction = aft,
                            id = Id("2000-07-23", "10:34:04")},
                    undefined, undefined, test_now(), undefined,
                    5, true, 5))]},

    {"After id with upper bound (last page, only recent entries).",
    %% after_recent_only
    ?_assertKeys(9, 7,
                ["2000-07-23T10:34:20", "2000-07-23T10:34:23"],
                lookup_messages(alice(),
                    #rsm_in{direction = aft,
                            id = Id("2000-07-23", "10:34:04")},
                    undefined,
                    to_microseconds("2000-07-23", "10:34:24"),
                    test_now(), undefined,
                    5, true, 5))},

    {"After id, PageSize = 2 (last page, only recent entries).",
    [?_assertEqual(inside, key_position(Key("2000-07-21", "13:10:10"),
                     undefined, undefined, WholeDigest)),
    %% after_middle_page
    ?_assertKeys(9, 2,
                ["2000-07-21T13:10:15", "2000-07-21T13:10:16"],
                lookup_messages(alice(),
                    #rsm_in{direction = aft,
                            id = Id("2000-07-21", "13:10:10")},
                    undefined, undefined, test_now(), undefined,
                    2, true, 2))]}
    ].


next_random_mess_id(MessId) ->
    %% 256 000 000 is one second.
    MessId + 256 * random_microsecond_delay().

random_microsecond_delay() ->
    %% One hour is the maximim delay.
    random:uniform(3600000000).

load_data3() ->
    FirstMessId = 352159376404720897,
    random:seed({1,2,3}),
    put_messages(FirstMessId, 100).


put_messages(MessId, N) when N > 0 ->
    Text = integer_to_list(N),
    Packet = message(iolist_to_binary(Text)),
    case random_destination() of
        outgoing ->
            ?MODULE:archive_message(
                MessId, outgoing, alice(), cat(), alice(), Packet);
        incoming ->
            ?MODULE:archive_message(
                MessId, incoming, alice(), cat(), cat(), Packet)
    end,
    [MessId|put_messages(next_random_mess_id(MessId), N - 1)];
put_messages(_, 0) ->
    [].

random_destination() ->
    case random:uniform(2) of
        1 -> incoming;
        2 -> outgoing
    end.

incremental_pagination_case() ->
    MessIDs = [key_to_mess_id(Key) || Key <- all_keys(message_bucket())],
    TotalCount = length(MessIDs),
    [{"Trigger digest creation.",
    ?_assertKeys(TotalCount, 0, [],
                lookup_messages(alice(), undefined,
                    undefined, undefined, test_now(), undefined,
                    0, true, 0))},
     %% 0 id is before any message id in the archive.
     incremental_pagination_gen(0, 1, 10, 0, MessIDs, TotalCount)].

incremental_pagination_gen(
    LastMessID, PageNum, PageSize, Offset, MessIDs, TotalCount)
    when is_integer(LastMessID) ->
    {ok, {ResultTotalCount, ResultOffset, ResultRows}} =
    ?MODULE:lookup_messages(alice(),
        #rsm_in{direction = aft, id = LastMessID},
        undefined, undefined, test_now(), undefined,
        PageSize, true, PageSize),
    ResultMessIDs = [message_row_to_mess_id(Row) || Row <- ResultRows],
    {PageMessIDs, LeftMessIDs} = safe_split(PageSize, MessIDs),
    NewOffset = Offset + length(ResultRows),
    NewLastMessID = lists:last(PageMessIDs),
    [{"Page " ++ integer_to_list(PageNum),
     [?_assertEqual(TotalCount, ResultTotalCount),
      ?_assertEqual(Offset, ResultOffset),
      ?_assertEqual(PageMessIDs, ResultMessIDs)]}
    | case NewOffset >= TotalCount of
        true -> [];
        false ->
            safe_generator(fun() -> incremental_pagination_gen(
                NewLastMessID, PageNum + 1, PageSize,
                NewOffset, LeftMessIDs,  TotalCount)
             end)
      end].

decremental_pagination_case() ->
    MessIDs = [key_to_mess_id(Key) || Key <- all_keys(message_bucket())],
    TotalCount = length(MessIDs),
    PageSize = 10,
    PageNum = TotalCount div PageSize,
    Offset = TotalCount - PageSize,
    [{"Trigger digest creation.",
    ?_assertKeys(TotalCount, 0, [],
                lookup_messages(alice(), undefined,
                    undefined, undefined, test_now(), undefined,
                    0, true, 0))},
     decremental_pagination_gen(
            undefined, PageNum, PageSize, Offset, MessIDs, TotalCount)].

decremental_pagination_gen(
    BeforeMessID, PageNum, PageSize, Offset, MessIDs, TotalCount) ->
    {ok, {ResultTotalCount, ResultOffset, ResultRows}} =
    ?MODULE:lookup_messages(alice(),
        #rsm_in{direction = before, id = BeforeMessID},
        undefined, undefined, test_now(), undefined,
        PageSize, true, PageSize),
    ResultMessIDs = [message_row_to_mess_id(Row) || Row <- ResultRows],
    {LeftMessIDs, PageMessIDs} = safe_split(Offset, MessIDs),
    NewOffset = Offset - length(ResultRows),
    NewBeforeMessID = hd(PageMessIDs),
    [{"Page " ++ integer_to_list(PageNum),
     [?_assertEqual(TotalCount, ResultTotalCount),
      ?_assertEqual(Offset, ResultOffset),
      ?_assertEqual(PageMessIDs, ResultMessIDs)]}
    | case NewOffset =< 0 of
        true -> [];
        false ->
            safe_generator(fun() -> decremental_pagination_gen(
                NewBeforeMessID, PageNum - 1, PageSize,
                NewOffset, LeftMessIDs,  TotalCount)
             end)
      end].


digest_update_case() ->
    %% EUnit calls any generator twice.
    %% This generator saves new messages, that is why
    %% the generator will produce different results after each call.
    reset_mock(),
    Id = fun(Date) -> id(list_to_binary(Date)) end,
    %% 13:30
    set_now(to_microseconds("2000-01-01", "13:30:00")),
    archive_message(Id("2000-01-01T13:30:00Z"),
        outgoing, alice(), cat(), alice(), packet()),
    %% 13:40
    set_now(to_microseconds("2000-01-01", "13:40:00")),
    Generators1 =
    assert_keys(1, 0, ["2000-01-01T13:30:00"],
        lookup_messages(alice(), undefined, undefined, undefined,
            to_microseconds("2000-01-01", "13:40:00"), undefined, 10, true, 10)),
    %% 13:50
    set_now(to_microseconds("2000-01-01", "13:50:00")),
    archive_message(Id("2000-01-01T13:50:00Z"),
        outgoing, alice(), cat(), alice(), packet()),
    %% 14:30
    set_now(to_microseconds("2000-01-01", "14:30:00")),
    Generators2 =
    assert_keys(2, 0, ["2000-01-01T13:30:00", "2000-01-01T13:50:00"],
        lookup_messages(alice(), undefined, undefined, undefined,
            to_microseconds("2000-01-01", "14:30:00"), undefined, 10, true, 10)),
    Generators1 ++ Generators2.

index_nothing_from_digest_case() ->
    %% nothing_from_digest
    reset_mock(),
    Id = fun(Date) -> id(list_to_binary(Date)) end,
    %% 13:30
    set_now(to_microseconds("2000-01-01", "13:30:00")),
    archive_message(Id("2000-01-01T13:30:00Z"),
        outgoing, alice(), cat(), alice(), packet()),
    %% 13:40
    set_now(to_microseconds("2000-01-01", "13:40:00")),
    Generators1 =
    assert_keys(1, 0, ["2000-01-01T13:30:00"],
        lookup_messages(alice(), undefined, undefined, undefined,
            to_microseconds("2000-01-01", "13:40:00"), undefined, 10, true, 10)),
    %% 13:50
    set_now(to_microseconds("2000-01-01", "13:50:00")),
    archive_message(Id("2000-01-01T13:50:00Z"),
        outgoing, alice(), cat(), alice(), packet()),
    %% 14:30
    set_now(to_microseconds("2000-01-01", "14:30:00")),
    Generators2 =
    assert_keys(2, 5, [],
        lookup_messages(alice(), #rsm_in{index=5}, undefined, undefined,
            to_microseconds("2000-01-01", "14:30:00"), undefined, 10, true, 10)),
    Generators1 ++ Generators2.

index_pagination_case() ->
    reset_mock(),
    set_now(datetime_to_microseconds({{2000,1,1},{0,0,0}})),
    archive_message(id(), incoming, alice(), cat(), cat(), packet()),

    set_now(datetime_to_microseconds({{2000,1,1},{5,13,5}})),
    archive_message(id(), outgoing, alice(), cat(), alice(), packet()),

    set_now(datetime_to_microseconds({{2000,1,1},{5,32,10}})),
    assert_keys(2, 1, ["2000-01-01T05:13:05"],
        lookup_messages(alice(), #rsm_in{index=1}, undefined, undefined,
            get_now(), undefined, 5, true, 5)).

only_from_digest_non_empty_hour_offset_case() ->
    %% only_from_digest
    %% FirstHourOffset is not zero.
    reset_mock(),
    set_now(datetime_to_microseconds({{2000,1,1},{0,0,0}})),
    archive_message(id(), outgoing, cat(), alice(), cat(), packet()),   % 0
    set_now(datetime_to_microseconds({{2000,1,1},{0,50,26}})),
    archive_message(id(), incoming, cat(), alice(), alice(), packet()), % 1
    set_now(datetime_to_microseconds({{2000,1,1},{1,41,1}})),
    archive_message(id(), outgoing, cat(), alice(), cat(), packet()),   % 2
    set_now(datetime_to_microseconds({{2000,1,1},{3,5,26}})),
    archive_message(id(), outgoing, cat(), alice(), cat(), packet()),   % 3
    set_now(datetime_to_microseconds({{2000,1,1},{4,21,20}})),
    lookup_messages(cat(), undefined, undefined, undefined,
        get_now(), undefined, 6, true, 256),
    set_now(datetime_to_microseconds({{2000,1,1},{5,18,1}})),
    archive_message(id(), incoming, cat(), alice(), alice(), packet()), % 4
    set_now(datetime_to_microseconds({{2000,1,1},{6,4,39}})),
    archive_message(id(), outgoing, cat(), alice(), cat(), packet()),   % 5

    set_now(datetime_to_microseconds({{2000,1,1},{6,55,40}})),
    archive_message(id(), outgoing, cat(), alice(), cat(), packet()),   % 6
    set_now(datetime_to_microseconds({{2000,1,1},{8,20,55}})),
    archive_message(id(), outgoing, cat(), alice(), cat(), packet()),   % 7
    set_now(datetime_to_microseconds({{2000,1,1},{15,47,1}})),
    archive_message(id(), outgoing, cat(), alice(), cat(), packet()),
    set_now(datetime_to_microseconds({{2000,1,1},{18,29,24}})),
    archive_message(id(), outgoing, cat(), alice(), cat(), packet()),

    set_now(datetime_to_microseconds({{2000,1,1},{18,47,56}})),
    archive_message(id(), incoming, cat(), alice(), alice(), packet()),
    set_now(datetime_to_microseconds({{2000,1,1},{19,13,5}})),
    assert_keys(11, 6,
        ["2000-01-01T06:55:40", "2000-01-01T08:20:55",
         "2000-01-01T15:47:01", "2000-01-01T18:29:24"],
        lookup_messages(cat(), #rsm_in{index=6}, undefined, undefined,
            get_now(), undefined, 4, true, 256)).

proper_case() ->
    reset_now(),
    [].

purge_multiple_messages_lower_bounded_case() ->
    reset_now(),
    set_now(datetime_to_microseconds({{2000,1,1},{0,41,49}})),
    archive_message(id(), outgoing, alice(), cat(), alice(), packet()),
    set_now(datetime_to_microseconds({{2000,1,1},{1,9,18}})),
    purge_multiple_messages(alice(),
        datetime_to_microseconds({{2000,1,1},{0,0,0}}), undefined,
        get_now(), cat()),
    [].

purge_multiple_messages_upper_bounded_case() ->
    reset_now(),
    set_now(datetime_to_microseconds({{2000,1,1},{0,41,49}})),
    archive_message(id(), outgoing, alice(), cat(), alice(), packet()),
    set_now(datetime_to_microseconds({{2000,1,1},{1,9,18}})),
    purge_multiple_messages(alice(),
        undefined, datetime_to_microseconds({{2000,1,1},{0,0,0}}),
        get_now(), cat()),
    [].

index_bounded_last_page_case() ->
    %% bounded_last_page
    reset_now(),
    set_now(datetime_to_microseconds({{2000,1,1},{0,0,0}})),
    archive_message(id(), outgoing, alice(), cat(), alice(), packet()),
    set_now(datetime_to_microseconds({{2000,1,1},{0,45,22}})),
    archive_message(id(), outgoing, alice(), cat(), alice(), packet()),
    set_now(datetime_to_microseconds({{2000,1,1},{2,16,32}})),
    archive_message(id(), outgoing, alice(), cat(), alice(), packet()),
    set_now(datetime_to_microseconds({{2000,1,1},{2,57,0}})),
    archive_message(id(), incoming, alice(), cat(), cat(), packet()),
    set_now(datetime_to_microseconds({{2000,1,1},{4,16,0}})),
    archive_message(id(), outgoing, alice(), cat(), alice(), packet()),
    set_now(datetime_to_microseconds({{2000,1,1},{4,55,3}})),
    archive_message(id(), outgoing, alice(), cat(), alice(), packet()),
    set_now(datetime_to_microseconds({{2000,1,1},{5,31,3}})),
    archive_message(id(), outgoing, alice(), cat(), alice(), packet()),
    set_now(datetime_to_microseconds({{2000,1,1},{6,4,12}})),
    archive_message(id(), incoming, alice(), cat(), cat(), packet()),
    set_now(datetime_to_microseconds({{2000,1,1},{7,58,47}})),
    lookup_messages(alice(), undefined, undefined, undefined,
        get_now(), undefined, 2, true, 256),
    set_now(datetime_to_microseconds({{2000,1,1},{9,11,18}})),
    assert_keys(8, 6, [],
        lookup_messages(alice(), #rsm_in{index=6},
            datetime_to_microseconds({{2000,1,1},{0,0,0}}),
            datetime_to_microseconds({{2000,1,1},{6,4,12}}),
            get_now(), undefined, 1, true, 256)).

index_upper_bounded_last_page_case() ->
    %% index_upper_bounded_last_page
    reset_now(),
    set_now(datetime_to_microseconds({{2000,1,1},{0,28,25}})),
    archive_message(id(), outgoing, alice(), cat(), alice(), packet()),
    set_now(datetime_to_microseconds({{2000,1,1},{6,8,20}})),
    lookup_messages(alice(), undefined, undefined, undefined,
        get_now(), undefined, 4, true, 256),
    set_now(datetime_to_microseconds({{2000,1,1},{6,51,32}})),
    assert_keys(0, 0, [],
        lookup_messages(alice(), #rsm_in{index=0},
            undefined, datetime_to_microseconds({{2000,1,1},{0,0,0}}),
            get_now(), undefined, 1, true, 256)).

bounded_first_page_case() ->
    %% bounded_first_page
    reset_now(),
    set_now(datetime_to_microseconds({{2000,1,1},{0,46,27}})),
    archive_message(id(), incoming, alice(), cat(), cat(), packet()),
    set_now(datetime_to_microseconds({{2000,1,1},{2,15,6}})),
    lookup_messages(alice(), undefined, undefined, undefined,
        get_now(), undefined, 4, true, 256),
    set_now(datetime_to_microseconds({{2000,1,1},{6,0,37}})),
    assert_keys(0, 0, [],
        lookup_messages(alice(), #rsm_in{index=0},
            datetime_to_microseconds({{2000,1,1},{0,0,0}}) + 14,
            datetime_to_microseconds({{2000,1,1},{0,0,0}}) + 28,
            get_now(), undefined, 0, true, 256)).

index_upper_bounded_last_page2_case() ->
    %% index_upper_bounded_last_page
    reset_mock(),
    set_now(datetime_to_microseconds({{2000,1,1},{0,0,0}})),
    archive_message(id(), outgoing, alice(), cat(), alice(), packet()),
    set_now(datetime_to_microseconds({{2000,1,1},{0,45,34}})),
    archive_message(id(), incoming, alice(), cat(), cat(), packet()),
    set_now(datetime_to_microseconds({{2000,1,1},{0,45,40}})),
    archive_message(id(), incoming, alice(), cat(), cat(), packet()),
    set_now(datetime_to_microseconds({{2000,1,1},{5,23,53}})),
    lookup_messages(alice(), undefined, undefined, undefined,
        get_now(), undefined, 4, true, 256),
    set_now(datetime_to_microseconds({{2000,1,1},{5,56,26}})),
    assert_keys(1, 2, [],
        lookup_messages(alice(), #rsm_in{index=2}, undefined,
            datetime_to_microseconds({{2000,1,1},{0,0,0}}),
            get_now(), undefined, 1, true, 256)).

tiny_empty_date_range_lookup_case() ->
    reset_mock(),
    set_now(datetime_to_microseconds({{2000,1,1},{0,28,49}})),
    archive_message(id(), outgoing, alice(), cat(), alice(), packet()),
    set_now(datetime_to_microseconds({{2000,1,1},{1,27,47}})),
    lookup_messages(alice(), undefined, undefined, undefined,
        get_now(), undefined, 0, true, 256),
    set_now(datetime_to_microseconds({{2000,1,1},{2,16,51}})),
    assert_keys(0, 0, [],
        lookup_messages(alice(), #rsm_in{direction=before},
            datetime_to_microseconds({{2000,1,1},{0,0,0}}) + 4,
            datetime_to_microseconds({{2000,1,1},{0,0,0}}) + 17,
            get_now(), undefined, 0, true, 256)).

tiny_empty_date_range_lookup2_case() ->
    reset_mock(),
    set_now(datetime_to_microseconds({{2000,1,1},{0,0,0}})),
    archive_message(id(), incoming, alice(), cat(), cat(), packet()),
    set_now(datetime_to_microseconds({{2000,1,1},{0,38,30}})),
    archive_message(id(), incoming, alice(), cat(), cat(), packet()),
    set_now(datetime_to_microseconds({{2000,1,1},{3,11,47}})),
    lookup_messages(alice(), undefined, undefined, undefined,
        get_now(), undefined, 0, true, 256),
    set_now(datetime_to_microseconds({{2000,1,1},{4,12,49}})),
    assert_keys(0, 2, [],
        lookup_messages(alice(), #rsm_in{index=2},
            datetime_to_microseconds({{2000,1,1},{0,0,0}}) + 4,
            datetime_to_microseconds({{2000,1,1},{0,0,0}}) + 17,
            get_now(), undefined, 0, true, 256)).

tiny_not_empty_date_range_lookup_case() ->
    reset_mock(),
    set_now(datetime_to_microseconds({{2000,1,1},{0,0,0}})),
    archive_message(id(), incoming, alice(), cat(), cat(), packet()),
    set_now(datetime_to_microseconds({{2000,1,1},{0,38,30}})),
    archive_message(id(), incoming, alice(), cat(), cat(), packet()),
    set_now(datetime_to_microseconds({{2000,1,1},{3,11,47}})),
    lookup_messages(alice(), undefined, undefined, undefined,
        get_now(), undefined, 0, true, 256),
    set_now(datetime_to_microseconds({{2000,1,1},{4,12,49}})),
    assert_keys(1, 2, [],
        lookup_messages(alice(), #rsm_in{index=2},
            datetime_to_microseconds({{2000,1,1},{0,0,0}}),
            datetime_to_microseconds({{2000,1,1},{0,0,0}}) + 17,
            get_now(), undefined, 0, true, 256)).

purge_single_message_case() ->
    reset_mock(),
    set_now(datetime_to_microseconds({{2000,1,1},{0,0,0}})),
    archive_message(id(), outgoing, alice(), cat1(), alice(), packet()),
    set_now(datetime_to_microseconds({{2000,1,1},{1,16,51}})),
    archive_message(id(), outgoing, alice(), cat1(), alice(), packet()),
    set_now(datetime_to_microseconds({{2000,1,1},{1,27,31}})),
    ?assertEqual(ok, purge_single_message(alice(),
        datetime_to_mess_id({{2000,1,1},{0,0,0}}), get_now())),
    set_now(datetime_to_microseconds({{2000,1,1},{2,10,35}})),
    assert_keys(1, 0, ["2000-01-01T01:16:51"],
        lookup_messages(alice(), #rsm_in{direction=before}, undefined, undefined,
            get_now(), undefined, 4, true, 256)).

purge_single_message_digest_case() ->
    reset_mock(),
    %% Add a message.
    set_now(datetime_to_microseconds({{2000,1,1},{0,1,0}})),
    archive_message(id(), incoming, alice(), cat1(), cat1(), packet()),
    set_now(datetime_to_microseconds({{2000,1,1},{1,46,11}})),
    %% Trigger digest creation.
    lookup_messages(alice(), undefined, undefined, undefined,
        get_now(), undefined, 4, true, 256),
    %% Delete the message, update the digest.
    set_now(datetime_to_microseconds({{2000,1,1},{5,0,6}})),
    purge_single_message(alice(),
        datetime_to_mess_id({{2000,1,1},{0,1,0}}), get_now()),
    set_now(datetime_to_microseconds({{2000,1,1},{5,26,2}})),
    %% Use a new digest.
    assert_keys(0, 0, [],
        lookup_messages(alice(),
            #rsm_in{direction=aft,
                    id=datetime_to_mess_id({{2000,1,1},{4,44,46}})},
            undefined, undefined, get_now(), undefined, 1, true, 256)).

purge_single_message_digest_zero_page_size_case() ->
    %% analyse_digest_before_case/0
    %% Strategy whole_digest_and_recent
    reset_mock(),
    set_now(datetime_to_microseconds({{2000,1,1},{1,24,43}})),
    archive_message(id(), outgoing, alice(), cat(), alice(), packet()),
    set_now(datetime_to_microseconds({{2000,1,1},{2,9,27}})),
    archive_message(id(), outgoing, alice(), cat1(), alice(), packet()),
    set_now(datetime_to_microseconds({{2000,1,1},{3,11,32}})),
    %% Build user's digest.
    lookup_messages(alice(), #rsm_in{index=5}, undefined, undefined,
        get_now(), undefined, 3, true, 256),
    set_now(datetime_to_microseconds({{2000,1,1},{3,20,55}})),
    purge_single_message(alice(),
        datetime_to_mess_id({{2000,1,1},{2,9,27}}), get_now()),
    set_now(datetime_to_microseconds({{2000,1,1},{6,55,58}})),
    %% Offset is undefined.
    assert_keys(1, 1, [],
        lookup_messages(alice(), #rsm_in{direction=before},
            undefined, undefined, get_now(), undefined, 0, true, 256)).

after_last_page_case() ->
    reset_mock(),
    %% after_last_page
    set_now(datetime_to_microseconds({{2000,1,1},{0,0,0}})),
    archive_message(id(), incoming, alice(), cat(), cat(), packet()),
    set_now(datetime_to_microseconds({{2000,1,1},{0,40,1}})),
    archive_message(id(), outgoing, alice(), cat(), alice(), packet()),
    %% AFTER
    set_now(datetime_to_microseconds({{2000,1,1},{2,5,51}})),
    lookup_messages(alice(), undefined, undefined, undefined, get_now(), undefined, 3, true, 256),
    set_now(datetime_to_microseconds({{2000,1,1},{2,30,52}})),
    archive_message(id(), incoming, alice(), cat1(), cat1(), packet()),
    set_now(datetime_to_microseconds({{2000,1,1},{2,33,44}})),
    assert_keys(3, 2,
        ["2000-01-01T02:30:52"],
        lookup_messages(alice(),
            #rsm_in{direction=aft, id=datetime_to_mess_id({{2000,1,1},{0,49,27}})},
            undefined, undefined, get_now(), undefined, 1, true, 256)).


%% `lists:split/3' that does allow small lists.
%% `lists:split/2' is already safe.
safe_split(N, L) when length(L) < N ->
    {L, []};
safe_split(N, L) ->
    lists:split(N, L).

safe_generator(F) ->
    {generator, fun() -> run_safe_generator(F) end}.

run_safe_generator(F) ->
    try
        F()
    catch error:Reason ->
        T = erlang:get_stacktrace(),
        ?debugVal(T),
        [?_assertEqual('BAD GENERATOR', Reason)]
    end.

message_row_to_mess_id({MessId, _, _}) -> MessId.

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
    Microseconds = mess_id_to_microseconds(MessID),
    TimeStamp = mod_mam_utils:microseconds_to_now(Microseconds),
    {{Year, Month, Day}, {Hour, Minute, Second}} =
    calendar:now_to_universal_time(TimeStamp),
    lists:flatten(
      io_lib:format("~4..0w-~2..0w-~2..0wT~2..0w:~2..0w:~2..0w",
                    [Year, Month, Day, Hour, Minute, Second])).

packet() -> <<"hi">>. % any term


alice() ->
    #jid{luser = <<"alice">>, lserver = <<"wonderland">>, lresource = <<>>,
          user = <<"alice">>,  server = <<"wonderland">>,  resource = <<>>}.

cat() ->
    #jid{luser = <<"cat">>, lserver = <<"wonderland">>, lresource = <<>>,
          user = <<"cat">>,  server = <<"wonderland">>,  resource = <<>>}.

cat1() ->
    #jid{luser = <<"cat">>, lserver = <<"wonderland">>, lresource = <<"1">>,
          user = <<"cat">>,  server = <<"wonderland">>,  resource = <<"1">>}.

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

%% used by eunit
user_id(<<"alice">>) -> 1;

%% Next part of the function called from statem
user_id(<<"cat">>)      -> 2;
user_id(<<"hatter">>)   -> 3;
user_id(<<"duchess">>)  -> 4;
user_id(<<"dormouse">>) -> 5.

message(Text) when is_binary(Text) ->
    #xmlel{
        name = <<"message">>,
        children = [
            #xmlel{
                name = <<"body">>,
                children = #xmlcdata{content = Text}
            }]
    }.

id() ->
    Microseconds = get_now(),
    encode_compact_uuid(Microseconds, 1).

id(Date) when is_binary(Date) ->
    Microseconds = mod_mam_utils:maybe_microseconds(Date),
    encode_compact_uuid(Microseconds, 1).

datetime_to_mess_id(DateTime) ->
    Microseconds = datetime_to_microseconds(DateTime),
    encode_compact_uuid(Microseconds, 1).
    

date_to_hour(Date, Time) ->
    date_to_hour(list_to_binary(join_date_time_z(Date, Time))).

date_to_hour(DateTime) when is_binary(DateTime) ->
    Microseconds = mod_mam_utils:maybe_microseconds(DateTime),
    hour(Microseconds).

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

safe_sublist(L, S, C) when S =< length(L), S > 0 ->
    lists:sublist(L, S, C);
safe_sublist(_, _, _) ->
    [].

is_single_page(PageSize, PageDigest) ->
    dig_total(PageDigest) =< PageSize.

undefined_bound() -> undefined.

maybe_min(undefined, X) -> X;
maybe_min(X, undefined) -> X;
maybe_min(X, Y) -> min(X, Y).

maybe_max(undefined, X) -> X;
maybe_max(X, undefined) -> X;
maybe_max(X, Y) -> max(X, Y).


merge_and_purge_digest_objects(Conn, Start, End, DigestObjects) ->
    [merge_and_purge_digest_object(Conn, Start, End, DigestObj)
     || DigestObj <- DigestObjects].

merge_and_purge_digest_object(Conn, Start, End, DigestObj) ->
    RQ = make_query(Conn, DigestObj),
    MergedDigestObj = merge_siblings(RQ, DigestObj),
    Digest = binary_to_digest(riakc_obj:get_value(MergedDigestObj)),
    NewDigest = dig_purge(RQ, Start, End, Digest),
    riakc_obj:update_value(MergedDigestObj, digest_to_binary(NewDigest)).

merge_and_single_purge_digest_object(Conn, MessID, DigestObj)
    when is_integer(MessID) ->
    RQ = make_query(Conn, DigestObj),
    MergedDigestObj = merge_siblings(RQ, DigestObj),
    Digest = binary_to_digest(riakc_obj:get_value(MergedDigestObj)),
    NewDigest = dig_decrease(Digest, mess_id_to_hour(MessID), 1),
    riakc_obj:update_value(MergedDigestObj, digest_to_binary(NewDigest)).

%% @doc Delete any information from the digest about records
%%      in the period from `Start' to `End'.
%% @end
dig_purge(RQ, Start, End, Digest) ->
    StartHour = hour(Start),
    EndHour   = hour(End),
    BeforeStartDigest = dig_before_hour(StartHour, Digest),
    AfterEndDigest    = dig_after_hour(EndHour, Digest),
    NewStartHourCnt = get_hour_key_count_from(RQ, Digest, Start),
    NewEndHourCnt   = get_hour_key_count_to(RQ, Digest, End),
    dig_concat([BeforeStartDigest, dig_cell(StartHour, NewStartHourCnt),
                dig_cell(EndHour, NewEndHourCnt), AfterEndDigest]).
    
dig_cell(_,    0  ) -> [];
dig_cell(Hour, Cnt) -> [{Hour, Cnt}].

sparse_purge(Conn, DeletedMessKeys, DigestKey) ->
    case riakc_pb_socket:get(Conn, digest_bucket(), DigestKey) of
        {error, notfound} -> [];
        {ok, DigestObj} ->
            [sparse_purge_object(Conn, DeletedMessKeys, DigestObj)]
    end.

dense_purge(Conn, Start, End, DigestKey) ->
    case riakc_pb_socket:get(Conn, digest_bucket(), DigestKey) of
        {error, notfound} -> [];
        {ok, DigestObj} ->
            [merge_and_purge_digest_object(Conn, Start, End, DigestObj)]
    end.

single_purge(Conn, MessID, DigestKey) ->
    case riakc_pb_socket:get(Conn, digest_bucket(), DigestKey) of
        {error, notfound} -> [];
        {ok, DigestObj} ->
            [merge_and_single_purge_digest_object(Conn, MessID, DigestObj)]
    end.

sparse_purge_object(Conn, DeletedMessKeys, DigestObj) ->
    RQ = make_query(Conn, DigestObj),
    %% Naive implementation (without extracting all records).
    DeletedDigest = dig_new(DeletedMessKeys),
    MergedDigestObj = merge_siblings(RQ, DigestObj),
    Digest = binary_to_digest(riakc_obj:get_value(MergedDigestObj)),
    NewDigest = dig_subtract(Digest, DeletedDigest),
    riakc_obj:update_value(MergedDigestObj, NewDigest).

save_updated_digests(Conn, ModifiedObjects) ->
    {Deleted, Updated} = lists:partition(fun is_empty_digest_obj/1, ModifiedObjects),
    [riakc_pb_socket:put(Conn, Obj) || Obj <- Updated],
    [riakc_pb_socket:delete_obj(Conn, Obj) || Obj <- Deleted],
    ok.

filter_midified_objects(OldDigestObjects, NewDigestObjects) ->
    [New || {Old, New} <- lists:zip(OldDigestObjects, NewDigestObjects),
     Old =/= New].
