-module(mod_mam_riak_arch).
-export([start/1,
         archive_message/6,
         wait_flushing/1,
         archive_size/2,
         lookup_messages/9,
         remove_user_from_db/2]).

%% UID
-import(mod_mam_utils,
        [encode_compact_uuid/2]).

%% Time
-import(mod_mam_utils,
        [datetime_to_microseconds/1]).

-type message_row() :: tuple().

-ifdef(TEST).
-export([load_mock/1,
         unload_mock/0,
         reset_mock/0,
         set_now/1]).

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
            update_bucket(Conn, digest_bucket(), [{allow_mult, true}])
        end,
    with_connection(Host, F).

lww_props() ->
    [{allow_mult, false}, {last_write_wins, true}].


archive_size(LServer, LUser) ->
    Now = mod_mam_utils:now_to_microseconds(now()),
    {ok, {TotalCount, 0, []}} = lookup_messages(
            jlib:make_jid(LUser, LServer, <<>>),
            none, undefined, undefined, Now, undefined, 0, true, 0),
    TotalCount.

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
    {KeysBeforeCnt, save_sublist(Keys, KeysBeforeCnt + 1, PageSize)};
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


-spec lookup_messages(UserJID, RSM, Start, End, Now, WithJID, PageSize,
                      LimitPassed, MaxResultLimit) ->
    {ok, {TotalCount, Offset, MessageRows}} | {error, 'policy-violation'}
			     when
    UserJID :: #jid{},
    RSM     :: #rsm_in{} | none,
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
    SecIndex = choose_index(WithJID),
    BWithJID = maybe_jid_to_opt_binary(LocLServer, WithJID),
    DigestKey = digest_key(BUserID, BWithJID),
    MessIdxKeyMaker = mess_index_key_maker(BUserID, BWithJID),
    RSM1 = fix_rsm(BUserID, RSM),
    F = fun(Conn) ->
        QueryAllF = fun() ->
            query_all(Conn, SecIndex, MessIdxKeyMaker, Start, End, PageSize, RSM1)
            end,
        check_result_and_return(Conn, MaxResultLimit, LimitPassed,
        case is_short_range(Now, Start, End) of
        true ->
            %% ignore index digest.
            QueryAllF();
        false ->
            case get_actual_digest(
                Conn, DigestKey, Now, SecIndex, MessIdxKeyMaker) of
            {error, notfound} ->
                Result = QueryAllF(),
                should_create_digest(LocLServer, Result)
                andalso
                create_non_empty_digest(Conn, DigestKey,
                    fetch_old_keys(Conn, SecIndex, MessIdxKeyMaker, Now)),
                Result;
            {ok, Digest} ->
                analyse_digest(Conn, QueryAllF, SecIndex, MessIdxKeyMaker,
                               Start, End, PageSize, RSM1, Digest)
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


analyse_digest(Conn, QueryAllF, SecIndex, MessIdxKeyMaker,
               Start, End, PageSize, RSM, Digest) ->
    case RSM of
        #rsm_in{index = Offset} when is_integer(Offset), Offset >= 0 ->
            %% If `Start' is not recorded, that requested result
            %% set contains new messages only
            case is_defined(Start) andalso not dig_is_recorded(hour(Start), Digest) of
                %% `Start' is after the digist.
                %% Match only recent messages.
                true -> QueryAllF();
                false ->
                    analyse_digest_index(
                        Conn, QueryAllF, SecIndex, MessIdxKeyMaker,
                        Offset, Start, End, PageSize, Digest)
            end;
        %% Last page.
        #rsm_in{direction = before, id = undefined} ->
            analyse_digest_last_page(Conn, QueryAllF, SecIndex, MessIdxKeyMaker,
                                     Start, End, PageSize, Digest);
        #rsm_in{direction = before, id = Id} ->
            analyse_digest_before(Conn, QueryAllF, SecIndex, MessIdxKeyMaker,
                                  Start, End, PageSize, Id, Digest);
        #rsm_in{direction = aft, id = Id} ->
            analyse_digest_after(Conn, QueryAllF, SecIndex, MessIdxKeyMaker,
                                 Start, End, PageSize, Id, Digest);
        _ ->
            QueryAllF()
    end.

analyse_digest_index(Conn, QueryAllF, SecIndex, MessIdxKeyMaker,
                     Offset, Start, End, PageSize, Digest) ->
    Strategy = choose_strategy(Start, End, Digest),
    case Strategy of
    empty -> return_empty();
    recent_only -> QueryAllF();
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
            RecentCnt = get_recent_entry_count(
                Conn, SecIndex, MessIdxKeyMaker, End, Digest),
            TotalCount = dig_total(Digest) + RecentCnt,
            return_matched_keys(TotalCount, Offset, []);
        nothing_from_digest ->
            %% Left to skip
            LeftOffset = Offset - DigestCnt,
            LHour = dig_last_hour(Digest),
            LBound = MessIdxKeyMaker({lower, hour_upper_bound(LHour)+1}),
            UBound = MessIdxKeyMaker({upper, End}),
            Keys = get_key_range(Conn, SecIndex, LBound, UBound),
            MatchedKeys = save_sublist(Keys, LeftOffset+1, PageSize),
            TotalCount = DigestCnt + length(Keys),
            return_matched_keys(TotalCount, Offset, MatchedKeys);
        part_from_digest ->
            %% Head of  `PageDigest' contains last entries of the previous
            %% page and starting messages of the requsted page.
            %% `FirstHourOffset' is how many entries to skip in `PageDigest'.
            {FirstHourOffset, PageDigest} = dig_skip_n(Offset, Digest),
            LHour = dig_first_hour(PageDigest),
            LBound = MessIdxKeyMaker({lower, hour_lower_bound(LHour)}),
            UBound = MessIdxKeyMaker({upper, End}),
            Keys = get_key_range(Conn, SecIndex, LBound, UBound),
            MatchedKeys = save_sublist(Keys, FirstHourOffset+1, PageSize),
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
            LBound = MessIdxKeyMaker({lower, hour_lower_bound(LHour)}),
            UBound = MessIdxKeyMaker({upper, hour_upper_bound(UHour)}),
            Keys = get_key_range(Conn, SecIndex, LBound, UBound),
            MatchedKeys = save_sublist(Keys, FirstHourOffset+1, PageSize),
            RecentCnt = get_recent_entry_count(Conn, SecIndex, MessIdxKeyMaker, End, Digest),
            TotalCount = DigestCnt + RecentCnt,
            return_matched_keys(TotalCount, Offset, MatchedKeys)
        end;
    lower_bounded ->
        %% RSet includes a start hour
        RSetDigest = dig_from_hour(hour(Start), Digest),
        RSetDigestCnt = dig_total(RSetDigest),
        StartHourOffset = start_hour_offset(
            Conn, MessIdxKeyMaker, SecIndex, Start, Digest),
        RSetOffset = StartHourOffset + Offset,
        LBoundPos = dig_nth_pos(RSetOffset, RSetDigest),
        UBoundPos = dig_nth_pos(RSetOffset + PageSize, RSetDigest),
        LHour = case LBoundPos of
            inside  -> dig_nth(RSetOffset, RSetDigest);
            'after' -> dig_last_hour(RSetDigest)
            end,
        LBound = MessIdxKeyMaker({lower, hour_lower_bound(LHour)}),
        UBound = case UBoundPos of
            inside ->
                UHour = dig_nth(RSetOffset + PageSize, RSetDigest),
                MessIdxKeyMaker({upper, hour_upper_bound(UHour)});
            'after' -> MessIdxKeyMaker({upper, undefined})
            end,
        Keys = get_key_range(Conn, SecIndex, LBound, UBound),
        %% How many entries in `RSetDigest' before `Keys'.
        SkippedCnt = dig_total(dig_before_hour(LHour, RSetDigest)),
        %% How many unwanted keys were extracted.
        PageOffset = RSetOffset - SkippedCnt,
        MatchedKeys = save_sublist(Keys, PageOffset+1, PageSize),
        RecentCnt = case UBoundPos of
            inside -> get_recent_entry_count(
                    Conn, SecIndex, MessIdxKeyMaker, End, Digest);
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
            case End < hour_upper_bound(UHour1) of
            true -> last_page;
            false -> middle_page
            end
        end,
        case Strategy2 of
        count_only ->
            AfterHourCnt = after_hour_cnt(
                Conn, MessIdxKeyMaker, SecIndex, End, RSetDigest),
            TotalCount = RSetDigestCnt - AfterHourCnt,
            return_matched_keys(TotalCount, Offset, []);
        middle_page ->
            LHour = dig_nth(Offset, RSetDigest),
            UHour = dig_nth(Offset + PageSize, RSetDigest),
            LBound = MessIdxKeyMaker({lower, hour_lower_bound(LHour)}),
            UBound = MessIdxKeyMaker({upper, hour_upper_bound(UHour)}),
            Keys = get_key_range(Conn, SecIndex, LBound, UBound),
            PageOffset = dig_total(dig_before_hour(LHour, RSetDigest)),
            MatchedKeys = save_sublist(Keys, PageOffset+1, PageSize),
            AfterHourCnt = after_hour_cnt(
                Conn, MessIdxKeyMaker, SecIndex, End, RSetDigest),
            TotalCount = RSetDigestCnt - AfterHourCnt,
            return_matched_keys(TotalCount, Offset, MatchedKeys);
        last_page ->
            LHour = dig_nth(Offset, RSetDigest),
            LBound = MessIdxKeyMaker({lower, hour_lower_bound(LHour)}),
            UBound = MessIdxKeyMaker({upper, End}),
            Keys = get_key_range(Conn, SecIndex, LBound, UBound),
            PageOffset = dig_total(dig_before_hour(LHour, RSetDigest)),
            MatchedKeys = save_sublist(Keys, PageOffset+1, PageSize),
            AfterHourCnt = filter_after_timestamp(End+1, Keys),
            TotalCount = RSetDigestCnt - AfterHourCnt,
            return_matched_keys(TotalCount, Offset, MatchedKeys)
        end;
    bounded ->
        RSetDigest = dig_to_hour(hour(End), dig_from_hour(hour(Start), Digest)),
        %% Expected size of the Result Set in best-cast scenario (maximim)
        RSetDigestCnt = dig_total(RSetDigest),
        HeadCnt = dig_first_count(RSetDigest),
        LastCnt = dig_last_count(RSetDigest),
        Strategy2 =
        case {Offset + PageSize > RSetDigestCnt,
              Offset < HeadCnt,
              Offset + PageSize > RSetDigestCnt - LastCnt} of
        {true, _, _} -> bounded_count_only;
        {false, true, true} -> bounded_query_all;
        {false, true, false} -> bounded_first_page;
        {false, false, true} -> bounded_last_page;
        {false, false, false} -> bounded_middle_page
        end,
        case Strategy2 of
        bounded_query_all -> QueryAllF();
        bounded_count_only ->
            AfterHourCnt = after_hour_cnt(
                Conn, MessIdxKeyMaker, SecIndex, End, RSetDigest),
            StartHourOffset = start_hour_offset(
                Conn, MessIdxKeyMaker, SecIndex, Start, Digest),
            TotalCount = RSetDigestCnt - StartHourOffset - AfterHourCnt,
            return_matched_keys(TotalCount, Offset, []);
        bounded_first_page ->
            UHour = dig_nth(HeadCnt + Offset + PageSize, RSetDigest),
            LBound = MessIdxKeyMaker({lower, Start}),
            UBound = MessIdxKeyMaker({upper, hour_upper_bound(UHour)}),
            Keys = get_key_range(Conn, SecIndex, LBound, UBound),
            MatchedKeys = save_sublist(Keys, Offset+1, PageSize),
            StartHourCnt = filter_after_timestamp(hour_upper_bound(hour(Start))+1, Keys),
            AfterHourCnt = after_hour_cnt(
                Conn, MessIdxKeyMaker, SecIndex, End, RSetDigest),
            StartHourOffset = HeadCnt - StartHourCnt,
            TotalCount = RSetDigestCnt - StartHourOffset - AfterHourCnt,
            return_matched_keys(TotalCount, Offset, MatchedKeys);
        bounded_last_page ->
            StartHourOffset = start_hour_offset(
                Conn, MessIdxKeyMaker, SecIndex, Start, Digest),
            RSetOffset = HeadCnt + Offset - StartHourOffset,
            LHour = dig_nth(RSetOffset, RSetDigest),
            LBound = MessIdxKeyMaker({lower, hour_upper_bound(LHour)}),
            UBound = MessIdxKeyMaker({upper, End}),
            Keys = get_key_range(Conn, SecIndex, LBound, UBound),
            PageOffset = RSetOffset + dig_total(dig_before_hour(LHour, RSetDigest)),
            MatchedKeys = save_sublist(Keys, PageOffset+1, PageSize),
            LastHourCnt = filter_after_timestamp(hour_lower_bound(hour(End)), Keys),
            %% expected - real
            AfterHourCnt = LastCnt - LastHourCnt,
            TotalCount = RSetDigestCnt - StartHourOffset - AfterHourCnt,
            return_matched_keys(TotalCount, Offset, MatchedKeys);
        bounded_middle_page ->
            AfterHourCnt = after_hour_cnt(
                Conn, MessIdxKeyMaker, SecIndex, End, RSetDigest),
            StartHourOffset = start_hour_offset(
                Conn, MessIdxKeyMaker, SecIndex, Start, Digest),
            TotalCount = RSetDigestCnt - StartHourOffset - AfterHourCnt,
            RSetOffset = HeadCnt + Offset - StartHourOffset,
            LHour = dig_nth(RSetOffset, RSetDigest),
            UHour = dig_nth(RSetOffset + PageSize, RSetDigest),
            LBound = MessIdxKeyMaker({lower, hour_lower_bound(LHour)}),
            UBound = MessIdxKeyMaker({upper, hour_upper_bound(UHour)}),
            Keys = get_key_range(Conn, SecIndex, LBound, UBound),
            PageOffset = RSetOffset + dig_total(dig_before_hour(LHour, RSetDigest)),
            MatchedKeys = save_sublist(Keys, PageOffset+1, PageSize),
            return_matched_keys(TotalCount, Offset, MatchedKeys)
        end
    end.


%% Is the key `Key' before, `after' or inside of the result set?
key_position(Key, Start, End, Digest) ->
    %% Primary key to secondary
    IdxKey   = timestamp_bound_to_binary({key, Key}),
    StartKey = timestamp_bound_to_binary({lower, Start}),
    EndKey   = timestamp_bound_to_binary({upper, End}),
    MinKey   = dig_minimum_key(Digest),
    MaxKey   = dig_maximum_key(Digest),
    LBoundKey = max(MinKey, StartKey),
    UBoundKey = min(MaxKey, EndKey),
    if IdxKey < LBoundKey -> before;
       IdxKey > UBoundKey -> 'after';
       true -> inside
    end.

dig_minimum_key(Digest) ->
    timestamp_bound_to_binary({lower, hour_lower_bound(dig_first_hour(Digest))}).

dig_maximum_key(Digest) ->
    timestamp_bound_to_binary({upper, hour_upper_bound(dig_last_hour(Digest))}).


get_recent_entry_count(Conn, SecIndex, MessIdxKeyMaker, End, Digest) ->
    LastKnownHour = dig_last_hour(Digest),
    S1 = MessIdxKeyMaker({lower, hour_lower_bound(LastKnownHour+1)}),
    E1 = MessIdxKeyMaker({upper, End}),
    get_entry_count_between(Conn, SecIndex, S1, E1).


analyse_digest_before(Conn, QueryAllF, SecIndex, MessIdxKeyMaker,
                      Start, End, PageSize, BeforeKey, Digest) ->
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
    query_all -> QueryAllF();
    %% B S E
    count_only -> %% return a total count only
        StartHourCnt = start_hour_count(
            Conn, MessIdxKeyMaker, SecIndex, Start, End, Digest),
        RecentCnt = get_recent_entry_count(
            Conn, SecIndex, MessIdxKeyMaker, End, RSetDigest),
        RSetDigestCnt = dig_total(RSetDigest),
        TotalCount = StartHourCnt + RSetDigestCnt + RecentCnt,
        return_matched_keys(TotalCount, 0, []);
    %% S B E
    first_page ->
        %% Page is in the beginning of the RSet.
        %% `PageDigest' is empty.
        LBound = MessIdxKeyMaker({lower, Start}),
        UBound = MessIdxKeyMaker({key, previous_key(BeforeKey)}),
        Keys = get_key_range(Conn, SecIndex, LBound, UBound),
        StartHourCnt = filter_and_count_keys_before_digest(Keys, RSetDigest),
        MatchedKeys = sublist_r(Keys, PageSize),
        Offset = length(Keys) - length(MatchedKeys),
        RecentCnt = get_recent_entry_count(
            Conn, SecIndex, MessIdxKeyMaker, End, RSetDigest),
        RSetDigestCnt = dig_total(RSetDigest),
        TotalCount = StartHourCnt + RSetDigestCnt + RecentCnt,
        return_matched_keys(TotalCount, Offset, MatchedKeys);
    %% S B E
    middle_page ->
        MinHour = page_minimum_hour(PageSize, PageDigest),
        LBound = MessIdxKeyMaker({lower, hour_lower_bound(MinHour)}),
        UBound = MessIdxKeyMaker({key, previous_key(BeforeKey)}),
        Keys = get_key_range(Conn, SecIndex, LBound, UBound),
        MatchedKeys = sublist_r(Keys, PageSize),
        StartHourCnt = start_hour_count(
            Conn, MessIdxKeyMaker, SecIndex, Start, End, Digest),
        RecentCnt = get_recent_entry_count(
            Conn, SecIndex, MessIdxKeyMaker, End, RSetDigest),
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
        PrevBeforeKey = previous_key(BeforeKey),
        PrevBeforeIdxKey = MessIdxKeyMaker({key, PrevBeforeKey}),
        EndKey = MessIdxKeyMaker({upper, End}),
        MinHour = page_minimum_hour(PageSize, PageDigest),
        UBound = min(PrevBeforeIdxKey, EndKey),
        LBound = MessIdxKeyMaker({lower, hour_lower_bound(MinHour)}),
        Keys = get_key_range(Conn, SecIndex, LBound, UBound),
        MatchedKeys = sublist_r(Keys, PageSize),
        StartHourCnt = start_hour_count(
            Conn, MessIdxKeyMaker, SecIndex, Start, End, Digest),
        %% Count of messages between `PageDigest' and `BeforeKey'.
        BeforeOffset = filter_and_count_recent_keys(Keys, PageDigest),
        LastMatchedOffset = StartHourCnt + dig_total(PageDigest) + BeforeOffset,
        %% if PrevBeforeIdxKey > EndKey, returns 0.
        RecentCnt = get_entry_count_between(
            Conn, SecIndex, PrevBeforeIdxKey, EndKey),
        TotalCount = LastMatchedOffset + RecentCnt,
        Offset = LastMatchedOffset - length(MatchedKeys),
        return_matched_keys(TotalCount, Offset, MatchedKeys)
    end.


analyse_digest_after(Conn, QueryAllF, SecIndex, MessIdxKeyMaker,
                     Start, End, PageSize, AfterKey, Digest) ->
    %% Any value from `RSetDigest' is inside the result set.
    %% `RSetDigest' does not contain `hour(Start)' and `hour(End)'.
    RSetDigest = dig_between(Start, End, Digest),
    %% Hour in which a message with the passed id was send.
    AfterHour = key_to_hour(AfterKey),
    %% Top values from `PageDigest' will be on the page.
    %% `AfterHour' is included.
    PageDigest = dig_from_hour(AfterHour, RSetDigest),
    KeyPos = key_position(AfterKey, Start, End, RSetDigest),
    Strategy = case {dig_is_empty(RSetDigest),
                     dig_is_empty(PageDigest),
                     is_single_page(PageSize, PageDigest), KeyPos} of
        {true,  _,     _,     _      } -> query_all;
        {false, _,     _,     before } -> first_page;
        {false, false, true,  inside } -> last_page;
        {false, true,  true,  'after'} -> recent_only;
        {false, false, false, inside } -> middle_page
        end,
    %% Place different points on the timeline:
    %% A - after, S - start, E - end, N - now
    case Strategy of
    query_all -> QueryAllF();
    %% S E A
    count_only -> %% return a total count only
        StartHourCnt = start_hour_count(
            Conn, MessIdxKeyMaker, SecIndex, Start, End, Digest),
        RecentCnt = get_recent_entry_count(
            Conn, SecIndex, MessIdxKeyMaker, End, RSetDigest),
        RSetDigestCnt = dig_total(RSetDigest),
        TotalCount = StartHourCnt + RSetDigestCnt + RecentCnt,
        return_matched_keys(TotalCount, 0, []);
    %% A S E
    first_page ->
        %% Page is in the beginning of the RSet.
        analyse_digest_index(Conn, QueryAllF, SecIndex, MessIdxKeyMaker,
                             0, Start, End, PageSize, Digest);
    %% S A E
    middle_page ->
        %% Primary key.
        NextAfterKey = next_key(AfterKey),
        %% Secondary index key.
        NextAfterIdxKey = MessIdxKeyMaker({key, NextAfterKey}),
        StartHourCnt = start_hour_count(
            Conn, MessIdxKeyMaker, SecIndex, Start, End, Digest),
        RecentCnt = get_recent_entry_count(
            Conn, SecIndex, MessIdxKeyMaker, End, RSetDigest),
        RSetDigestCnt = dig_total(RSetDigest),
        TotalCount = StartHourCnt + RSetDigestCnt + RecentCnt,
        MaxHour = page_maximum_hour_for_tail(PageSize, PageDigest),
        LBound = NextAfterIdxKey,
        UBound = MessIdxKeyMaker({upper, hour_upper_bound(MaxHour)}),
        Keys = get_key_range(Conn, SecIndex, LBound, UBound),
        MatchedKeys = lists:sublist(Keys, PageSize),
        %% Position in `RSetDigestCnt' of the last selected key.
        LastKeyOffset = dig_total(dig_to_hour(MaxHour, RSetDigest)),
        Offset = StartHourCnt + LastKeyOffset - length(Keys),
        return_matched_keys(TotalCount, Offset, MatchedKeys);
    %% S A E
    last_page ->
        %% Primary key.
        NextAfterKey = next_key(AfterKey),
        %% Secondary index key.
        NextAfterIdxKey = MessIdxKeyMaker({key, NextAfterKey}),
        %% Request keys between `PageDigest' and the end of the RSet.
        LBound = NextAfterIdxKey,
        UBound = MessIdxKeyMaker({upper, End}),
        Keys = get_key_range(Conn, SecIndex, LBound, UBound),
        %% Keys in the tail of the digest
        DigestTailCnt = filter_and_count_digest_keys(Keys, RSetDigest),
        MatchedKeys = lists:sublist(Keys, PageSize),
        StartHourCnt = start_hour_count(
            Conn, MessIdxKeyMaker, SecIndex, Start, End, Digest),
        RSetDigestCnt = dig_total(RSetDigest),
        %% Count of messages between `PageDigest' and `AfterKey'.
        Offset = StartHourCnt + RSetDigestCnt - DigestTailCnt,
        TotalCount = Offset + length(Keys),
        return_matched_keys(TotalCount, Offset, MatchedKeys);
    recent_only ->
        %% assert
        true  = dig_is_empty(PageDigest),
        false = dig_is_empty(RSetDigest),
        LastDigestHour = dig_last_hour(RSetDigest),
        LBound = MessIdxKeyMaker({lower, hour_lower_bound(LastDigestHour+1)}),
        UBound = MessIdxKeyMaker({upper, End}),
        %% Get recent keys after `RSetDigest'.
        Keys = get_key_range(Conn, SecIndex, LBound, UBound),
        AfterKeys = filter_after(AfterKey, Keys),
        MatchedKeys = lists:sublist(AfterKeys, PageSize),
        StartHourCnt = start_hour_count(
            Conn, MessIdxKeyMaker, SecIndex, Start, End, Digest),
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

%% Count of entries between `Start' and `hour(Start)'.
%% TODO: can be optimized: if `minutes(hour(Start)) < 30', count a hour offset.
%% Limitation: `hour(Start) =/= hour(End)' enforced in `is_short_range/3'.
start_hour_count(_, _, _, undefined, _, _) ->
    0; % Start is undefined.
start_hour_count(Conn, MessIdxKeyMaker, SecIndex, Start, End, Digest) ->
    %% Use metainformation to optimize, if the hour is empty.
    Strategy = choose_strategy(Start, End, Digest),
    Request = Strategy =:= lower_bounded orelse Strategy =:= bounded,
    case Request of
        false -> 0;
        true -> request_start_hour_count(
                Conn, MessIdxKeyMaker, SecIndex, Start, Digest)
    end.

start_hour_offset(Conn, MessIdxKeyMaker, SecIndex, Start, Digest) ->
    IsZero = (not is_defined(Start)) orelse
             dig_is_skipped(hour(Start), Digest),
    %% Count of messages from the hour beginning to `Start'.
    case IsZero of
    true -> 0;
    false -> %% should calculate
        S = MessIdxKeyMaker({lower, hour_lower_bound(hour(Start))}),
        E = MessIdxKeyMaker({upper, Start}),
        get_entry_count_between(Conn, SecIndex, S, E)
    end.

after_hour_cnt(Conn, MessIdxKeyMaker, SecIndex, End, Digest) ->
    IsZero = (not is_defined(End)) orelse
             dig_is_skipped(hour(End), Digest),
    case IsZero of
    true -> 0;
    false -> %% should calculate
        S = MessIdxKeyMaker({lower, End}),
        E = MessIdxKeyMaker({upper, hour_upper_bound(hour(End))}),
        get_entry_count_between(Conn, SecIndex, S, E)
    end.

request_start_hour_count(Conn, MessIdxKeyMaker, SecIndex, Start, Digest) ->
    case dig_is_skipped(hour(Start), Digest) of
    true -> 0;
    false -> get_hour_entry_count_after(
        Conn, MessIdxKeyMaker, SecIndex, Start)
    end.

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

analyse_digest_last_page(Conn, QueryAllF, SecIndex, MessIdxKeyMaker,
                         Start, End, PageSize, Digest) ->
    Strategy = choose_strategy(Start, End, Digest),
    case Strategy of
        empty -> return_empty();
        recent_only -> QueryAllF();
        whole_digest_and_recent ->
            Keys = get_minimum_key_range_before(
                Conn, MessIdxKeyMaker, SecIndex, End, PageSize, Digest),
            RecentCnt = filter_and_count_recent_keys(Keys, Digest),
            TotalCount = dig_total(Digest) + RecentCnt,
            MatchedKeys = sublist_r(Keys, PageSize),
            Offset = TotalCount - length(MatchedKeys),
            return_matched_keys(TotalCount, Offset, MatchedKeys);
        lower_bounded ->
            %% Contains records, that will be in RSet for sure
            %% Digest2 does not contain hour(Start).
            Digest2 = dig_after_hour(hour(Start), Digest),
            case dig_total(Digest2) < PageSize of
            true -> QueryAllF(); %% It is a small range
            false ->
                StartHourCnt = request_start_hour_count(
                    Conn, MessIdxKeyMaker, SecIndex, Start, Digest),
                Keys = get_minimum_key_range_before(
                    Conn, MessIdxKeyMaker, SecIndex, End, PageSize, Digest2),
                RecentCnt = filter_and_count_recent_keys(Keys, Digest2),
                TotalCount = StartHourCnt + dig_total(Digest2) + RecentCnt,
                MatchedKeys = sublist_r(Keys, PageSize),
                Offset = TotalCount - length(MatchedKeys),
                return_matched_keys(TotalCount, Offset, MatchedKeys)
            end;
        upper_bounded ->
            Digest2 = dig_before_hour(hour(End), Digest),
            case dig_total(Digest2) < PageSize of
            true -> QueryAllF(); %% It is a small range
            false ->
                Keys = get_minimum_key_range_before(
                    Conn, MessIdxKeyMaker, SecIndex, End, PageSize, Digest2),
                LastHourCnt = filter_and_count_recent_keys(Keys, Digest2),
                TotalCount = dig_total(Digest2) + LastHourCnt,
                MatchedKeys = sublist_r(Keys, PageSize),
                Offset = TotalCount - length(MatchedKeys),
                return_matched_keys(TotalCount, Offset, MatchedKeys)
            end;
        bounded ->
            Digest2 = dig_before_hour(hour(End),
                dig_after_hour(hour(Start), Digest)),
            case dig_total(Digest2) < PageSize of
            true -> QueryAllF(); %% It is a small range
            false ->
                StartHourCnt = request_start_hour_count(
                    Conn, MessIdxKeyMaker, SecIndex, Start, Digest),
                Keys = get_minimum_key_range_before(
                    Conn, MessIdxKeyMaker, SecIndex, End, PageSize, Digest2),
                LastHourCnt = filter_and_count_recent_keys(Keys, Digest2),
                TotalCount = StartHourCnt + dig_total(Digest2) + LastHourCnt,
                MatchedKeys = sublist_r(Keys, PageSize),
                Offset = TotalCount - length(MatchedKeys),
                return_matched_keys(TotalCount, Offset, MatchedKeys)
            end;
        _ -> QueryAllF()
    end.

return_empty() ->
    {ok, {0, 0, []}}.


query_all(Conn, SecIndex, MessIdxKeyMaker, Start, End, PageSize, RSM) ->
    LBound = MessIdxKeyMaker({lower, Start}),
    UBound = MessIdxKeyMaker({upper, End}),
    Keys = get_key_range(Conn, SecIndex, LBound, UBound),
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
    Value = term_to_binary(Digest),
    DigestObj = riakc_obj:new(digest_bucket(), DigestKey, Value),
    riakc_pb_socket:put(Conn, DigestObj).

get_actual_digest(Conn, DigestKey, Now, SecIndex, MessIdxKeyMaker) ->
    case riakc_pb_socket:get(Conn, digest_bucket(), DigestKey) of
        {error, notfound} ->
            {error, notfound};
        {ok, DigestObj} ->
            DigestObj2 = merge_siblings(Conn, SecIndex, MessIdxKeyMaker, DigestObj),
            Digest2 = binary_to_term(riakc_obj:get_value(DigestObj2)),
            Last = last_modified(DigestObj2),
            case hour(Last) =:= hour(Now) of
                %% Digest is actual.
                true ->
                    %% Write the merged version.
                    has_siblings(DigestObj) andalso riakc_pb_socket:put(Conn, DigestObj2),
                    {ok, Digest2};
                false ->
                    Digest3 = update_digest(Conn, Now, Digest2, SecIndex, MessIdxKeyMaker),
                    DigestObj3 = riakc_obj:update_value(DigestObj2, term_to_binary(Digest3)),
                    riakc_pb_socket:put(Conn, DigestObj3),
                    {ok, Digest3}
            end
    end.

has_siblings(Obj) ->
    length(riakc_obj:get_values(Obj)) > 1.

merge_siblings(Conn, SecIndex, MessIdxKeyMaker, DigestObj) ->
    case has_siblings(DigestObj) of
        true ->
            DigestObj2 = fix_metadata(DigestObj),
            Values = riakc_obj:get_values(DigestObj),
            NewValue = dig_merge(
                Conn, SecIndex, MessIdxKeyMaker, deserialize_values(Values)),
            riakc_obj:update_value(DigestObj2, NewValue);
        false ->
            DigestObj
    end.

%% @doc Request new entries from the server.
%% `Now' are microseconds.
update_digest(Conn, Now, Digest, SecIndex, MessIdxKeyMaker) ->
    LHour = dig_last_hour(Digest) + 1,
    UHour = hour(Now) - 1,
    LBound = MessIdxKeyMaker({lower, hour_lower_bound(LHour)}),
    UBound = MessIdxKeyMaker({upper, hour_upper_bound(UHour)}),
    {ok, ?INDEX_RESULTS{keys=Keys}} =
    riakc_pb_socket:get_index_range(Conn, message_bucket(), SecIndex, LBound, UBound),
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

min_digest_key(BUserID) ->
    digest_key(BUserID, undefined).

max_digest_key(BUserID) ->
    %% JID cannot contain 255
    digest_key(BUserID, <<255>>).

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

max_binary_mess_id() ->
    <<-1:64>>. %% set all bits.

min_binary_mess_id() ->
    <<>>. %% any(binary()) >= <<>>

timestamp_bound_to_binary({lower, undefined}) ->
    min_binary_mess_id();
timestamp_bound_to_binary({lower, Microseconds}) ->
    mess_id_to_binary(encode_compact_uuid(Microseconds, 0));
timestamp_bound_to_binary({upper, undefined}) ->
    max_binary_mess_id();
timestamp_bound_to_binary({upper, Microseconds}) ->
    mess_id_to_binary(encode_compact_uuid(Microseconds, 255));
timestamp_bound_to_binary({mess_id, MessID}) ->
    mess_id_to_binary(MessID);
timestamp_bound_to_binary({key, Key}) ->
    mess_id_to_binary(key_to_mess_id(Key)).

remove_user_from_db(LServer, LUser) ->
    F = fun(Conn) ->
        delete_digests(Conn, LServer, LUser),
        delete_messages(Conn, LServer, LUser)
        end,
    with_connection(LServer, F),
    ok.


delete_digests(Conn, LServer, LUser) ->
    Keys = get_all_digest_keys(Conn, LServer, LUser),
    [riakc_pb_socket:delete(Conn, digest_bucket(), Key)
    || Key <- Keys],
    ok.

get_all_digest_keys(Conn, LServer, LUser) ->
    UserID = mod_mam_cache:user_id(LServer, LUser),
    BUserID = user_id_to_binary(UserID),
    LBound = min_digest_key(BUserID),
    UBound = max_digest_key(BUserID),
    get_key_range(Conn, digest_bucket(), key_index(), LBound, UBound).

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


with_connection(_LocLServer, F) ->
    ?MEASURE_TIME(with_connection,
        riak_pool:with_connection(mam_cluster, F)).

user_id_to_binary(UserID) ->
    <<UserID:64/big>>.

mess_id_to_binary(MessID) ->
    <<MessID:64/big>>.

%% Transforms `{{2013,7,21},{15,43,36}} => {{2013,7,21},{15,0,0}}'.
%% microseconds to hour
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

key_to_hour(Key) when is_binary(Key) ->
    hour(key_to_microseconds(Key)).

key_to_microseconds(Key) when is_binary(Key) ->
    {Microseconds, _} = mod_mam_utils:decode_compact_uuid(key_to_mess_id(Key)),
    Microseconds.

key_to_mess_id(Key) ->
    %% Get 8 bytes.
    <<MessID:64/big>> = binary:part(Key, byte_size(Key), -8),
    MessID.

%% This function is used to create a key, that will be lower that `BeforeKey',
%% but greater or equal, than any key before `BeforeKey'.
previous_key(BeforeKey) ->
    PrevMessID = key_to_mess_id(BeforeKey) - 1,
    replace_mess_id(PrevMessID, BeforeKey).

next_key(AfterKey) ->
    PrevMessID = key_to_mess_id(AfterKey) + 1,
    replace_mess_id(PrevMessID, AfterKey).

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

%% Start included
filter_after_timestamp(Start, Keys) ->
    filter_after_mess_id(hour_lower_bound(Start), Keys).

filter_after_mess_id(_, []) ->
    [];
filter_after_mess_id(MessID, Keys) ->
    AfterKey = replace_mess_id(MessID, hd(Keys)),
    filter_after(AfterKey, Keys).

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

dig_merge(Conn, SecIndex, MessIdxKeyMaker, Digests) ->
    SiblingCnt = length(Digests),
    F1 = fun({Hour, Cnt}, Dict) -> dict:append(Hour, Cnt, Dict) end,
    F2 = fun(Digest, Dict) -> lists:foldl(F1, Dict, Digest) end,
    Hour2Cnts = dict:to_list(lists:foldl(F2, dict:new(), Digests)),
    [{NewHour, NewCnt} ||
        {Hour, Cnts} <- Hour2Cnts,
        {NewHour, NewCnt} <- merge_digest(
                Conn, SecIndex, MessIdxKeyMaker, Hour, Cnts, SiblingCnt)].

merge_digest(Conn, SecIndex, MessIdxKeyMaker, Hour, Cnts, SiblingCnt) ->
    case all_equal(Cnts) andalso length(Cnts) =:= SiblingCnt of
        true -> [{Hour, hd(Cnts)}]; %% good value
        false ->
            %% collision
            case get_hour_entry_count(Conn, SecIndex, MessIdxKeyMaker, Hour) of
                0 -> [];
                NewCnt -> [{Hour, NewCnt}]
            end
    end.

all_equal([H|T]) ->
    all_equal(H, T).

all_equal(H, [H|T]) -> all_equal(H, T);
all_equal(_, [_|_]) -> false;
all_equal(_, []) -> true.


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
get_entry_count_between(Conn, SecIndex, LBound, UBound) when LBound =< UBound ->
    {ok, ?INDEX_RESULTS{keys=Keys}} =
    riakc_pb_socket:get_index_range(Conn, message_bucket(), SecIndex, LBound, UBound),
    length(Keys);
get_entry_count_between(_, _, _, _) ->
    0.

get_hour_entry_count_after(Conn, MessIdxKeyMaker, SecIndex, Start) ->
    LBound = MessIdxKeyMaker({lower, Start}),
    UBound = MessIdxKeyMaker({upper, hour_upper_bound(hour(Start))}),
    get_entry_count_between(Conn, SecIndex, LBound, UBound).

get_hour_entry_count(Conn, SecIndex, MessIdxKeyMaker, Hour) ->
    LBound = MessIdxKeyMaker({lower, hour_lower_bound(Hour)}),
    UBound = MessIdxKeyMaker({upper, hour_upper_bound(Hour)}),
    get_entry_count_between(Conn, SecIndex, LBound, UBound).

get_key_range(Conn, SecIndex, LBound, UBound)
    when is_binary(LBound), is_binary(UBound) ->
    get_key_range(Conn, message_bucket(), SecIndex, LBound, UBound).

get_key_range(Conn, Bucket, SecIndex, LBound, UBound)
    when is_binary(LBound), is_binary(UBound) ->
    {ok, ?INDEX_RESULTS{keys=Keys}} =
    riakc_pb_socket:get_index_range(Conn, Bucket, SecIndex, LBound, UBound),
    Keys.

get_minimum_key_range_before(Conn, MessIdxKeyMaker, SecIndex, Before, PageSize, Digest) ->
    MinHour = page_minimum_hour(PageSize, Digest),
    LBound = MessIdxKeyMaker({lower, hour_lower_bound(MinHour)}),
    UBound = MessIdxKeyMaker({upper, Before}),
    get_key_range(Conn, SecIndex, LBound, UBound).

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
page_maximum_hour_for_tail(PageSize, [_,_|_] = PageDigest) ->
    page_maximum_hour(PageSize, dig_tail(PageDigest));
page_maximum_hour_for_tail(_PageSize, [{Hour, _}]) ->
    Hour.

-spec get_message_rows(term(), [binary()]) -> list(message_row()).
get_message_rows(Conn, Keys) ->
    FullKeys = [{message_bucket(), K} || K <- Keys],
    Values = to_values(Conn, FullKeys),
    MessageRows = deserialize_values(Values),
    lists:usort(MessageRows).

fetch_old_keys(Conn, SecIndex, MessIdxKeyMaker, Now) ->
    UHour = hour(Now) - 1,
    LBound = MessIdxKeyMaker({lower, undefined}),
    UBound = MessIdxKeyMaker({upper, hour_upper_bound(UHour)}),
    get_key_range(Conn, SecIndex, LBound, UBound).


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
       {generator, fun digest_update_case/0}}},

     {"With recent messages by index.",
      {setup,
       fun() -> load_mock(0) end,
       fun(_) -> unload_mock() end,
       {generator, fun index_nothing_from_digest_case/0}}},

     {"Paginate by index without digest (it is not yet generated).",
      {setup,
       fun() -> load_mock(0) end,
       fun(_) -> unload_mock() end,
       {generator, fun index_pagination_case/0}}},

     {"Paginate by index. All entries are in digest. "
      "PageDigest begins in the middle of an hour.",
      {setup,
       fun() -> load_mock(0) end,
       fun(_) -> unload_mock() end,
       {generator, fun only_from_digest_non_empty_hour_offset_case/0}}},

     {"From proper.",
      {setup,
       fun() -> load_mock(0) end,
       fun(_) -> unload_mock() end,
       {generator, fun proper_case/0}}},

     {"Without index digest.",
      {setup,
       fun() -> load_mock(10000), load_data() end,
       fun(_) -> unload_mock() end,
       {timeout, 60, {generator, fun long_case/0 }}}},
     {"With index digest.",
      {setup,
       fun() -> load_mock(0), load_data() end,
       fun(_) -> unload_mock() end,
       {timeout, 60, {generator, fun long_case/0}}}},
     {"Without index digest.",
      {setup,
       fun() -> load_mock(10000), load_data2() end,
       fun(_) -> unload_mock() end,
       {timeout, 60, {generator, fun short_case/0}}}},
     {"With index digest.",
      {setup,
       fun() -> load_mock(0), load_data2() end,
       fun(_) -> unload_mock() end,
       {timeout, 60, {generator, fun short_case/0}}}},
     {"Without index digest.",
      {setup,
       fun() -> load_mock(10000), load_data3() end,
       fun(_) -> unload_mock() end,
       {timeout, 60, {generator, fun incremental_pagination_case/0}}}},
     {"With index digest.",
      {setup,
       fun() -> load_mock(0), load_data3() end,
       fun(_) -> unload_mock() end,
       {timeout, 60, {generator, fun incremental_pagination_case/0}}}},
     {"Without index digest.",
      {setup,
       fun() -> load_mock(10000), load_data3() end,
       fun(_) -> unload_mock() end,
       {timeout, 60, {generator, fun decremental_pagination_case/0}}}},
     {"With index digest.",
      {setup,
       fun() -> load_mock(0), load_data3() end,
       fun(_) -> unload_mock() end,
       {timeout, 60, {generator, fun decremental_pagination_case/0}}}}
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
    OM = riakc_obj,
    meck:new(OM),
    meck:expect(OM, new, fun(Bucket, Key, Value) ->
        Md = [],
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
                lookup_messages(alice(), none,
                    undefined, undefined, test_now(), undefined,
                    0, true, 0))},

    {"First 5.",
    ?_assertKeys(34, 0,
                [join_date_time("2000-07-21", Time)
                 || Time <- ["01:50:00", "01:50:05", "01:50:15",
                             "01:50:16", "01:50:17"]],
                lookup_messages(alice(),
                    none, undefined, undefined, test_now(), undefined,
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

    %% upper_bounded
    {"Last 5 from both conversations with an upper bound.",
    %% lower_bounded
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
                    #rsm_in{direction = before,
                            id = mod_mam_utils:mess_id_to_external_binary(
                                Log2Id("06:50:10"))},
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
                    #rsm_in{direction = before,
                            id = mod_mam_utils:mess_id_to_external_binary(
                                Log2Id("06:50:10"))},
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
                lookup_messages(alice(), none,
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
                            id = mod_mam_utils:mess_id_to_external_binary(
                                Id("2000-07-22", "16:00:05"))},
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
                            id = mod_mam_utils:mess_id_to_external_binary(
                                Id("2000-07-22", "15:59:59"))},
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
                            id = mod_mam_utils:mess_id_to_external_binary(
                                Id("2000-07-28", "00:00:00"))},
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
                            id = mod_mam_utils:mess_id_to_external_binary(
                                Id("2000-07-20", "00:00:00"))},
                    undefined, undefined, test_now(), undefined,
                    5, true, 5))},


    {"After id (last page, part of messages are in RSetDigest).",
    % key_position(Key, Start, End, Digest)
    [?_assertEqual(inside, key_position(Key("2000-07-22", "15:59:59"),
                     undefined, undefined, WholeDigest)),
    %% last_page
    ?_assertKeys(9, 5,
                ["2000-07-22T16:00:05", "2000-07-23T10:34:04",
                 "2000-07-23T10:34:20", "2000-07-23T10:34:23"],
                lookup_messages(alice(),
                    #rsm_in{direction = aft,
                            id = mod_mam_utils:mess_id_to_external_binary(
                                Id("2000-07-22", "15:59:59"))},
                    undefined, undefined, test_now(), undefined,
                    5, true, 5))]},

    {"After id (last page, only recent entries).",
    [?_assertEqual(inside, key_position(Key("2000-07-23", "10:34:23"),
                     undefined, undefined, WholeDigest)),
    %% recent_only
    ?_assertKeys(9, 7,
                ["2000-07-23T10:34:20", "2000-07-23T10:34:23"],
                lookup_messages(alice(),
                    #rsm_in{direction = aft,
                            id = mod_mam_utils:mess_id_to_external_binary(
                                Id("2000-07-23", "10:34:04"))},
                    undefined, undefined, test_now(), undefined,
                    5, true, 5))]},

    {"After id (last page, only recent entries).",
    %% recent_only
    ?_assertKeys(9, 7,
                ["2000-07-23T10:34:20", "2000-07-23T10:34:23"],
                lookup_messages(alice(),
                    #rsm_in{direction = aft,
                            id = mod_mam_utils:mess_id_to_external_binary(
                                Id("2000-07-23", "10:34:04"))},
                    undefined,
                    to_microseconds("2000-07-23", "10:34:24"),
                    test_now(), undefined,
                    5, true, 5))},

    {"After id (last page, only recent entries).",
    %% middle_page
    ?_assertKeys(9, 2,
                ["2000-07-21T13:10:15", "2000-07-21T13:10:16"],
                lookup_messages(alice(),
                    #rsm_in{direction = aft,
                            id = mod_mam_utils:mess_id_to_external_binary(
                                Id("2000-07-21", "13:10:10"))},
                    undefined, undefined, test_now(), undefined,
                    2, true, 2))}
    ].


next_mess_id(PrevMessId) ->
    %% 256 000 000 is one second.
    PrevMessId + 256 * random_microsecond_delay().

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
    [MessId|put_messages(next_mess_id(MessId), N - 1)];
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
                lookup_messages(alice(), none,
                    undefined, undefined, test_now(), undefined,
                    0, true, 0))},
     %% 0 id is before any message id in the archive.
     incremental_pagination_gen(0, 1, 10, 0, MessIDs, TotalCount)].

incremental_pagination_gen(
    LastMessID, PageNum, PageSize, Offset, MessIDs, TotalCount)
    when is_integer(LastMessID) ->
    {ok, {ResultTotalCount, ResultOffset, ResultRows}} =
    ?MODULE:lookup_messages(alice(),
        #rsm_in{direction = aft,
            id = mod_mam_utils:mess_id_to_external_binary(LastMessID)},
        undefined, undefined, test_now(), undefined,
        PageSize, true, PageSize),
    ResultMessIDs = [message_row_to_mess_id(Row) || Row <- ResultRows],
    {PageMessIDs, LeftMessIDs} = save_split(PageSize, MessIDs),
    NewOffset = Offset + length(ResultRows),
    NewLastMessID = lists:last(PageMessIDs),
    [{"Page " ++ integer_to_list(PageNum),
     [?_assertEqual(TotalCount, ResultTotalCount),
      ?_assertEqual(Offset, ResultOffset),
      ?_assertEqual(PageMessIDs, ResultMessIDs)]}
    | case NewOffset >= TotalCount of
        true -> [];
        false ->
            {generator, fun() -> incremental_pagination_gen(
                NewLastMessID, PageNum + 1, PageSize,
                NewOffset, LeftMessIDs,  TotalCount)
             end}
      end].

decremental_pagination_case() ->
    MessIDs = [key_to_mess_id(Key) || Key <- all_keys(message_bucket())],
    TotalCount = length(MessIDs),
    PageSize = 10,
    PageNum = TotalCount div PageSize,
    Offset = TotalCount - PageSize,
    [{"Trigger digest creation.",
    ?_assertKeys(TotalCount, 0, [],
                lookup_messages(alice(), none,
                    undefined, undefined, test_now(), undefined,
                    0, true, 0))},
     decremental_pagination_gen(
            undefined, PageNum, PageSize, Offset, MessIDs, TotalCount)].

decremental_pagination_gen(
    BeforeMessID, PageNum, PageSize, Offset, MessIDs, TotalCount) ->
    {ok, {ResultTotalCount, ResultOffset, ResultRows}} =
    ?MODULE:lookup_messages(alice(),
        #rsm_in{direction = before,
            id = maybe_mess_id_to_external_binary(BeforeMessID)},
        undefined, undefined, test_now(), undefined,
        PageSize, true, PageSize),
    ResultMessIDs = [message_row_to_mess_id(Row) || Row <- ResultRows],
    {LeftMessIDs, PageMessIDs} = save_split(Offset, MessIDs),
    NewOffset = Offset - length(ResultRows),
    NewBeforeMessID = hd(PageMessIDs),
    [{"Page " ++ integer_to_list(PageNum),
     [?_assertEqual(TotalCount, ResultTotalCount),
      ?_assertEqual(Offset, ResultOffset),
      ?_assertEqual(PageMessIDs, ResultMessIDs)]}
    | case NewOffset =< 0 of
        true -> [];
        false ->
            {generator, fun() -> decremental_pagination_gen(
                NewBeforeMessID, PageNum - 1, PageSize,
                NewOffset, LeftMessIDs,  TotalCount)
             end}
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
        lookup_messages(alice(), none, undefined, undefined,
            to_microseconds("2000-01-01", "13:40:00"), undefined, 10, true, 10)),
    %% 13:50
    set_now(to_microseconds("2000-01-01", "13:50:00")),
    archive_message(Id("2000-01-01T13:50:00Z"),
        outgoing, alice(), cat(), alice(), packet()),
    %% 14:30
    set_now(to_microseconds("2000-01-01", "14:30:00")),
    Generators2 =
    assert_keys(2, 0, ["2000-01-01T13:30:00", "2000-01-01T13:50:00"],
        lookup_messages(alice(), none, undefined, undefined,
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
        lookup_messages(alice(), none, undefined, undefined,
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
    lookup_messages(cat(), none, undefined, undefined,
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
    [].

%% `lists:split/3' that does allow small lists.
%% `lists:split/2' is already save.
save_split(N, L) when length(L) < N ->
    {L, []};
save_split(N, L) ->
    lists:split(N, L).


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
    {Microseconds, _} = mod_mam_utils:decode_compact_uuid(MessID),
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

id(Date) when is_binary(Date) ->
    Microseconds = mod_mam_utils:maybe_microseconds(Date),
    encode_compact_uuid(Microseconds, 1).

id() ->
    Microseconds = get_now(),
    encode_compact_uuid(Microseconds, 1).
    

date_to_hour(Date, Time) ->
    date_to_hour(list_to_binary(join_date_time_z(Date, Time))).

date_to_hour(DateTime) when is_binary(DateTime) ->
    Microseconds = mod_mam_utils:maybe_microseconds(DateTime),
    hour(Microseconds).

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


page_minimum_hour_test_() ->
    [?_assertEqual(3, page_minimum_hour( 5, [{1, 6}, {2, 5}, {3, 8}])),
     ?_assertEqual(2, page_minimum_hour(10, [{1, 6}, {2, 5}, {3, 8}])),
     ?_assertEqual(1, page_minimum_hour(20, [{1, 6}, {2, 5}, {3, 8}]))
    ].


maybe_mess_id_to_external_binary(undefined) ->
    undefined;
maybe_mess_id_to_external_binary(MessID) ->
    mod_mam_utils:mess_id_to_external_binary(MessID).



save_sublist_test_() ->
    [?_assertEqual(save_sublist([1,2,3], 1, 10), [1,2,3])
    ,?_assertEqual(save_sublist([1,2,3], 2, 10), [2,3])
    ,?_assertEqual(save_sublist([1,2,3], 3, 10), [3])
    ,?_assertEqual(save_sublist([1,2,3], 4, 10), [])
    ].

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

save_sublist(L, S, C) when S =< length(L) ->
    lists:sublist(L, S, C);
save_sublist(_, _, _) ->
    [].

is_single_page(PageSize, PageDigest) ->
    dig_total(PageDigest) =< PageSize.
