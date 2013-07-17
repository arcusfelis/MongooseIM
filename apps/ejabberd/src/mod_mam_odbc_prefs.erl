-module(mod_mam_odbc_prefs).
-export([get_behaviour/3,
         get_prefs/3,
         update_settings/5,
         remove_user_from_db/2]).

-include_lib("ejabberd/include/ejabberd.hrl").
-include_lib("ejabberd/include/jlib.hrl").
-include_lib("exml/include/exml.hrl").

get_behaviour(DefaultBehaviour,
              #jid{lserver=LocLServer, luser=LocLUser},
              #jid{} = RemJID) ->
    RemLJID      = jlib:jid_tolower(RemJID),
    SRemLBareJID = esc_jid(jlib:jid_remove_resource(RemLJID)),
    SRemLJID     = esc_jid(jlib:jid_tolower(RemJID)),
    UserID       = mod_mam_cache:user_id(LocLServer, LocLUser),
    SUserID      = integer_to_list(UserID),
    case query_behaviour(LocLServer, SUserID, SRemLJID, SRemLBareJID) of
        {selected, ["behaviour"], [{Behavour}]} ->
            decode_behaviour(Behavour);
        _ -> DefaultBehaviour
    end.

update_settings(LServer, LUser, DefaultMode, AlwaysJIDs, NeverJIDs) ->
    UserID = mod_mam_cache:user_id(LServer, LUser),
    SUserID = integer_to_list(UserID),
    DelQuery = ["DELETE FROM mam_config WHERE user_id = '", SUserID, "'"],
    InsQuery = ["INSERT INTO mam_config(user_id, behaviour, remote_jid) "
       "VALUES ", encode_first_config_row(SUserID, encode_behaviour(DefaultMode), ""),
       [encode_config_row(SUserID, "A", ejabberd_odbc:escape(JID))
        || JID <- AlwaysJIDs],
       [encode_config_row(SUserID, "N", ejabberd_odbc:escape(JID))
        || JID <- NeverJIDs]],
    %% Run as a transaction
    {atomic, [{updated, _}, {updated, 1}]} =
        sql_transaction_map(LServer, [DelQuery, InsQuery]),
    ok.

get_prefs(LServer, LUser, GlobalDefaultMode) ->
    UserID = mod_mam_cache:user_id(LServer, LUser),
    SUserID = integer_to_list(UserID),
    {selected, _ColumnNames, Rows} =
    ejabberd_odbc:sql_query(
      LServer,
      ["SELECT remote_jid, behaviour "
       "FROM mam_config "
       "WHERE user_id='", SUserID, "'"]),
    decode_prefs_rows(Rows, GlobalDefaultMode, [], []).

remove_user_from_db(LServer, LUser) ->
    UserID = mod_mam_cache:user_id(LServer, LUser),
    SUserID = integer_to_list(UserID),
    {updated, _} =
    ejabberd_odbc:sql_query(
      LServer,
      ["DELETE "
       "FROM mam_config "
       "WHERE user_id='", SUserID, "'"]),
    ok.

query_behaviour(LServer, SUserID, SRemLJID, SRemLBareJID) ->
    Result =
    ejabberd_odbc:sql_query(
      LServer,
      ["SELECT behaviour "
       "FROM mam_config "
       "WHERE user_id='", SUserID, "' "
         "AND (remote_jid='' OR remote_jid='", SRemLJID, "'",
               case SRemLBareJID of
                    SRemLJID -> "";
                    _        -> [" OR remote_jid='", SRemLBareJID, "'"]
               end,
         ") "
       "ORDER BY remote_jid DESC "
       "LIMIT 1"]),
    ?DEBUG("query_behaviour query returns ~p", [Result]),
    Result.

%% ----------------------------------------------------------------------
%% Helpers

encode_behaviour(roster) -> "R";
encode_behaviour(always) -> "A";
encode_behaviour(never)  -> "N".

decode_behaviour(<<"R">>) -> roster;
decode_behaviour(<<"A">>) -> always;
decode_behaviour(<<"N">>) -> never.

esc_jid(JID) ->
    ejabberd_odbc:escape(jlib:jid_to_binary(JID)).

encode_first_config_row(SUserID, SBehavour, SJID) ->
    ["('", SUserID, "', '", SBehavour, "', '", SJID, "')"].

encode_config_row(SUserID, SBehavour, SJID) ->
    [", ('", SUserID, "', '", SBehavour, "', '", SJID, "')"].

sql_transaction_map(LServer, Queries) ->
    AtomicF = fun() ->
        [ejabberd_odbc:sql_query(LServer, Query) || Query <- Queries]
    end,
    ejabberd_odbc:sql_transaction(LServer, AtomicF).

decode_prefs_rows([{<<>>, Behavour}|Rows], _DefaultMode, AlwaysJIDs, NeverJIDs) ->
    decode_prefs_rows(Rows, decode_behaviour(Behavour), AlwaysJIDs, NeverJIDs);
decode_prefs_rows([{JID, <<"A">>}|Rows], DefaultMode, AlwaysJIDs, NeverJIDs) ->
    decode_prefs_rows(Rows, DefaultMode, [JID|AlwaysJIDs], NeverJIDs);
decode_prefs_rows([{JID, <<"N">>}|Rows], DefaultMode, AlwaysJIDs, NeverJIDs) ->
    decode_prefs_rows(Rows, DefaultMode, AlwaysJIDs, [JID|NeverJIDs]);
decode_prefs_rows([], DefaultMode, AlwaysJIDs, NeverJIDs) ->
    {DefaultMode, AlwaysJIDs, NeverJIDs}.


