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
    SLocLUser    = ejabberd_odbc:escape(LocLUser),
    SRemLBareJID = esc_jid(jlib:jid_remove_resource(RemLJID)),
    SRemLJID     = esc_jid(jlib:jid_tolower(RemJID)),
    case query_behaviour(LocLServer, SLocLUser, SRemLJID, SRemLBareJID) of
        {selected, ["behaviour"], [{Behavour}]} ->
            decode_behaviour(Behavour);
        _ -> DefaultBehaviour
    end.

update_settings(LServer, LUser, DefaultMode, AlwaysJIDs, NeverJIDs) ->
    SUser = ejabberd_odbc:escape(LUser),
    DelQuery = ["DELETE FROM mam_config WHERE local_username = '", SUser, "'"],
    InsQuery = ["INSERT INTO mam_config(local_username, behaviour, remote_jid) "
       "VALUES ", encode_first_config_row(SUser, encode_behaviour(DefaultMode), ""),
       [encode_config_row(SUser, "A", ejabberd_odbc:escape(JID))
        || JID <- AlwaysJIDs],
       [encode_config_row(SUser, "N", ejabberd_odbc:escape(JID))
        || JID <- NeverJIDs]],
    %% Run as a transaction
    {atomic, [DelResult, InsResult]} =
        sql_transaction_map(LServer, [DelQuery, InsQuery]),
    ?DEBUG("update_settings query returns ~p and ~p", [DelResult, InsResult]),
    ok.

get_prefs(LServer, LUser, GlobalDefaultMode) ->
    SUser = ejabberd_odbc:escape(LUser),
    {selected, _ColumnNames, Rows} =
    ejabberd_odbc:sql_query(
      LServer,
      ["SELECT remote_jid, behaviour "
       "FROM mam_config "
       "WHERE local_username='", SUser, "'"]),
    decode_prefs_rows(Rows, GlobalDefaultMode, [], []).

remove_user_from_db(LServer, LUser) ->
    SUser = ejabberd_odbc:escape(LUser),
    ejabberd_odbc:sql_query(
      LServer,
      ["DELETE "
       "FROM mam_config "
       "WHERE local_username='", SUser, "'"]),
    ok.

query_behaviour(LServer, SUser, SRemLJID, SRemLBareJID) ->
    Result =
    ejabberd_odbc:sql_query(
      LServer,
      ["SELECT behaviour "
       "FROM mam_config "
       "WHERE local_username='", SUser, "' "
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

encode_first_config_row(SUser, SBehavour, SJID) ->
    ["('", SUser, "', '", SBehavour, "', '", SJID, "')"].

encode_config_row(SUser, SBehavour, SJID) ->
    [", ('", SUser, "', '", SBehavour, "', '", SJID, "')"].

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


