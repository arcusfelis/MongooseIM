%%%-------------------------------------------------------------------
%%% @author Uvarov Michael <arcusfelis@gmail.com>
%%% @copyright (C) 2013, Uvarov Michael
%%% @doc Stores MAM preferencies for rooms using ODBC.
%%% @end
%%%-------------------------------------------------------------------
-module(mod_mam_muc_odbc_prefs).

%% Client API
-export([create_room_archive/2,
         delete_archive/2,
         delete_archive_after_destruction/2,
         delete_archive_after_destruction/3,
         enable_logging/3,
         enable_querying/3]).

%% Handlers
-export([get_behaviour/3,
         get_prefs/3,
         set_prefs/5,
         remove_user_from_db/2]).

-include_lib("ejabberd/include/ejabberd.hrl").
-include_lib("ejabberd/include/jlib.hrl").
-include_lib("exml/include/exml.hrl").

%% ----------------------------------------------------------------------
%% Settings getters

delete_archive_after_destruction(LServer, RoomName) ->
    rewrite_undefined(true,
        select_bool(LServer, RoomName,
                    "delete_archive_after_destruction")).

rewrite_undefined(Def, undefined) -> Def;
rewrite_undefined(_, Val)         -> Val.

%% ----------------------------------------------------------------------
%% Settings setters

%% @doc All messages will be deleted after the room is destroyed.
%% If `DeleteIt' is true, than messages will be lost.
%% If `DeleteIt' is false, than messages will be stored.
%% If `DeleteIt' is undefined, then the default behaviour will be chosen.
delete_archive_after_destruction(LServer, RoomName, DeleteIt) ->
    set_bool(LServer, RoomName,
             "delete_archive_after_destruction", DeleteIt),
    ok.

%% @doc Enable logging for the room.
enable_logging(LServer, RoomName, Enabled) ->
    set_bool(LServer, RoomName, "enable_logging", Enabled),
    mod_mam_muc_cache:update_logging_enabled(LServer, RoomName, Enabled),
    ok.

%% @doc Enable access to the archive for the room.
enable_querying(LServer, RoomName, Enabled) ->
    set_bool(LServer, RoomName, "enable_querying", Enabled),
    mod_mam_muc_cache:update_querying_enabled(LServer, RoomName, Enabled),
    ok.


%% ----------------------------------------------------------------------
%% API

delete_archive(LServer, RoomName) ->
    RoomId = mod_mam_muc_cache:room_id(LServer, RoomName),
    SRoomId = integer_to_list(RoomId),
    %% TODO: use transaction
    {updated, _} =
    mod_mam_utils:success_sql_query(LServer,
    ["DELETE FROM mam_muc_message WHERE room_id = '", SRoomId, "'"]),
    {updated, _} =
    mod_mam_utils:success_sql_query(LServer,
    ["DELETE FROM mam_muc_room WHERE id = '", SRoomId, "'"]),
    true.

create_room_archive(LServer, RoomName) ->
    SRoomName = ejabberd_odbc:escape(RoomName),
    mod_mam_utils:success_sql_query(
      LServer,
      ["INSERT INTO mam_muc_room(room_name) "
       "VALUES ('", SRoomName,"')"]),
    ok.

%% ----------------------------------------------------------------------
%% Handlers

get_behaviour(DefaultBehaviour,
              #jid{lserver=LServer, luser=RoomName},
              #jid{} = _RemJID) ->
    %% TODO: handle "roster" (registered user).
    rewrite_undefined(DefaultBehaviour,
        mod_mam_muc_cache:is_logging_enabled(LServer, RoomName)).

%% @doc Delete all messages from the room.
remove_user_from_db(LServer, RoomName) ->
    case delete_archive_after_destruction(LServer, RoomName) of
        true -> delete_archive(LServer, RoomName);
        false -> false
    end.

set_prefs(LServer, LUser, DefaultMode, _AlwaysJIDs, _NeverJIDs) ->
    enable_logging(LServer, LUser, DefaultMode =/= newer),
    ok.

get_prefs(LServer, LUser, _GlobalDefaultMode) ->
    case mod_mam_muc_cache:is_logging_enabled(LServer, LUser) of
        true -> {always, [], []};
        false -> {newer, [], []}
    end.

%% ----------------------------------------------------------------------
%% SQL Internal functions

sql_bool(true)      -> "'1'";
sql_bool(false)     -> "'0'";
sql_bool(undefined) -> "null".

set_bool(LServer, RoomName, FieldName, FieldValue) ->
    RoomId = mod_mam_muc_cache:room_id(LServer, RoomName),
    SRoomId = integer_to_list(RoomId),
    mod_mam_utils:success_sql_query(
      LServer,
      ["UPDATE mam_muc_room "
       "SET ", FieldName, " = ", sql_bool(FieldValue), " "
       "WHERE id = '", SRoomId, "'"]).

select_bool(LServer, RoomName, Field) ->
    SRoomName = ejabberd_odbc:escape(RoomName),
    Result =
    mod_mam_utils:success_sql_query(
      LServer,
      ["SELECT " ++ Field ++ " "
       "FROM mam_muc_room "
       "WHERE room_name='", SRoomName, "' "
       "LIMIT 1"]),
    case Result of
        {selected, [Field], [{Res}]} ->
            case Res of
                null    -> undefined;
                <<"1">> -> true;
                <<"0">> -> false
            end;
        {selected, [Field], []} ->
            %% The room is not found
            create_room_archive(LServer, RoomName),
            undefined
    end.


%% ----------------------------------------------------------------------
%% Helpers


