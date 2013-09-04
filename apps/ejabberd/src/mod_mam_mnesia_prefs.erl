%%%-------------------------------------------------------------------
%%% @author Uvarov Michael <arcusfelis@gmail.com>
%%% @copyright (C) 2013, Uvarov Michael
%%% @doc A backend for storing MAM preferencies using Mnesia.
%%% @end
%%%-------------------------------------------------------------------
-module(mod_mam_mnesia_prefs).
-export([start/1,
         get_behaviour/3,
         get_prefs/3,
         set_prefs/5,
         remove_archive/2]).

-include_lib("ejabberd/include/ejabberd.hrl").
-include_lib("ejabberd/include/jlib.hrl").
-include_lib("exml/include/exml.hrl").

-record(mam_prefs_rule, {key, behaviour}).
-record(mam_prefs_user, {host_user, default_mode, always_rules, never_rules}).

start(_Host) ->
    mnesia:create_table(mam_prefs_rule,
            [{disc_copies, [node()]},
             {attributes, record_info(fields, mam_prefs_rule)}]),
    mnesia:create_table(mam_prefs_user,
            [{disc_copies, [node()]},
             {attributes, record_info(fields, mam_prefs_user)}]),
    ok.

get_behaviour(DefaultBehaviour,
              LocJID=#jid{},
              RemJID=#jid{}) ->
    case mnesia:dirty_read(mam_prefs_rule, key1(LocJID, RemJID)) of
        [] ->
            case mnesia:dirty_read(mam_prefs_rule, key2(LocJID, RemJID)) of
                [] ->
                    case mnesia:dirty_read(mam_prefs_rule, key3(LocJID)) of
                        [] -> DefaultBehaviour;
                        [#mam_prefs_rule{behaviour=B}] -> B
                    end;
                [#mam_prefs_rule{behaviour=B}] -> B
            end;
        [#mam_prefs_rule{behaviour=B}] -> B
    end.

set_prefs(LServer, LUser, DefaultMode, AlwaysJIDs, NeverJIDs) ->
    NewARules = lists:usort(rules(AlwaysJIDs)),
    NewNRules = lists:usort(rules(NeverJIDs)),
    SU = {LServer, LUser},
    {atomic, ok} = mnesia:transaction(fun() ->
        case mnesia:read(mam_prefs_user, SU) of
            [] -> 
                update_rules(always, LServer, LUser, [], NewARules),
                update_rules(newer, LServer, LUser, [], NewNRules),
                mnesia:write(#mam_prefs_user{
                             host_user=SU, default_mode=DefaultMode,
                             always_rules=NewARules, never_rules=NewNRules});
            [#mam_prefs_user{default_mode=DefaultMode,
                             always_rules=NewARules, never_rules=NewNRules}] ->
                ok; %% same
            [#mam_prefs_user{always_rules=OldARules, never_rules=OldNRules}] ->
                update_rules(always, LServer, LUser, OldARules, NewARules),
                update_rules(newer, LServer, LUser, OldNRules, NewNRules),
                mnesia:write(#mam_prefs_user{
                             host_user=SU, default_mode=DefaultMode,
                             always_rules=NewARules, never_rules=NewNRules}),
                ok
        end
    end),
    ok.

get_prefs(LServer, LUser, GlobalDefaultMode) ->
    case mnesia:dirty_read(mam_prefs_user, {LServer, LUser}) of
        [] -> 
            {GlobalDefaultMode, [], []};
        [#mam_prefs_user{default_mode=DefaultMode,
                         always_rules=ARules, never_rules=NRules}] ->
            AlwaysJIDs = jids(ARules),
            NeverJIDs = jids(NRules),
            {DefaultMode, AlwaysJIDs, NeverJIDs}
    end.

remove_archive(LServer, LUser) ->
    {atomic, ok} = mnesia:transaction(fun() ->
        case mnesia:read(mam_prefs_user, {LServer, LUser}) of
            [] -> ok; %% already deleted
            [#mam_prefs_user{always_rules=ARules, never_rules=NRules}] ->
                Rules = [key3(LServer, LUser)] ++ ARules ++ NRules,
                Keys = [key(LServer, LUser, Rule) || Rule <- Rules],
                [mnesia:delete(mam_prefs_rule, Key, write) || Key <- Keys],
                ok
        end
    end),
    ok.

%% ----------------------------------------------------------------------
%% Helpers

key1(#jid{lserver=LocLServer, luser=LocLUser},
     #jid{lserver=RemLUser, server=RemLServer, lresource=RemLResource}) ->
    {LocLServer, LocLUser, RemLUser, RemLServer, RemLResource}.

key2(#jid{lserver=LocLServer, luser=LocLUser},
     #jid{lserver=RemLUser, server=RemLServer}) ->
    {LocLServer, LocLUser, RemLUser, RemLServer}.

key3(#jid{lserver=LocLServer, luser=LocLUser}) ->
    {LocLServer, LocLUser}.

key3(LocLServer, LocLUser) ->
    {LocLServer, LocLUser}.

%% @doc Expand short rule.
key(LServer, LUser, {RemLServer, RemLUser}) ->
    {LServer, LUser, RemLServer, RemLUser}; %% key1
key(LServer, LUser, {RemLServer, RemLUser, RemLResource}) ->
    {LServer, LUser, RemLServer, RemLUser, RemLResource}. %% key2

jids(Rules) ->
    [jlib:jid_to_binary(rule_to_jid(Rule)) || Rule <- Rules].

rule_to_jid({RemLServer, RemLUser, RemLResource}) ->
    {RemLUser, RemLServer, RemLResource};
rule_to_jid({RemLServer, RemLUser}) ->
    {RemLUser, RemLServer, <<>>}.

rules(BinJIDs) ->
    [rule(jlib:binary_to_jid(BinJID)) || BinJID <- BinJIDs].

rule(#jid{lserver=RemLServer, luser=RemLUser, lresource = <<>>}) ->
    {RemLServer, RemLUser};
rule(#jid{lserver=RemLServer, luser=RemLUser, lresource=RemLResource}) ->
    {RemLServer, RemLUser, RemLResource}.


update_rules(Mode, LServer, LUser, OldNRules, NewNRules) ->
    DelRules = ordsets:subtract(OldNRules, NewNRules),
    InsRules = ordsets:subtract(NewNRules, OldNRules),
    DelKeys = [key(LServer, LUser, Rule) || Rule <- DelRules],
    InsKeys = [key(LServer, LUser, Rule) || Rule <- InsRules],
    [mnesia:delete(mam_prefs_rule, Key, write) || Key <- DelKeys],
    [mnesia:write(#mam_prefs_rule{key=Key, behaviour=Mode}) || Key <- InsKeys],
    ok.
