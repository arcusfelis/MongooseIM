%%%----------------------------------------------------------------------
%%% File    : mod_vcard.erl
%%% Author  : Alexey Shchepin <alexey@process-one.net>
%%% Purpose : vCard support via RDBMS
%%% Created :  2 Jan 2003 by Alexey Shchepin <alexey@process-one.net>
%%%
%%%
%%% ejabberd, Copyright (C) 2002-2011   ProcessOne
%%%
%%% This program is free software; you can redistribute it and/or
%%% modify it under the terms of the GNU General Public License as
%%% published by the Free Software Foundation; either version 2 of the
%%% License, or (at your option) any later version.
%%%
%%% This program is distributed in the hope that it will be useful,
%%% but WITHOUT ANY WARRANTY; without even the implied warranty of
%%% MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.  See the GNU
%%% General Public License for more details.
%%%
%%% You should have received a copy of the GNU General Public License
%%% along with this program; if not, write to the Free Software
%%% Foundation, Inc., 51 Franklin Street, Fifth Floor, Boston, MA 02110-1301 USA
%%%
%%%----------------------------------------------------------------------

-module(mod_vcard_rdbms).

-behaviour(mod_vcard).

%% mod_vcards callbacks
-export([init/2,
         remove_user/2,
         get_vcard/2,
         set_vcard/4,
         search/2,
         search_fields/1,
         search_reported_fields/2]).

-include("mongoose.hrl").
-include("jlib.hrl").
-include("mod_vcard.hrl").

init(_VHost, _Options) ->
    ok.

remove_user(LUser, LServer) ->
    Username = mongoose_rdbms:escape_string(LUser),
    mongoose_rdbms:sql_transaction(
      LServer,
      [["delete from vcard where username=", mongoose_rdbms:use_escaped_string(Username), ";"],
       ["delete from vcard_search where lusername=", mongoose_rdbms:use_escaped_string(Username), ";"]]).

get_vcard(LUser, LServer) ->
    U = mongoose_rdbms:escape_string(LUser),
    S = mongoose_rdbms:escape_string(LServer),
    case rdbms_queries:get_vcard(LServer, U, S) of
        {selected, [{SVCARD}]} ->
            case exml:parse(SVCARD) of
                {error, Reason} ->
                    ?LOG_WARNING(#{what => vcard_corrupted,
                                   text => <<"Not sending back bad vCard XML">>,
                                   reason => Reason, svcard => SVCARD,
                                   user => LUser, host => LServer}),
                    {error, mongoose_xmpp_errors:service_unavailable()};
                {ok, VCARD} ->
                    {ok, [VCARD]}
            end;
        {selected, []} ->
            {error, mongoose_xmpp_errors:item_not_found()}
    end.

set_vcard(User, VHost, VCard, VCardSearch) ->
    LUser = jid:nodeprep(User),
    SUsername = mongoose_rdbms:escape_string(User),
    SLUsername = mongoose_rdbms:escape_string(LUser),
    SLServer = mongoose_rdbms:escape_string(VHost),
    SVCARD = mongoose_rdbms:escape_string(exml:to_binary(VCard)),

    SFN = mongoose_rdbms:escape_string(VCardSearch#vcard_search.fn),
    SLFN = mongoose_rdbms:escape_string(VCardSearch#vcard_search.lfn),
    SFamily = mongoose_rdbms:escape_string(VCardSearch#vcard_search.family),
    SLFamily = mongoose_rdbms:escape_string(VCardSearch#vcard_search.lfamily),
    SGiven = mongoose_rdbms:escape_string(VCardSearch#vcard_search.given),
    SLGiven = mongoose_rdbms:escape_string(VCardSearch#vcard_search.lgiven),
    SMiddle = mongoose_rdbms:escape_string(VCardSearch#vcard_search.middle),
    SLMiddle = mongoose_rdbms:escape_string(VCardSearch#vcard_search.lmiddle),
    SNickname = mongoose_rdbms:escape_string(VCardSearch#vcard_search.nickname),
    SLNickname = mongoose_rdbms:escape_string(VCardSearch#vcard_search.lnickname),
    SBDay = mongoose_rdbms:escape_string(VCardSearch#vcard_search.bday),
    SLBDay = mongoose_rdbms:escape_string(VCardSearch#vcard_search.lbday),
    SCTRY = mongoose_rdbms:escape_string(VCardSearch#vcard_search.ctry),
    SLCTRY = mongoose_rdbms:escape_string(VCardSearch#vcard_search.lctry),
    SLocality = mongoose_rdbms:escape_string(VCardSearch#vcard_search.locality),
    SLLocality = mongoose_rdbms:escape_string(VCardSearch#vcard_search.llocality),
    SEMail = mongoose_rdbms:escape_string(VCardSearch#vcard_search.email),
    SLEMail = mongoose_rdbms:escape_string(VCardSearch#vcard_search.lemail),
    SOrgName = mongoose_rdbms:escape_string(VCardSearch#vcard_search.orgname),
    SLOrgName = mongoose_rdbms:escape_string(VCardSearch#vcard_search.lorgname),
    SOrgUnit = mongoose_rdbms:escape_string(VCardSearch#vcard_search.orgunit),
    SLOrgUnit = mongoose_rdbms:escape_string(VCardSearch#vcard_search.lorgunit),

    rdbms_queries:set_vcard(VHost,
                           SLServer, SLUsername, SBDay, SCTRY, SEMail,
                           SFN, SFamily, SGiven, SLBDay, SLCTRY,
                           SLEMail, SLFN, SLFamily, SLGiven,
                           SLLocality, SLMiddle, SLNickname,
                           SLOrgName, SLOrgUnit, SLocality,
                           SMiddle, SNickname, SOrgName,
                           SOrgUnit, SVCARD, SUsername),

    mongoose_hooks:vcard_set(VHost, ok, LUser, VCard),
    ok.

search(LServer, Data) ->
    RestrictionSQL = make_restriction_sql(LServer, Data),
    R = do_search(LServer, RestrictionSQL),
    lists:map(fun(I) -> record_to_item(LServer, I) end, R).

do_search(_LServer, "") ->
    [];
do_search(LServer, RestrictionSQL) ->
    Limit = mod_vcard:get_results_limit(LServer),
    case catch rdbms_queries:search_vcard(LServer, RestrictionSQL, Limit) of
        {selected, Rs} when is_list(Rs) ->
            Rs;
        Error ->
            ?LOG_ERROR(#{what => vcard_db_search_failed,
                         reason => Error, host => LServer}),
            []
    end.

search_fields(_VHost) ->
    mod_vcard:default_search_fields().

search_reported_fields(_VHost, Lang) ->
    mod_vcard:get_default_reported_fields(Lang).

%%--------------------------------------------------------------------
%% internal
%%--------------------------------------------------------------------
make_restriction_sql(LServer, Data) ->
    filter_fields(Data, "", LServer).

filter_fields([], "", _LServer) ->
    "";
filter_fields([], RestrictionSQLIn, LServer) ->
    [" where ", RestrictionSQLIn, " and ",
     "server = ", mongoose_rdbms:use_escaped_string(mongoose_rdbms:escape_string(LServer))];
filter_fields([{SVar, [Val]} | Ds], RestrictionSQL, LServer)
  when is_binary(Val) and (Val /= <<"">>) ->
    LVal = jid:str_tolower(Val),
    NewRestrictionSQL =
        case SVar of
            <<"user">>     -> make_val(RestrictionSQL, "lusername", LVal);
            <<"fn">>       -> make_val(RestrictionSQL, "lfn",       LVal);
            <<"last">>     -> make_val(RestrictionSQL, "lfamily",   LVal);
            <<"first">>    -> make_val(RestrictionSQL, "lgiven",    LVal);
            <<"middle">>   -> make_val(RestrictionSQL, "lmiddle",   LVal);
            <<"nick">>     -> make_val(RestrictionSQL, "lnickname", LVal);
            <<"bday">>     -> make_val(RestrictionSQL, "lbday",     LVal);
            <<"ctry">>     -> make_val(RestrictionSQL, "lctry",     LVal);
            <<"locality">> -> make_val(RestrictionSQL, "llocality", LVal);
            <<"email">>    -> make_val(RestrictionSQL, "lemail",    LVal);
            <<"orgname">>  -> make_val(RestrictionSQL, "lorgname",  LVal);
            <<"orgunit">>  -> make_val(RestrictionSQL, "lorgunit",  LVal);
            _              -> RestrictionSQL
        end,
    filter_fields(Ds, NewRestrictionSQL, LServer);
filter_fields([_ | Ds], RestrictionSQL, LServer) ->
    filter_fields(Ds, RestrictionSQL, LServer).

-spec make_val(RestrictionSQL, Field, Val) -> Result when
    RestrictionSQL :: iolist(),
    Field :: string(),
    Val :: binary(),
    Result :: iolist().
make_val(RestrictionSQL, Field, Val) ->
    Condition =
        case binary:last(Val) of
            $* ->
                Val1 = binary:part(Val, 0, byte_size(Val)-1),
                SVal = mongoose_rdbms:escape_like_prefix(Val1),
                [Field, " LIKE ", mongoose_rdbms:use_escaped_like(SVal)];
            _ ->
                SVal = mongoose_rdbms:escape_string(Val),
                [Field, " = ", mongoose_rdbms:use_escaped_string(SVal)]
        end,
    case RestrictionSQL of
        "" ->
            Condition;
        _ ->
            [RestrictionSQL, " and ", Condition]
    end.

record_to_item(_CallerVHost, {Username, VCardVHost, FN, Family, Given, Middle,
             Nickname, BDay, CTRY, Locality,
             EMail, OrgName, OrgUnit}) ->
    #xmlel{name = <<"item">>,
           children = [
                        ?FIELD(<<"jid">>, <<Username/binary, "@", VCardVHost/binary>>),
                        ?FIELD(<<"fn">>, FN),
                        ?FIELD(<<"last">>, Family),
                        ?FIELD(<<"first">>, Given),
                        ?FIELD(<<"middle">>, Middle),
                        ?FIELD(<<"nick">>, Nickname),
                        ?FIELD(<<"bday">>, BDay),
                        ?FIELD(<<"ctry">>, CTRY),
                        ?FIELD(<<"locality">>, Locality),
                        ?FIELD(<<"email">>, EMail),
                        ?FIELD(<<"orgname">>, OrgName),
                        ?FIELD(<<"orgunit">>, OrgUnit)
                       ]}.
