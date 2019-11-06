%% Spec examples:
%%
%%   {suites, "tests", amp_SUITE}.
%%   {groups, "tests", amp_SUITE, [discovery]}.
%%   {groups, "tests", amp_SUITE, [discovery], {cases, [stream_feature_test]}}.
%%   {cases, "tests", amp_SUITE, [stream_feature_test]}.
%%
%% For more info see:
%% http://www.erlang.org/doc/apps/common_test/run_test_chapter.html#test_specifications

%% do not remove below SUITE if testing mongoose
{suites, "tests", mongoose_sanity_checks_SUITE}.

{suites, "tests", muc_SUITE}.
{suites, "tests", muc_light_SUITE}.
{suites, "tests", muc_light_legacy_SUITE}.
{suites, "tests", muc_http_api_SUITE}.
{suites, "tests", muc_light_http_api_SUITE}.
{suites, "tests", oauth_SUITE}. % pgsql specific
{suites, "tests", presence_SUITE}.
{suites, "tests", rest_client_SUITE}.
{suites, "tests", s2s_SUITE}.
{suites, "tests", shared_roster_SUITE}.
{suites, "tests", sic_SUITE}.
{suites, "tests", users_api_SUITE}.
{suites, "tests", mongoose_cassandra_SUITE}.
{suites, "tests", mongoose_elasticsearch_SUITE}.

{specs, join, ["base.spec"]}.
