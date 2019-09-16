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

{suites, "tests", race_conditions_SUITE}.
{suites, "tests", acc_e2e_SUITE}.
{suites, "tests", adhoc_SUITE}.
{suites, "tests", amp_big_SUITE}.
{suites, "tests", anonymous_SUITE}.
{suites, "tests", bosh_SUITE}.
{suites, "tests", carboncopy_SUITE}.
{suites, "tests", component_SUITE}.
{suites, "tests", disco_and_caps_SUITE}.
{suites, "tests", jingle_SUITE}.
{suites, "tests", last_SUITE}.
{suites, "tests", mod_aws_sns_SUITE}.
{suites, "tests", mod_blocking_SUITE}.
{suites, "tests", mod_event_pusher_rabbit_SUITE}.
{suites, "tests", mod_http_notification_SUITE}.
{suites, "tests", mod_http_upload_SUITE}.
{suites, "tests", mod_ping_SUITE}.
{suites, "tests", mod_time_SUITE}.
{suites, "tests", mod_version_SUITE}.
{suites, "tests", mod_global_distrib_SUITE}.

{specs, join, ["base.spec"]}.
