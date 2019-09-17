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

{suites, "tests", connect_SUITE}.
{suites, "tests", login_SUITE}.
{suites, "tests", sm_SUITE}.
{suites, "tests", sasl_SUITE}.
{suites, "tests", mam_SUITE}.
{suites, "tests", pubsub_SUITE}.

{specs, join, ["base.spec"]}.
