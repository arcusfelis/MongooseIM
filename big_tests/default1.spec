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
{suites, "tests", oauth_SUITE}.
{suites, "tests", offline_SUITE}.
{suites, "tests", pep_SUITE}.
{suites, "tests", presence_SUITE}.
{suites, "tests", privacy_SUITE}.
{suites, "tests", private_SUITE}.
{suites, "tests", pubsub_SUITE}.
{suites, "tests", push_SUITE}.
{suites, "tests", push_http_SUITE}.
{suites, "tests", push_integration_SUITE}.
{suites, "tests", push_pubsub_SUITE}.
{suites, "tests", rest_SUITE}.
{suites, "tests", rest_client_SUITE}.
{suites, "tests", s2s_SUITE}.
{suites, "tests", sasl_SUITE}.
{suites, "tests", shared_roster_SUITE}.
{suites, "tests", sic_SUITE}.
{suites, "tests", sm_SUITE}.
{suites, "tests", users_api_SUITE}.
{suites, "tests", vcard_simple_SUITE}.
{suites, "tests", vcard_SUITE}.
{suites, "tests", websockets_SUITE}.
{suites, "tests", xep_0352_csi_SUITE}.
{suites, "tests", mod_global_distrib_SUITE}.
{suites, "tests", mongoose_cassandra_SUITE}.
{suites, "tests", mongoose_elasticsearch_SUITE}.
{suites, "tests", sasl_external_SUITE}.

%% Default config reader is:
%% {userconfig, {ct_config_plain, ["test.config"]}}.

%% But we use config with preprocessing:
%%{userconfig, {mim_ct_config, ["test.config"]}}.

{logdir, "ct_report"}.

%% ct_tty_hook will log CT failures to TTY verbosely
%% ct_mongoose_hook will:
%% * log suite start/end events in the MongooseIM console
%% * ensure preset value is passed to ct Config
%% * check server's purity after SUITE
{ct_hooks, [ct_groups_summary_hook, ct_tty_hook, ct_mongoose_hook, ct_progress_hook,
            ct_mim_config_hook,
            ct_markdown_errors_hook,
            {ct_mongoose_log_hook, [ejabberd_node, ejabberd_cookie]},
            {ct_mongoose_log_hook, [ejabberd2_node, ejabberd_cookie]}
           ]}.

%% To enable printing group and case enters on server side
%%{ct_hooks, [{ct_mongoose_hook, [print_group, print_case]}]}.
