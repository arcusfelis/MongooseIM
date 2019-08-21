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

{suites, "tests", ejabberdctl_SUITE}.
{suites, "tests", cluster_commands_SUITE}.
{suites, "tests", conf_reload_SUITE}.
{suites, "tests", metrics_api_SUITE}.
{suites, "tests", metrics_c2s_SUITE}.
{suites, "tests", metrics_roster_SUITE}.
{suites, "tests", metrics_register_SUITE}.
{suites, "tests", metrics_session_SUITE}.

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
