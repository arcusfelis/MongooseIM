-module(escalus_helper).
-export([attach_event_manager/2]).

%% Attach to story log
%% pop_incoming_stanza are not logged
%% You need a story
attach_event_manager(Config, UserSpec) ->
    %% To attach to event manager we need some resource at this point
    Resource = <<"escalus-default-resource">>,
    %% This is usually done in escalus_client module
    [{event_client, escalus_event:new_client(Config, UserSpec, Resource)},
     {resource, Resource}|UserSpec].
