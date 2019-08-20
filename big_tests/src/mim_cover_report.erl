-module(mim_cover_report).
-export([make_html/1]).

-define(CT_DIR, filename:join([".", "tests"])).
-define(CT_REPORT, filename:join([".", "ct_report"])).

make_html(Modules) ->
    {ok, Root} = file:get_cwd(),
    SortScript = Root ++ "/priv/sorttable.js",
    os:cmd("cp " ++ SortScript ++ " " ++ ?CT_REPORT),
    FilePath = case file:read_file(?CT_REPORT++"/index.html") of
        {ok, IndexFileData} ->
            R = re:replace(IndexFileData, "<a href=\"all_runs.html\">ALL RUNS</a>", "& <a href=\"cover.html\" style=\"margin-right:5px\">COVER</a>"),
            file:write_file(?CT_REPORT++"/index.html", R),
            ?CT_REPORT++"/cover.html";
        _ -> skip
    end,
    CoverageDir = filename:dirname(FilePath)++"/coverage",
    file:make_dir(CoverageDir),
    {ok, File} = file:open(FilePath, [write]),
    file:write(File, get_cover_header()),
    Fun = fun(Module, {CAcc, NCAcc}) ->
                  FileName = lists:flatten(io_lib:format("~s.COVER.html",[Module])),

                  %% We assume that import_code_paths/1 was called earlier
                  case cover:analyse(Module, module) of
                      {ok, {Module, {C, NC}}} ->
                          file:write(File, row(atom_to_list(Module), C, NC, percent(C,NC),"coverage/"++FileName)),
                          FilePathC = filename:join([CoverageDir, FileName]),
                          catch cover:analyse_to_file(Module, FilePathC, [html]),
                          {CAcc + C, NCAcc + NC};
                      Reason ->
                          error_logger:error_msg("issue=cover_analyse_failed module=~p reason=~p",
                                                 [Module, Reason]),
                          {CAcc, NCAcc}
                  end
          end,
    {CSum, NCSum} = lists:foldl(Fun, {0, 0}, Modules),
    file:write(File, row("Summary", CSum, NCSum, percent(CSum, NCSum), "#")),
    file:close(File).

row(Row, C, NC, Percent, Path) ->
    [
        "<tr>",
        "<td><a href='", Path, "'>", Row, "</a></td>",
        "<td>", integer_to_list(Percent), "%</td>",
        "<td>", integer_to_list(C), "</td>",
        "<td>", integer_to_list(NC), "</td>",
        "<td>", integer_to_list(C+NC), "</td>",
        "</tr>\n"
    ].

get_cover_header() ->
    "<html>\n<head></head>\n<body bgcolor=\"white\" text=\"black\" link=\"blue\" vlink=\"purple\" alink=\"red\">\n"
    "<head><script src='sorttable.js'></script></head>"
    "<h1>Coverage for application 'MongooseIM'</h1>\n"
    "<table class='sortable' border=3 cellpadding=5>\n"
    "<tr><th>Module</th><th>Covered (%)</th><th>Covered (Lines)</th><th>Not covered (Lines)</th><th>Total (Lines)</th></tr>".

percent(0, _) -> 0;
percent(C, NC) when C /= 0; NC /= 0 -> round(C / (NC+C) * 100);
percent(_, _)                       -> 100.
