-module(story_transform).
-export([parse_transform/2]).

parse_transform(Forms, _Options) ->
    FunNames = fun_names_to_rewrite(Forms),
    [handle_lvl1_func_def(Tree, FunNames) || Tree <- Forms].

handle_lvl1_func_def(Node, FunNames) ->
    case erl_syntax:type(Node) of
        function ->
            Name = erl_syntax:atom_value(erl_syntax:function_name(Node)),
            Arity = erl_syntax:function_arity(Node),
            case {lists:member(Name, FunNames), Arity} of
                {true, 1} ->
                    io:format(user, "rewrite_testcase ~p/1~n", [Name]),
                    rewrite_testcase(Node);
                {_, _} ->
%                   io:format(user, "ignore_function ~p/~p~n", [Name, Arity]),
                    Node
            end;
        _Type -> Node
    end.

rewrite_testcase(Node) ->
    %% Convert testcase function into higher-order function
    ConfigVar = erl_syntax:variable('ConfigInTransform'),
    Name = erl_syntax:function_name(Node),
    Clauses = erl_syntax:function_clauses(Node),
    AnonFun = erl_syntax:fun_expr(Clauses),

    %% Make "F(Config)"
    CallCase = erl_syntax:application(none, AnonFun, [ConfigVar]),
    AnonFun2Clause = erl_syntax:clause([], none, [CallCase]),
    %% Make "fun() -> F(Config) end"
    AnonFun2 = erl_syntax:fun_expr([AnonFun2Clause]),

    %% Replace original function with escalus:story(Config, [], fun() -> F(Config) end)
    Call = erl_syntax:application(erl_syntax:atom(escalus),
                                  erl_syntax:atom(story),
                                  [ConfigVar, erl_syntax:nil(), AnonFun2]),
    NewClause = erl_syntax:clause([ConfigVar], none, [Call]),

    NewFunTree = erl_syntax:function(Name, [NewClause]),
    erl_syntax:revert(NewFunTree).

fun_names_to_rewrite([]) ->
    [];
fun_names_to_rewrite([Node|Forms]) ->
    case erl_syntax:type(Node) of
        attribute ->
            case erl_syntax:atom_value(erl_syntax:attribute_name(Node)) of
                story_cases ->
                    [Names] = erl_syntax:attribute_arguments(Node),
                    [erl_syntax:atom_value(Name)
                     || Name <- erl_syntax:list_elements(Names)];
                _OtherAttribute ->
                    fun_names_to_rewrite(Forms)
            end;
        _OtherType ->
            fun_names_to_rewrite(Forms)
    end.
