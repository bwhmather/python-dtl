from typing import Annotated, List, Optional

import dtl.nodes as n
import dtl.tokens as t
from dtl.parser_generator import Delimiter, ParserGenerator


def _start(node):
    if isinstance(node, list):
        for item in node:
            start = _start(item)
            if start:
                return start
        return None

    if isinstance(node, t.Token):
        return node.start

    if isinstance(node, n.Node):
        return node.start

    if node is None:
        return None

    raise AssertionError(f"node {node!r} has no start")


def _start_action(*args):
    for arg in args:
        start = _start(arg)
        if start:
            return start
    return None


def _end(node):
    if isinstance(node, list):
        for item in reversed(node):
            end = _end(item)
            if end:
                return end
        return None

    if isinstance(node, t.Token):
        return node.end

    if isinstance(node, n.Node):
        return node.end

    if node is None:
        return None

    raise AssertionError(f"node {node!r} has no end")


def _end_action(*args):
    for arg in reversed(args):
        end = _end(arg)
        if end:
            return end
    return None


_generator = ParserGenerator(
    node_type=n.Node,
    token_type=t.Token,
    default_actions=dict(start=_start_action, end=_end_action),
)


# === Columns ==================================================================

_generator.register(
    n.UnqualifiedColumnName, [t.Name], column_name=lambda token: token.text
)

_generator.register(
    n.QualifiedColumnName,
    [t.Name, t.Dot, t.Name],
    table_name=lambda table, _, column: table.text,
    column_name=lambda table, _, column: column.text,
)

_generator.register(n.ColumnRefExpr, [n.ColumnName])


# === Tables ===================================================================

_generator.register(n.TableName, [t.Name], name=lambda token: token.text)

# TODO subqueries

_generator.register(n.TableRefExpr, [n.TableName])


# === Distinct =================================================================


_generator.register(
    n.DistinctClause, [t.Distinct], consecutive=lambda *_: False
)
_generator.register(
    n.DistinctClause, [t.Distinct, t.Consecutive], consecutive=lambda *_: True
)

# === Column Bindings ==========================================================

_generator.register(n.ColumnBinding, [n.Expression], alias=lambda *_: None)
_generator.register(
    n.ColumnBinding, [n.Expression, t.As, n.UnqualifiedColumnName]
)

# === From =====================================================================


_generator.register(n.TableBinding, [n.TableExpr], alias=lambda *_: None)
_generator.register(
    n.TableBinding,
    [n.TableExpr, t.As, t.Name],
    alias=lambda from_, source_, as_, alias: alias.text,
)
_generator.register(n.FromClause, [t.From, n.TableBinding])


# === Joins ====================================================================

_generator.register(n.JoinClause, [t.Join, n.TableBinding, n.JoinConstraint])

_generator.register(n.JoinOnConstraint, [t.On, n.Expression])


# === Filtering ================================================================

_generator.register(n.WhereClause, [t.Where, n.Expression])


# === Grouping =================================================================


_generator.register(
    n.GroupByClause,
    [t.Group, t.By, Annotated[List[n.Expression], Delimiter(t.Comma)]],
    consecutive=lambda *_: False,
)
_generator.register(
    n.GroupByClause,
    [
        t.Group,
        t.Consecutive,
        t.By,
        Annotated[List[n.Expression], Delimiter(t.Comma)],
    ],
    consecutive=lambda *_: True,
)

# === TableExpressions ==============================================================

_generator.register(
    n.SelectExpression,
    [
        t.Select,
        Optional[n.DistinctClause],
        Annotated[List[n.ColumnBinding], Delimiter(t.Comma)],
        n.FromClause,
        Annotated[List[n.JoinClause], Delimiter(t.Comma)],
        Optional[n.WhereClause],
        Optional[n.GroupByClause],
    ],
)


# === Statements ===============================================================

_generator.register(
    n.AssignmentStatement, [t.With, n.TableName, t.As, n.TableExpression]
)


_generator.register(
    n.StatementList, [Annotated[List[n.Statement], Delimiter(t.Semicolon)]]
)
_parser = _generator.parser(target=n.StatementList)


def _filter_tokens(tokens):
    for token in tokens:
        if isinstance(token, t.LineComment):
            continue

        if isinstance(token, t.BlockComment):
            continue

        if isinstance(token, t.Whitespace):
            continue

        yield token


def parse(tokens):
    return _parser.parse(_filter_tokens(tokens))
