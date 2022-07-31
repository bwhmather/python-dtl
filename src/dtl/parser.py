from typing import Annotated, Iterable, List, Optional

import dtl.nodes as n
import dtl.tokens as t
from dtl.parser_generator import Delimiter, ParserGenerator
from dtl.types import Location


def _start(
    node: t.Token | list[t.Token] | n.Node | list[n.Node] | None,
) -> Optional[Location]:
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


def _start_action(*args: t.Token | n.Node | None) -> Optional[Location]:
    for arg in args:
        start = _start(arg)
        if start:
            return start
    return None


def _end(
    node: t.Token | list[t.Token] | n.Node | list[n.Node] | None,
) -> Optional[Location]:
    if isinstance(node, list):
        for item in reversed(node):
            end = _end(item)  # type: ignore
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


def _end_action(*args: t.Token | n.Node | None) -> Optional[Location]:
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

# === Literals =================================================================


_generator.register(n.Boolean, [t.True_], value=lambda _: True)
_generator.register(n.Boolean, [t.False_], value=lambda _: False)


def _parse_float(raw: str) -> float:
    return float(raw)


_generator.register(
    n.Float, [t.Float], value=lambda token: _parse_float(token.text)
)


def _parse_integer(raw: str) -> int:
    return int(raw)


_generator.register(
    n.Integer, [t.Integer], value=lambda token: _parse_integer(token.text)
)


def _parse_string(raw: str) -> str:
    # TODO embarassing number of allocations.
    raw = raw[1:-1]
    out = ""
    while raw:
        next, raw = raw[0], raw[1:]

        if next == "\\":
            next, raw = raw[0], raw[1:]
            next = {
                "a": "\a",
                "b": "\b",
                "f": "\f",
                "n": "\n",
                "r": "\r",
                "t": "\t",
                "v": "\v",
            }.get(next, next)

        out += next
    return out


_generator.register(
    n.String, [t.String], value=lambda token: _parse_string(token.text)
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

_generator.register(n.ColumnReferenceExpression, [n.ColumnName])


_generator.register(n.LiteralExpression, [n.Literal])


_generator.register(
    n.FunctionCallExpression,
    [
        t.Name,
        t.OpenParen,
        Annotated[List[n.Expression], Delimiter(t.Comma)],  # type: ignore
        t.CloseParen,
    ],
    name=lambda token, *_: token.text,
)

_generator.register(n.EqualToExpression, [n.Expression, t.Eq, n.Expression])
_generator.register(
    n.LessThanExpression, [n.Expression, t.LessThan, n.Expression]
)
_generator.register(
    n.LessThanEqualExpression, [n.Expression, t.LessThanEqual, n.Expression]
)
_generator.register(
    n.GreaterThanExpression, [n.Expression, t.GreaterThan, n.Expression]
)
_generator.register(
    n.GreaterThanEqualExpression,
    [n.Expression, t.GreaterThanEqual, n.Expression],
)

_generator.register(n.AddExpression, [n.Expression, t.Plus, n.Expression])

_generator.register(
    n.SubtractExpression, [n.Expression, t.Minus, n.Expression]
)

_generator.register(n.MultiplyExpression, [n.Expression, t.Star, n.Expression])

_generator.register(n.DivideExpression, [n.Expression, t.Slash, n.Expression])

_generator.left(
    t.Eq, t.LessThan, t.LessThanEqual, t.GreaterThan, t.GreaterThanEqual
)
_generator.left(t.Plus, t.Minus)
_generator.left(t.Star, t.Slash)


# === Tables ===================================================================

_generator.register(n.TableName, [t.Name], name=lambda token: token.text)


# === Distinct =================================================================


_generator.register(
    n.DistinctClause, [t.Distinct], consecutive=lambda *_: False
)
_generator.register(
    n.DistinctClause, [t.Distinct, t.Consecutive], consecutive=lambda *_: True
)

# === Column Bindings ==========================================================

_generator.register(n.WildcardColumnBinding, [t.Star])

_generator.register(n.ImplicitColumnBinding, [n.Expression])

_generator.register(
    n.AliasedColumnBinding, [n.Expression, t.As, n.UnqualifiedColumnName]
)

# === From =====================================================================


_generator.register(
    n.TableBinding,
    [t.OpenParen, n.TableExpression, t.CloseParen],
    alias=lambda *_: None,
)
_generator.register(
    n.TableBinding, [n.TableReferenceExpression], alias=lambda *_: None
)

_generator.register(
    n.TableBinding,
    [t.OpenParen, n.TableExpression, t.CloseParen, t.As, t.Name],
    alias=lambda from_, source_, as_, alias: alias.text,
)
_generator.register(
    n.TableBinding,
    [n.TableReferenceExpression, t.As, t.Name],
    alias=lambda from_, source_, as_, alias: alias.text,
)

_generator.register(n.FromClause, [t.From, n.TableBinding])


# === Joins ====================================================================

_generator.register(n.JoinClause, [t.Join, n.TableBinding, n.JoinConstraint])

_generator.register(n.JoinOnConstraint, [t.On, n.Expression])

_generator.register(
    n.JoinUsingConstraint,
    [
        t.Using,
        t.OpenParen,
        Annotated[List[n.UnqualifiedColumnName], Delimiter(t.Comma)],  # type: ignore
        t.CloseParen,
    ],
)


# === Filtering ================================================================

_generator.register(n.WhereClause, [t.Where, n.Expression])


# === Grouping =================================================================


_generator.register(
    n.GroupByClause,
    [
        t.Group,
        t.By,
        Annotated[List[n.Expression], Delimiter(t.Comma)],  # type: ignore
    ],
    consecutive=lambda *_: False,
)
_generator.register(
    n.GroupByClause,
    [
        t.Group,
        t.Consecutive,
        t.By,
        Annotated[List[n.Expression], Delimiter(t.Comma)],  # type: ignore
    ],
    consecutive=lambda *_: True,
)

# === Table Expressions ========================================================

_generator.register(
    n.SelectExpression,
    [
        t.Select,
        Optional[n.DistinctClause],  # type: ignore
        Annotated[List[n.ColumnBinding], Delimiter(t.Comma)],  # type: ignore
        n.FromClause,
        Annotated[List[n.JoinClause], Delimiter(t.Comma)],  # type: ignore
        Optional[n.WhereClause],  # type: ignore
        Optional[n.GroupByClause],  # type: ignore
    ],
)

_generator.register(n.ImportExpression, [t.Import, n.String])

_generator.register(
    n.TableReferenceExpression, [t.Name], name=lambda token: token.text
)


# === Statements ===============================================================

_generator.register(
    n.AssignmentStatement,
    [t.With, n.TableName, t.As, n.TableExpression, t.Semicolon],
)
_generator.register(
    n.ExportStatement,
    [t.Export, n.TableExpression, t.To, n.String, t.Semicolon],
)

_generator.register(n.StatementList, [List[n.Statement]])  # type: ignore
_parser = _generator.parser(target=n.StatementList)


def _filter_tokens(tokens: Iterable[t.Token]) -> Iterable[t.Token]:
    for token in tokens:
        if isinstance(token, t.LineComment):
            continue

        if isinstance(token, t.BlockComment):
            continue

        if isinstance(token, t.Whitespace):
            continue

        yield token


def parse(tokens: Iterable[t.Token]) -> n.StatementList:
    statements = _parser.parse(_filter_tokens(tokens))
    assert isinstance(statements, n.StatementList)
    return statements
