from typing import Annotated, List, Optional

import dtl.nodes as n
import dtl.tokens as t
from dtl.parser_generator import Delimiter, ParserGenerator

_generator = ParserGenerator(node_type=n.Node, token_type=t.Token)


class ParseError(Exception):
    pass


def _reduce_ident(token: t.Name) -> str:
    return token.text


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

_generator.register(n.ColumnExpr, [n.ColumnName])

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

_generator.register(n.JoinOnConstraint, [t.On, n.ColumnExpr])


# === Filtering ================================================================

_generator.register(n.WhereClause, [t.Where, n.ColumnExpr])


# === Grouping =================================================================


_generator.register(
    n.GroupByClause,
    [t.Group, t.By, Annotated[List[n.ColumnExpr], Delimiter(t.Comma)]],
    consecutive=lambda *_: False,
)
_generator.register(
    n.GroupByClause,
    [
        t.Group,
        t.Consecutive,
        t.By,
        Annotated[List[n.ColumnExpr], Delimiter(t.Comma)],
    ],
    consecutive=lambda *_: True,
)

# === Expressions ==============================================================

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

_generator.register(n.AssignmentStatement, [n.TableName, t.Eq, n.Expression])
_generator.register(n.ExpressionStatement, [n.Expression])


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
