# from __future__ import annotations

from dataclasses import dataclass
from typing import List, Optional

import dtl.tokens as t
from dtl.types import Location


@dataclass(frozen=True)
class Node:
    start: Location
    end: Location


class Expression(Node):
    pass


# === Columns ==================================================================


class ColumnName(Node):
    pass


@dataclass(frozen=True)
class UnqualifiedColumnName(ColumnName):
    column_name: str


@dataclass(frozen=True)
class QualifiedColumnName(ColumnName):
    table_name: str
    column_name: str


class ColumnExpr(Node):
    """
    An expression which can be evaluated one rwo group at a time to give the
    cells of a column.
    """

    pass


@dataclass(frozen=True)
class ColumnRefExpr(ColumnExpr):
    """
    A reference, by name, to a single column.
    """

    name: ColumnName


# === Tables ===================================================================


@dataclass(frozen=True)
class TableName(Node):
    name: str


class TableExpr(Node):
    pass


@dataclass(frozen=True)
class SubqueryExpr(TableExpr):
    source: Expression


@dataclass(frozen=True)
class TableRefExpr(TableExpr):
    name: TableName


# === Distinct =================================================================


@dataclass(frozen=True)
class DistinctClause(Node):
    """
    For example:

    ...python::

        DISTINCT CONSECUTIVE
    """

    #: If false, all matching rows after the first occurence of a pattern should
    #: be dropped, regardless of where they appear in the table.  If true, only
    #: matching rows that appear immediately after each other should be droppe.
    consecutive: bool


# === Column Bindings ==========================================================


@dataclass(frozen=True)
class ColumnBinding(Node):
    """ """

    expression: ColumnExpr
    alias: Optional[UnqualifiedColumnName]


# === From =====================================================================


@dataclass(frozen=True)
class TableBinding(Node):
    expression: TableExpr
    alias: Optional[TableName]


@dataclass(frozen=True)
class FromClause(Node):
    # TODO: SQL allows the from clause to be optional.
    source: TableBinding


# === Joins ====================================================================


class JoinConstraint(Node):
    pass


@dataclass(frozen=True)
class JoinOnConstraint(JoinConstraint):
    """
    A single boolean expression that should be evaluated in the scope of the
    joined table and all previous tables to determine if combined rows should
    be yielded.

    For example:

    ..python::

        ON joined_table.a = root_table.b
    """

    predicate: ColumnExpr


@dataclass(frozen=True)
class JoinUsingConstraint(JoinConstraint):
    """
    A tuple of column names that should be matched between the joined table and
    the root table.

    For example:

    ...python::

        USING (column_a, column_b)
    """

    columns: List[UnqualifiedColumnName]


@dataclass(frozen=True)
class JoinClause(Node):
    table: TableBinding
    # Note that, because DTL lacks explicit indexes and foreign keys, the
    # constraint is not optional.
    constraint: JoinConstraint


# === Filtering ================================================================


@dataclass(frozen=True)
class WhereClause(Node):
    predicate: Optional[ColumnExpr]


# === Grouping =================================================================


@dataclass(frozen=True)
class GroupByClause(Node):
    """
    For example:

    ...python::

        GROUP CONSECUTIVE BY table.key, table.subkey
    """

    #: If false, all matching rows after the first occurence of a pattern should
    #: be grouped, regardless of where they appear in the table.  If true, only
    #: matching rows that appear immediately after each other should be grouped.
    consecutive: bool
    pattern: List[ColumnExpr]


# === Expressions ==============================================================


@dataclass(frozen=True)
class SelectExpression(Expression):
    distinct: Optional[DistinctClause]
    columns: List[ColumnBinding]
    source: FromClause
    join: List[JoinClause]
    where: Optional[WhereClause]
    group_by: Optional[GroupByClause]


# === Statements ===============================================================


class Statement(Node):
    pass


@dataclass(frozen=True)
class AssignmentStatement(Statement):
    target: TableName
    expression: Expression


@dataclass(frozen=True)
class ExpressionStatement(Statement):
    expression: Expression


@dataclass(frozen=True)
class BeginStatement(Statement):
    text: t.String


@dataclass(frozen=True)
class StatementList(Node):
    statements: List[Statement]
