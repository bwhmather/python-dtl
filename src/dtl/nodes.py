from dataclasses import dataclass
from typing import List, Optional

import dtl.tokens as t


@dataclass(frozen=True)
class Node:
    pass


@dataclass(frozen=True)
class TableName(Node):
    name: str


class ColumnName(Node):
    pass


@dataclass(frozen=True)
class UnqualifiedColumnName(ColumnName):
    column_name: str


@dataclass(frozen=True)
class QualifiedColumnName(ColumnName):
    table_name: str
    column_name: str


@dataclass(frozen=True)
class TableExpr(Node):
    """
    An expression which evaluates to a table.
    """

    expr: Node


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
class DistinctClause(Node):
    """
    For example:

    ...python:: DISTINCT CONSECUTIVE
    """

    #: If false, all matching rows after the first occurence of a pattern should
    #: be dropped, regardless of where they appear in the table.  If true, only
    #: matching rows that appear immediately after each other should be droppe.
    consecutive: bool


@dataclass(frozen=True)
class FromClause(Node):
    # TODO: SQL allows the from clause to be optional.
    source: TableExpr


@dataclass(frozen=True)
class JoinClause(Node):
    table: TableExpr
    # Note that, because DTL lacks explicit indexes and foreign keys, the
    # constraint is not optional.
    constraint: JoinConstraint


@dataclass(frozen=True)
class WhereClause(Node):
    predicate: Optional[ColumnExpr]


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


class Expression(Node):
    pass


@dataclass(frozen=True)
class SelectExpression(Expression):
    columns: List[ColumnExpr]
    source: FromClause
    join: List[JoinClause]
    where: Optional[WhereClause]
    group_by: Optional[GroupByClause]


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
