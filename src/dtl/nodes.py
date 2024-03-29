from __future__ import annotations

from dataclasses import dataclass
from typing import List, Optional

from dtl.types import Location


@dataclass(frozen=True)
class Node:
    start: Location
    end: Location


# === Literals =================================================================


class Literal(Node):
    pass


@dataclass(frozen=True)
class Boolean(Literal):
    value: bool


@dataclass(frozen=True)
class Integer(Literal):
    value: int


@dataclass(frozen=True)
class Float(Literal):
    value: float


@dataclass(frozen=True)
class String(Literal):
    value: str


@dataclass(frozen=True)
class Bytes(Literal):
    value: bytes


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


class Expression(Node):
    """
    An expression which can be evaluated one rwo group at a time to give the
    cells of a column.
    """

    pass


@dataclass(frozen=True)
class ColumnReferenceExpression(Expression):
    """
    A reference, by name, to a single column.
    """

    name: ColumnName


@dataclass(frozen=True)
class LiteralExpression(Expression):
    literal: Literal


@dataclass(frozen=True)
class FunctionCallExpression(Expression):
    name: str
    arguments: List[Expression]


@dataclass(frozen=True)
class EqualToExpression(Expression):
    left: Expression
    right: Expression


@dataclass(frozen=True)
class LessThanExpression(Expression):
    left: Expression
    right: Expression


@dataclass(frozen=True)
class LessThanEqualExpression(Expression):
    left: Expression
    right: Expression


@dataclass(frozen=True)
class GreaterThanExpression(Expression):
    left: Expression
    right: Expression


@dataclass(frozen=True)
class GreaterThanEqualExpression(Expression):
    left: Expression
    right: Expression


@dataclass(frozen=True)
class AddExpression(Expression):
    left: Expression
    right: Expression


@dataclass(frozen=True)
class SubtractExpression(Expression):
    left: Expression
    right: Expression


@dataclass(frozen=True)
class MultiplyExpression(Expression):
    left: Expression
    right: Expression


@dataclass(frozen=True)
class DivideExpression(Expression):
    left: Expression
    right: Expression


# === Tables ===================================================================


@dataclass(frozen=True)
class TableName(Node):
    name: str


# === Distinct =================================================================


@dataclass(frozen=True)
class DistinctClause(Node):
    """
    For example:

    ...python::

        DISTINCT CONSECUTIVE
    """

    #: If false, all matching rows after the first occurrence of a pattern
    #: should be dropped, regardless of where they appear in the table.  If true
    #: only matching rows that appear immediately after each other should be
    #: dropped.
    consecutive: bool


# === Column Bindings ==========================================================


class ColumnBinding(Node):
    pass


@dataclass(frozen=True)
class WildcardColumnBinding(ColumnBinding):
    pass


@dataclass(frozen=True)
class ImplicitColumnBinding(ColumnBinding):
    expression: Expression


@dataclass(frozen=True)
class AliasedColumnBinding(ColumnBinding):
    expression: Expression
    alias: UnqualifiedColumnName


# === From =====================================================================


@dataclass(frozen=True)
class TableBinding(Node):
    expression: "TableExpression"
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

    predicate: Expression


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
    predicate: Expression


# === Grouping =================================================================


@dataclass(frozen=True)
class GroupByClause(Node):
    """
    For example:

    ...python::

        GROUP CONSECUTIVE BY table.key, table.subkey
    """

    #: If false, all matching rows after the first occurrence of a pattern
    #: should be grouped, regardless of where they appear in the table.  If
    #: true, only matching rows that appear immediately after each other should
    #: be grouped.
    consecutive: bool
    pattern: List[Expression]


# === Table Expressions ========================================================


class TableExpression(Node):
    pass


@dataclass(frozen=True)
class SelectExpression(TableExpression):
    distinct: Optional[DistinctClause]
    columns: List[ColumnBinding]
    source: FromClause
    join: List[JoinClause]
    where: Optional[WhereClause]
    group_by: Optional[GroupByClause]


@dataclass(frozen=True)
class ImportExpression(TableExpression):
    location: String


@dataclass(frozen=True)
class TableReferenceExpression(TableExpression):
    name: str


# === Statements ===============================================================


class Statement(Node):
    pass


@dataclass(frozen=True)
class AssignmentStatement(Statement):
    target: TableName
    expression: TableExpression


@dataclass(frozen=True)
class UpdateStatement(Statement):
    pass


@dataclass(frozen=True)
class DeleteStatement(Statement):
    pass


@dataclass(frozen=True)
class InsertStatement(Statement):
    pass


@dataclass(frozen=True)
class ExportStatement(Statement):
    expression: TableExpression
    location: String


@dataclass(frozen=True)
class BeginStatement(Statement):
    text: String


@dataclass(frozen=True)
class StatementList(Node):
    statements: List[Statement]
