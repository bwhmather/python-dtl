import dataclasses
import enum
from typing import List, Optional, Set

from dtl import nodes as n

# === Expressions ==============================================================


class Expression:
    pass


@dataclasses.dataclass(frozen=True)
class Import(Expression):
    location: str
    name: str


@dataclasses.dataclass(frozen=True)
class Where(Expression):
    source: Expression
    mask: Expression


@dataclasses.dataclass(frozen=True)
class Pick(Expression):
    source: Expression
    elements: Expression


@dataclasses.dataclass(frozen=True)
class JoinLeft(Expression):
    source_a: Expression
    source_b: Expression


@dataclasses.dataclass(frozen=True)
class JoinRight(Expression):
    source_a: Expression
    source_b: Expression


@dataclasses.dataclass(frozen=True)
class Add:
    source_a: Expression
    source_b: Expression


# === Tables ===================================================================


class Level(enum.Enum):
    EXPORT = "EXPORT"
    ASSERTION = "ASSERTION"
    STATEMENT = "STATEMENT"

    #: The table represents the result of a table expression nested within a
    #: statement.
    TABLE_EXPRESSION = "TABLE_EXPRESSION"

    #: The table is a synthetic table that references the result of a single
    #: column expression.  Tables with this level are used only for tracing
    #: execution.
    COLUMN_EXPRESSION = "COLUMN_EXPRESSION"

    INTERNAL = "INTERNAL"


@dataclasses.dataclass(frozen=True)
class Column:
    #: The identifier of the column.  Column expressions that are evaluated in
    #: this table's context can reference this column by `name` prefixed with
    #: any of the prefix strings in `namespaces`, or unprefixed if `namespaces`
    #: contains `None`.
    name: str
    namespaces: Set[Optional[str]]

    expression: Expression


@dataclasses.dataclass(frozen=True)
class Table:
    #: An (optional) reference to the ast node from which this table was derived.
    ast_node: Optional[n.Node]
    level: Level

    columns: List[Column]


# === Program ==================================================================


@dataclasses.dataclass(frozen=True)
class Program:
    expressions: List[Expression]
    tables: List[Table]
    exports: List[Table]
