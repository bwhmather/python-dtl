from __future__ import annotations

import dataclasses
import enum
from functools import singledispatch
from typing import Callable, Dict, Iterable, List, Optional, Set

from dtl import nodes as n


class DType(enum.Enum):
    BOOL = "BOOL"
    INT32 = "INT32"
    DOUBLE = "DOUBLE"
    TEXT = "TEXT"
    BYTES = "BYTES"
    INDEX = "INDEX"


# === Shapes ===================================================================


@dataclasses.dataclass(frozen=True, eq=False)
class Shape:
    pass


# === Expressions ==============================================================


@dataclasses.dataclass(frozen=True, eq=False)
class Expression:
    dtype: DType
    shape: Shape


@dataclasses.dataclass(frozen=True, eq=False)
class ImportExpression(Expression):
    location: str
    name: str


@dataclasses.dataclass(frozen=True, eq=False)
class WhereExpression(Expression):
    source: Expression
    mask: Expression


@dataclasses.dataclass(frozen=True, eq=False)
class PickExpression(Expression):
    source: Expression
    indexes: Expression


@dataclasses.dataclass(frozen=True, eq=False)
class IndexExpression(Expression):
    """
    Evaluates to an array of indexes into source, sorted so that the values they
    point to are in ascending order.
    """

    source: Expression


@dataclasses.dataclass(frozen=True, eq=False)
class JoinLeftExpression(Expression):
    """
    Evaluates to the array of indexes into the first array that would give a
    full, unfiltered inner join with the second array.
    """

    shape_a: Shape
    shape_b: Shape


@dataclasses.dataclass(frozen=True, eq=False)
class JoinRightExpression(Expression):
    """
    Evaluates to the array of indexes into the second array that would give a
    full, unfiltered inner join with the first array.
    """

    shape_a: Shape
    shape_b: Shape


@dataclasses.dataclass(frozen=True, eq=False)
class JoinLeftEqualExpression(Expression):
    """
    Evaluates to an array of indexes into the left hand expression, where the
    value matches an equivalent value in the right expression.

    If there are multiple matches then the index will be repeated.

    Equivalent to, and generally compiled from:

    .. python::

        join_left = JoinLeftExpression(len(left), len(right))
        join_right = JoinRightExpression(len(left), len(right))
        mask = EqualToExpression(
            PickExpression(left, join_left),
            PickExpression(right, join_right)
        )
        join_left_equal = where(join_left, mask)

    """

    left: Expression
    right: Expression
    right_index: Expression


@dataclasses.dataclass(frozen=True, eq=False)
class JoinRightEqualExpression(Expression):
    """
    Evaluates to an array of indexes into the right hand expression, where the
    value matches an equivalent value in the left expression.

    If there are multiple matches then the index will be repeated.

    Equivalent to, and generally compiled from:

    .. python::

        join_left = JoinLeftExpression(len(left), len(right))
        join_right = JoinRightExpression(len(left), len(right))
        mask = EqualToExpression(
            PickExpression(left, join_left),
            PickExpression(right, join_right)
        )
        join_right_equal = where(join_right, mask)

    """

    left: Expression
    right: Expression
    right_index: Expression


@dataclasses.dataclass(frozen=True, eq=False)
class AddExpression(Expression):
    source_a: Expression
    source_b: Expression


@dataclasses.dataclass(frozen=True, eq=False)
class SubtractExpression(Expression):
    source_a: Expression
    source_b: Expression


@dataclasses.dataclass(frozen=True, eq=False)
class MultiplyExpression(Expression):
    source_a: Expression
    source_b: Expression


@dataclasses.dataclass(frozen=True, eq=False)
class DivideExpression(Expression):
    source_a: Expression
    source_b: Expression


@dataclasses.dataclass(frozen=True, eq=False)
class EqualToExpression(Expression):
    source_a: Expression
    source_b: Expression


assert AddExpression.__hash__ is Expression.__hash__


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


@dataclasses.dataclass(frozen=True, eq=False)
class Program:
    tables: List[Table] = dataclasses.field(init=False, default_factory=list)
    exports: Dict[str, Table] = dataclasses.field(
        init=False, default_factory=dict
    )


# === Helpers ==================================================================


@singledispatch
def _transform_children(
    expr: Expression, *, transform: Callable[[Expression], Expression]
) -> Expression:
    raise NotImplementedError(
        f"_transform_children not implemented for {type(expr).__name__}"
    )


@_transform_children.register(ImportExpression)
def _transform_import_expr_children(
    expr: ImportExpression, *, transform: Callable[[Expression], Expression]
) -> ImportExpression:
    return expr


@_transform_children.register(WhereExpression)
def _transform_where_expr_children(
    expr: WhereExpression, *, transform: Callable[[Expression], Expression]
) -> WhereExpression:
    source = transform(expr.source)
    mask = transform(expr.mask)

    if source is expr.source and mask is expr.mask:
        return expr

    return WhereExpression(
        dtype=expr.dtype,
        shape=expr.shape,
        source=source,
        mask=mask,
    )


@_transform_children.register(PickExpression)
def _transform_pick_expr_children(
    expr: PickExpression, *, transform: Callable[[Expression], Expression]
) -> PickExpression:
    source = transform(expr.source)
    indexes = transform(expr.indexes)

    if source is expr.source and indexes is expr.indexes:
        return expr

    return PickExpression(
        dtype=expr.dtype,
        shape=expr.shape,
        source=source,
        indexes=indexes,
    )


@_transform_children.register(IndexExpression)
def _transform_index_expr_children(
    expr: IndexExpression, *, transform: Callable[[Expression], Expression]
) -> IndexExpression:
    source = transform(expr.source)

    if source is expr.source:
        return expr

    return IndexExpression(
        dtype=expr.dtype,
        shape=source.shape,
        source=source,
    )


@_transform_children.register(JoinLeftExpression)
def _transform_join_left_expr_children(
    expr: JoinLeftExpression, *, transform: Callable[[Expression], Expression]
) -> JoinLeftExpression:
    return expr


@_transform_children.register(JoinRightExpression)
def _transform_join_right_expr_children(
    expr: JoinRightExpression, *, transform: Callable[[Expression], Expression]
) -> JoinRightExpression:
    return expr


@_transform_children.register(AddExpression)
def _transform_add_expr_children(
    expr: AddExpression, *, transform: Callable[[Expression], Expression]
) -> AddExpression:
    source_a = transform(expr.source_a)
    source_b = transform(expr.source_b)

    if source_a is expr.source_a and source_b is expr.source_b:
        return expr

    assert source_a.shape == source_b.shape

    return AddExpression(
        dtype=expr.dtype,
        shape=source_a.shape,
        source_a=source_a,
        source_b=source_b,
    )


def map(function, roots):
    mapping: Dict[Expression, Expression] = {}

    def _transform(expr: Expression) -> Expression:
        if expr not in mapping:
            mapping[expr] = function(
                _transform_children(expr, transform=_transform)
            )
        return mapping[expr]

    return [_transform(root) for root in roots]


@singledispatch
def dependencies(expr: Expression) -> Iterable[Expression]:
    """
    Yields the direct dependencies of the given expression.
    """
    raise NotImplementedError(
        f"dependencies not implemented for {type(expr).__name__}"
    )


@dependencies.register(ImportExpression)
def _get_import_expr_dependencies(
    expr: ImportExpression,
) -> Iterable[Expression]:
    return
    yield


@dependencies.register(WhereExpression)
def _get_where_expr_dependencies(
    expr: WhereExpression,
) -> Iterable[Expression]:
    yield expr.mask
    yield expr.source


@dependencies.register(PickExpression)
def _get_pick_expr_dependencies(expr: PickExpression) -> Iterable[Expression]:
    yield expr.indexes
    yield expr.source


@dependencies.register(IndexExpression)
def _get_index_expr_dependencies(
    expr: IndexExpression,
) -> Iterable[Expression]:
    yield expr.source


@dependencies.register(JoinLeftExpression)
def _get_join_left_expr_dependencies(
    expr: JoinLeftExpression,
) -> Iterable[Expression]:
    # TODO
    return
    yield


@dependencies.register(JoinRightExpression)
def _get_join_right_expr_dependencies(
    expr: JoinRightExpression,
) -> Iterable[Expression]:
    # TODO
    return
    yield


@dependencies.register(AddExpression)
def _get_add_expr_dependencies(expr: AddExpression) -> Iterable[Expression]:
    yield expr.source_a
    yield expr.source_b


@dependencies.register(SubtractExpression)
def _get_subtract_expr_dependencies(
    expr: SubtractExpression,
) -> Iterable[Expression]:
    yield expr.source_a
    yield expr.source_b


@dependencies.register(MultiplyExpression)
def _get_multiply_expr_dependencies(
    expr: MultiplyExpression,
) -> Iterable[Expression]:
    yield expr.source_a
    yield expr.source_b


@dependencies.register(DivideExpression)
def _get_divide_expr_dependencies(
    expr: DivideExpression,
) -> Iterable[Expression]:
    yield expr.source_a
    yield expr.source_b


@dependencies.register(EqualToExpression)
def _get_equal_to_expr_dependencies(
    expr: EqualToExpression,
) -> Iterable[Expression]:
    yield expr.source_a
    yield expr.source_b


def traverse_depth_first(roots):
    """
    Yields all expression reachable from the list of roots, in depth first
    order, i.e. with dependencies always being yielded before the expressions
    that depend on them.

    Reachable expressions will be yielded no more than once.
    """
    visited = set()
    stack = [list(roots)]
    while stack:
        # Expand the dependency list as far to the right as possible.
        while stack[-1]:
            next = stack[-1][-1]
            stack.append(
                [dep for dep in dependencies(next) if dep not in visited]
            )

        # Consume completed frames.
        while True:
            if stack[-1]:
                break

            stack.pop()

            if not stack:
                break

            next = stack[-1].pop()

            if next not in visited:
                yield next
            visited.add(next)
