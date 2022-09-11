from __future__ import annotations

import dataclasses
import enum
from functools import singledispatch
from typing import Callable, Dict, Iterable, List

from dtl import nodes as n


class DType(enum.Enum):
    BOOL = "BOOL"
    INT32 = "INT32"
    INT64 = "INT64"
    DOUBLE = "DOUBLE"
    TEXT = "TEXT"
    BYTES = "BYTES"
    INDEX = "INDEX"
    TIMESTAMP = "TIMESTAMP"
    DATE = "DATE"


# === Expressions ==============================================================


@dataclasses.dataclass(frozen=True, eq=False)
class Expression:
    pass


@dataclasses.dataclass(frozen=True, eq=False)
class ShapeExpression(Expression):
    pass


@dataclasses.dataclass(frozen=True, eq=False)
class ImportShapeExpression(ShapeExpression):
    location: str


@dataclasses.dataclass(frozen=True, eq=False)
class ArrayExpression(Expression):
    dtype: DType
    shape: ShapeExpression


@dataclasses.dataclass(frozen=True, eq=False)
class WhereShapeExpression(ShapeExpression):
    mask: ArrayExpression


@dataclasses.dataclass(frozen=True, eq=False)
class JoinShapeExpression(ShapeExpression):
    shape_a: ShapeExpression
    shape_b: ShapeExpression


@dataclasses.dataclass(frozen=True, eq=False)
class BooleanLiteralExpression(ArrayExpression):
    value: bool


@dataclasses.dataclass(frozen=True, eq=False)
class IntegerLiteralExpression(ArrayExpression):
    value: int


@dataclasses.dataclass(frozen=True, eq=False)
class FloatLiteralExpression(ArrayExpression):
    value: float


@dataclasses.dataclass(frozen=True, eq=False)
class TextLiteralExpression(ArrayExpression):
    value: str


@dataclasses.dataclass(frozen=True, eq=False)
class BytesLiteralExpression(ArrayExpression):
    value: bytes


@dataclasses.dataclass(frozen=True, eq=False)
class ImportExpression(ArrayExpression):
    location: str
    name: str


@dataclasses.dataclass(frozen=True, eq=False)
class WhereExpression(ArrayExpression):
    source: ArrayExpression
    mask: ArrayExpression


@dataclasses.dataclass(frozen=True, eq=False)
class PickExpression(ArrayExpression):
    source: ArrayExpression
    indexes: ArrayExpression


@dataclasses.dataclass(frozen=True, eq=False)
class IndexExpression(ArrayExpression):
    """
    Evaluates to an array of indexes into source, sorted so that the values they
    point to are in ascending order.
    """

    source: ArrayExpression


@dataclasses.dataclass(frozen=True, eq=False)
class JoinLeftExpression(ArrayExpression):
    """
    Evaluates to the array of indexes into the first array that would give a
    full, unfiltered inner join with the second array.
    """

    shape_a: ShapeExpression
    shape_b: ShapeExpression


@dataclasses.dataclass(frozen=True, eq=False)
class JoinRightExpression(ArrayExpression):
    """
    Evaluates to the array of indexes into the second array that would give a
    full, unfiltered inner join with the first array.
    """

    shape_a: ShapeExpression
    shape_b: ShapeExpression


@dataclasses.dataclass(frozen=True, eq=False)
class JoinLeftEqualExpression(ArrayExpression):
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

    left: ArrayExpression
    right: ArrayExpression
    right_index: ArrayExpression


@dataclasses.dataclass(frozen=True, eq=False)
class JoinRightEqualExpression(ArrayExpression):
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

    left: ArrayExpression
    right: ArrayExpression
    right_index: ArrayExpression


@dataclasses.dataclass(frozen=True, eq=False)
class AddExpression(ArrayExpression):
    source_a: ArrayExpression
    source_b: ArrayExpression


@dataclasses.dataclass(frozen=True, eq=False)
class SubtractExpression(ArrayExpression):
    source_a: ArrayExpression
    source_b: ArrayExpression


@dataclasses.dataclass(frozen=True, eq=False)
class MultiplyExpression(ArrayExpression):
    source_a: ArrayExpression
    source_b: ArrayExpression


@dataclasses.dataclass(frozen=True, eq=False)
class DivideExpression(ArrayExpression):
    source_a: ArrayExpression
    source_b: ArrayExpression


@dataclasses.dataclass(frozen=True, eq=False)
class EqualToExpression(ArrayExpression):
    source_a: ArrayExpression
    source_b: ArrayExpression


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
    name: str
    expression: ArrayExpression


@dataclasses.dataclass(frozen=True, kw_only=True)
class Table:
    columns: List[Column]


@dataclasses.dataclass(frozen=True, kw_only=True)
class TraceTable(Table):
    #: An (optional) reference to the ast node from which this table was derived.
    ast_node: n.Node
    level: Level


@dataclasses.dataclass(frozen=True, kw_only=True)
class ExportTable(Table):
    #: The name of the file to create if this table is to be exported.  Will be
    #: None if this is just an intermediate trace table.
    export_as: str


# === Program ==================================================================


@dataclasses.dataclass(frozen=True, eq=False)
class Program:
    tables: List[Table]


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
    shape = transform(expr.shape)
    assert isinstance(shape, ShapeExpression)

    source = transform(expr.source)
    assert isinstance(source, ArrayExpression)

    mask = transform(expr.mask)
    assert isinstance(mask, ArrayExpression)

    if shape is expr.shape and source is expr.source and mask is expr.mask:
        return expr

    return WhereExpression(
        dtype=expr.dtype,
        shape=shape,
        source=source,
        mask=mask,
    )


@_transform_children.register(PickExpression)
def _transform_pick_expr_children(
    expr: PickExpression, *, transform: Callable[[Expression], Expression]
) -> PickExpression:
    shape = transform(expr.shape)
    assert isinstance(shape, ShapeExpression)

    source = transform(expr.source)
    assert isinstance(source, ArrayExpression)

    indexes = transform(expr.indexes)
    assert isinstance(indexes, ArrayExpression)

    if (
        shape is expr.shape
        and source is expr.source
        and indexes is expr.indexes
    ):
        return expr

    return PickExpression(
        dtype=expr.dtype,
        shape=shape,
        source=source,
        indexes=indexes,
    )


@_transform_children.register(IndexExpression)
def _transform_index_expr_children(
    expr: IndexExpression, *, transform: Callable[[Expression], Expression]
) -> IndexExpression:
    shape = transform(expr.shape)
    assert isinstance(shape, ShapeExpression)

    source = transform(expr.source)
    assert isinstance(source, ArrayExpression)

    if shape is expr.shape and source is expr.source:
        return expr

    return IndexExpression(
        dtype=expr.dtype,
        shape=shape,
        source=source,
    )


@_transform_children.register(JoinLeftExpression)
def _transform_join_left_expr_children(
    expr: JoinLeftExpression, *, transform: Callable[[Expression], Expression]
) -> JoinLeftExpression:
    shape = transform(expr.shape)
    assert isinstance(shape, ShapeExpression)

    shape_a = transform(expr.shape_a)
    assert isinstance(shape_a, ShapeExpression)

    shape_b = transform(expr.shape_b)
    assert isinstance(shape_b, ShapeExpression)

    if (
        shape is expr.shape
        and shape_a is expr.shape_a
        and shape_b is expr.shape_b
    ):
        return expr

    return JoinLeftExpression(
        dtype=expr.dtype,
        shape=shape,
        shape_a=shape_a,
        shape_b=shape_b,
    )


@_transform_children.register(JoinRightExpression)
def _transform_join_right_expr_children(
    expr: JoinRightExpression, *, transform: Callable[[Expression], Expression]
) -> JoinRightExpression:
    shape = transform(expr.shape)
    assert isinstance(shape, ShapeExpression)

    shape_a = transform(expr.shape_a)
    assert isinstance(shape_a, ShapeExpression)

    shape_b = transform(expr.shape_b)
    assert isinstance(shape_b, ShapeExpression)

    if (
        shape is expr.shape
        and shape_a is expr.shape_a
        and shape_b is expr.shape_b
    ):
        return expr

    return JoinRightExpression(
        dtype=expr.dtype,
        shape=shape,
        shape_a=shape_a,
        shape_b=shape_b,
    )


@_transform_children.register(AddExpression)
def _transform_add_expr_children(
    expr: AddExpression, *, transform: Callable[[Expression], Expression]
) -> AddExpression:
    shape = transform(expr.shape)
    assert isinstance(shape, ShapeExpression)

    source_a = transform(expr.source_a)
    assert isinstance(source_a, ArrayExpression)

    source_b = transform(expr.source_b)
    assert isinstance(source_b, ArrayExpression)

    if (
        shape is expr.shape
        and source_a is expr.source_a
        and source_b is expr.source_b
    ):
        return expr

    return AddExpression(
        dtype=expr.dtype,
        shape=shape,
        source_a=source_a,
        source_b=source_b,
    )


def map(
    function: Callable[[Expression], Expression], roots: list[Expression]
) -> list[Expression]:
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


@dependencies.register(ImportShapeExpression)
def _get_import_shape_expr_dependencies(
    expr: ImportShapeExpression,
) -> Iterable[Expression]:
    return
    yield


@dependencies.register(WhereShapeExpression)
def _get_where_shape_expr_dependencies(
    expr: WhereShapeExpression,
) -> Iterable[Expression]:
    yield expr.mask


@dependencies.register(JoinShapeExpression)
def _get_join_shape_expr_dependencies(
    expr: JoinShapeExpression,
) -> Iterable[Expression]:
    yield expr.shape_a
    yield expr.shape_b


@dependencies.register(BooleanLiteralExpression)
def _get_boolean_literal_expr_dependencies(
    expr: BooleanLiteralExpression,
) -> Iterable[Expression]:
    yield expr.shape


@dependencies.register(IntegerLiteralExpression)
def _get_integer_literal_expr_dependencies(
    expr: IntegerLiteralExpression,
) -> Iterable[Expression]:
    yield expr.shape


@dependencies.register(FloatLiteralExpression)
def _get_float_literal_expr_dependencies(
    expr: FloatLiteralExpression,
) -> Iterable[Expression]:
    yield expr.shape


@dependencies.register(TextLiteralExpression)
def _get_text_literal_expr_dependencies(
    expr: TextLiteralExpression,
) -> Iterable[Expression]:
    yield expr.shape


@dependencies.register(BytesLiteralExpression)
def _get_bytes_literal_expr_dependencies(
    expr: BytesLiteralExpression,
) -> Iterable[Expression]:
    yield expr.shape


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
    yield expr.shape_a
    yield expr.shape_b


@dependencies.register(JoinRightExpression)
def _get_join_right_expr_dependencies(
    expr: JoinRightExpression,
) -> Iterable[Expression]:
    yield expr.shape_a
    yield expr.shape_b


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


def traverse_depth_first(roots: Iterable[Expression]) -> Iterable[Expression]:
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
