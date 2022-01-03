import dataclasses
import enum
from collections import OrderedDict
from typing import Dict, Iterator, List, Optional, Set
from uuid import UUID, uuid4

from validation import validate_text, validate_uuid

from dtl import nodes as n


class DType(enum.Enum):
    BOOL = "BOOL"
    INT32 = "INT32"
    DOUBLE = "DOUBLE"
    TEXT = "TEXT"
    BYTES = "BYTES"


# === Expressions ==============================================================


ExpressionRef = UUID


@dataclasses.dataclass(frozen=True)
class Expression:
    dtype: DType


@dataclasses.dataclass(frozen=True)
class ImportExpression(Expression):
    location: str
    name: str


@dataclasses.dataclass(frozen=True)
class WhereExpression(Expression):
    source: ExpressionRef
    mask: ExpressionRef


@dataclasses.dataclass(frozen=True)
class PickExpression(Expression):
    source: ExpressionRef
    indexes: ExpressionRef


@dataclasses.dataclass(frozen=True)
class IndexExpression(Expression):
    source: ExpressionRef


@dataclasses.dataclass(frozen=True)
class JoinLeftExpression(Expression):
    source_a: ExpressionRef
    source_b: ExpressionRef


@dataclasses.dataclass(frozen=True)
class JoinRightExpression(Expression):
    source_a: ExpressionRef
    source_b: ExpressionRef


@dataclasses.dataclass(frozen=True)
class AddExpression:
    source_a: ExpressionRef
    source_b: ExpressionRef


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

    expression: ExpressionRef


@dataclasses.dataclass(frozen=True)
class Table:
    #: An (optional) reference to the ast node from which this table was derived.
    ast_node: Optional[n.Node]
    level: Level

    columns: List[Column]


# === Validation ===============================================================

_undefined = object()


def validate_dtype(value=_undefined, *, required=True):
    def _validate_dtype(value):
        if value is None:
            if required:
                raise TypeError("required sha1 is None")
            return

        if not isinstance(value, DType):
            raise TypeError(
                f"expected DType, but value is of type {type(value).__name__}"
            )

    if value is not _undefined:
        return _validate_dtype(value)
    return _validate_dtype


# === Program ==================================================================


class ExpressionSet:
    def __init__(self):
        self.__expressions = OrderedDict()

    def push_import_expr(
        self,
        *,
        location: str,
        name: str,
        dtype: DType,
        ref: Optional[ExpressionRef] = None,
    ) -> ExpressionRef:
        validate_text(location)
        validate_text(name)
        validate_dtype(dtype)
        validate_uuid(ref, required=False)

        if ref is None:
            ref = uuid4()
        assert ref not in self.__expressions

        self.__expressions[ref] = ImportExpression(
            dtype=dtype,
            name=name,
            location=location,
        )

        return ref

    def push_where_expr(
        self,
        *,
        source: ExpressionRef,
        mask: ExpressionRef,
        dtype: Optional[DType],
        ref: Optional[ExpressionRef] = None,
    ) -> ExpressionRef:
        validate_uuid(source)
        validate_uuid(mask)
        validate_dtype(dtype, required=False)
        validate_uuid(ref, required=False)

        if ref is None:
            ref = uuid4()
        assert ref not in self.__expressions

        if source not in self.__expressions:
            raise Exception("dependency error")
        source_expr = self.__expressions[source]

        if dtype is not None and source_expr.dtype != dtype:
            raise AssertionError("specified dtype does not match derived")

        if mask not in self.__expressions:
            raise Exception("dependency error")
        mask_expr = self.__expressions[mask]

        if mask_expr.dtype != DType.Bool:
            raise Exception("Type error")

        self.__expressions[ref] = WhereExpression(
            dtype=source_expr.dtype, source=source, mask=mask
        )

        return ref

    def push_pick_expr(
        self,
        *,
        source: ExpressionRef,
        indexes: ExpressionRef,
        dtype: Optional[DType],
        ref: Optional[ExpressionRef] = None,
    ) -> ExpressionRef:
        validate_uuid(source)
        validate_uuid(indexes)
        validate_dtype(required=False)
        validate_uuid(ref, required=False)

        if ref is None:
            ref = uuid4()
        assert ref not in self.__expressions

        if source not in self.__expressions:
            raise Exception("Dependency error")
        source_expr = self.__expressions[source]

        if dtype is not None and source_expr.dtype != dtype:
            raise AssertionError("specified dtype does not match derived")

        if indexes not in self.__expressions:
            raise Exception("Dependency error")
        indexes_expr = self.__expressions[indexes]

        if indexes_expr.dtype != DType.Int:
            raise Exception("Type error")

        self.__expressions[ref] = PickExpression(
            dtype=source_expr.dtype,
            source=source,
            indexes=indexes,
        )

        return ref

    def push_index_expr(
        self,
        *,
        source: ExpressionRef,
        ref: Optional[ExpressionRef] = None,
    ) -> ExpressionRef:
        validate_uuid(source)
        validate_uuid(ref, required=False)

        if ref is None:
            ref = uuid4()
        assert ref not in self.__expressions

        if source not in self.__expressions:
            raise Exception("Dependency error")

        self.__expressions[ref] = IndexExpression(
            dtype=DType.Int32,
            source=source,
        )

    def get(self, key: ExpressionRef) -> Expression:
        return self.__expressions.get(key)

    def __getitem__(self, key: ExpressionRef) -> ExpressionRef:
        return self.__expressions[key]

    def __iter__(self) -> Iterator[ExpressionRef]:
        yield from self.__expressions


@dataclasses.dataclass(frozen=True, eq=False)
class Program:
    expressions: ExpressionSet = dataclasses.field(
        init=False, default_factory=ExpressionSet
    )
    tables: List[Table] = dataclasses.field(init=False, default_factory=list)
    exports: Dict[str, Table] = dataclasses.field(
        init=False, default_factory=dict
    )
