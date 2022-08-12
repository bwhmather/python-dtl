from __future__ import annotations

import dataclasses
from functools import singledispatch
from typing import Dict, Iterable, List, Optional, cast

import pyarrow as pa

from dtl import ir as ir
from dtl import nodes as n
from dtl.io import Importer


@dataclasses.dataclass(frozen=True)
class ScopeColumn:
    #: The identifier of the column.  Column expressions that are evaluated in
    #: this table's context can reference this column by `name` prefixed with
    #: any of the prefix strings in `namespaces`, or unprefixed if `namespaces`
    #: contains `None`.
    name: str
    namespaces: set[Optional[str]]

    expression: ir.ArrayExpression


@dataclasses.dataclass(frozen=True)
class Scope:
    columns: list[ScopeColumn]


def _qualified_column_name(column: ScopeColumn) -> str:
    if None in column.namespaces:
        return column.name

    # TODO there should be a way to figure out the closest namespace.
    namespace = sorted(cast(set[str], column.namespaces))[0]
    return f"{namespace}.{column.name}"


def _strip_namespaces(columns: List[ScopeColumn]) -> List[ScopeColumn]:
    output = []
    for column in columns:
        if None not in column.namespaces:
            raise Exception("compilation error")
        output.append(
            ScopeColumn(
                name=column.name,
                namespaces={None},
                expression=column.expression,
            )
        )
    return output


@dataclasses.dataclass(frozen=True, eq=False)
class Context:
    importer: Importer
    tables: list[ir.Table] = dataclasses.field(
        init=False, default_factory=list
    )
    bindings: Dict[str, Scope] = dataclasses.field(
        init=False, default_factory=dict
    )

    def trace(
        self, scope: Scope, /, *, ast_node: n.Node, level: ir.Level
    ) -> None:
        columns = [
            ir.Column(
                name=_qualified_column_name(column),
                expression=column.expression,
            )
            for column in scope.columns
        ]
        table = ir.TraceTable(ast_node=ast_node, level=level, columns=columns)
        self.tables.append(table)

    def export(self, scope: Scope, /, *, name: str) -> None:
        columns = [
            ir.Column(
                name=_qualified_column_name(column),
                expression=column.expression,
            )
            for column in scope.columns
        ]
        table = ir.ExportTable(
            export_as=name,
            columns=columns,
        )
        self.tables.append(table)


@singledispatch
def expression_name(expr: n.Expression) -> str:
    raise Exception("no name could be derived for expression")


@expression_name.register(n.ColumnReferenceExpression)
def column_reference_expression_name(expr: n.ColumnReferenceExpression) -> str:
    # TODO qualified column names.
    assert isinstance(expr.name, n.UnqualifiedColumnName)
    return expr.name.column_name


@singledispatch
def compile_expression(
    expr: n.Expression,
    *,
    scope: Scope,
    context: Context,
) -> ir.ArrayExpression:
    raise NotImplementedError(
        f"compile_expression not implemented for {type(expr).__name__}"
    )


@compile_expression.register(n.ColumnReferenceExpression)
def compile_column_reference_expression(
    expr: n.ColumnReferenceExpression,
    *,
    scope: Scope,
    context: Context,
) -> ir.ArrayExpression:
    if isinstance(expr.name, n.UnqualifiedColumnName):
        namespace = None
        name = expr.name.column_name
    elif isinstance(expr.name, n.QualifiedColumnName):
        namespace = expr.name.table_name
        name = expr.name.column_name
    else:
        raise AssertionError

    for column in scope.columns:
        if namespace not in column.namespaces:
            continue

        if name != column.name:
            continue

        return column.expression

    raise Exception(f"could not find {name}")


@compile_expression.register(n.LiteralExpression)
def compile_literal_expression(
    expr: n.LiteralExpression,
    *,
    scope: Scope,
    context: Context,
) -> ir.ArrayExpression:
    # TODO it might make more sense to compile literals to a single row array
    # that is joined with the scope.
    # TODO handle tables with no columns.
    shape = scope.columns[0].expression.shape

    if isinstance(expr.literal, n.Boolean):
        return ir.BooleanLiteralExpression(
            dtype=ir.DType.BOOL,
            shape=shape,
            value=expr.literal.value,
        )

    if isinstance(expr.literal, n.Integer):
        return ir.IntegerLiteralExpression(
            dtype=ir.DType.INT64,
            shape=shape,
            value=expr.literal.value,
        )

    if isinstance(expr.literal, n.Float):
        return ir.FloatLiteralExpression(
            dtype=ir.DType.DOUBLE,
            shape=shape,
            value=expr.literal.value,
        )

    if isinstance(expr.literal, n.String):
        return ir.TextLiteralExpression(
            dtype=ir.DType.TEXT,
            shape=shape,
            value=expr.literal.value,
        )

    if isinstance(expr.literal, n.Bytes):
        return ir.BytesLiteralExpression(
            dtype=ir.DType.BYTES,
            shape=shape,
            value=expr.literal.value,
        )

    raise NotImplementedError()


@compile_expression.register(n.FunctionCallExpression)
def compile_function_call_expression(
    expr: n.FunctionCallExpression,
    *,
    scope: Scope,
    context: Context,
) -> ir.ArrayExpression:
    if expr.name == "add":
        assert len(expr.arguments) == 2

        expr_a, expr_b = expr.arguments
        source_a = compile_expression(expr_a, scope=scope, context=context)
        source_b = compile_expression(expr_b, scope=scope, context=context)

        if source_a.dtype != source_b.dtype:
            raise Exception("Type error")

        if source_a.shape != source_b.shape:
            raise AssertionError("Shape mismatch error")

        return ir.AddExpression(
            dtype=source_a.dtype,
            shape=source_a.shape,
            source_a=source_a,
            source_b=source_b,
        )

    raise NotImplementedError()


@compile_expression.register(n.AddExpression)
def compile_add_expression(
    expr: n.AddExpression,
    *,
    scope: Scope,
    context: Context,
) -> ir.ArrayExpression:
    left = compile_expression(expr.left, scope=scope, context=context)
    right = compile_expression(expr.right, scope=scope, context=context)

    if left.dtype != right.dtype:
        raise Exception(
            f"Type error: cannot add {right.dtype} to {left.dtype}"
        )

    if left.shape != right.shape:
        raise AssertionError("Shape mismatch error")

    return ir.AddExpression(
        dtype=left.dtype, shape=left.shape, source_a=left, source_b=right
    )


@compile_expression.register(n.SubtractExpression)
def compile_subtract_expression(
    expr: n.SubtractExpression,
    *,
    scope: Scope,
    context: Context,
) -> ir.ArrayExpression:
    left = compile_expression(expr.left, scope=scope, context=context)
    right = compile_expression(expr.right, scope=scope, context=context)

    if left.dtype != right.dtype:
        raise Exception(
            f"Type error: cannot subtract {right.dtype} from {left.dtype}"
        )

    if left.shape != right.shape:
        raise AssertionError("Shape mismatch error")

    return ir.SubtractExpression(
        dtype=left.dtype, shape=left.shape, source_a=left, source_b=right
    )


@compile_expression.register(n.MultiplyExpression)
def compile_multiply_expression(
    expr: n.MultiplyExpression,
    *,
    scope: Scope,
    context: Context,
) -> ir.ArrayExpression:
    left = compile_expression(expr.left, scope=scope, context=context)
    right = compile_expression(expr.right, scope=scope, context=context)

    if left.dtype != right.dtype:
        raise Exception(
            f"Type error: cannot multiply {left.dtype} by {left.dtype}"
        )

    if left.shape != right.shape:
        raise AssertionError("Shape mismatch error")

    return ir.MultiplyExpression(
        dtype=left.dtype, shape=left.shape, source_a=left, source_b=right
    )


@compile_expression.register(n.DivideExpression)
def compile_divide_expression(
    expr: n.DivideExpression,
    *,
    scope: Scope,
    context: Context,
) -> ir.ArrayExpression:
    left = compile_expression(expr.left, scope=scope, context=context)
    right = compile_expression(expr.right, scope=scope, context=context)

    if left.dtype != right.dtype:
        raise Exception(
            f"Type error: cannot divide {left.dtype} by {left.dtype}"
        )

    if left.shape != right.shape:
        raise AssertionError("Shape mismatch error")

    return ir.DivideExpression(
        dtype=left.dtype, shape=left.shape, source_a=left, source_b=right
    )


@compile_expression.register(n.EqualToExpression)
def compile_equal_to_expression(
    expr: n.EqualToExpression,
    *,
    scope: Scope,
    context: Context,
) -> ir.ArrayExpression:
    left = compile_expression(expr.left, scope=scope, context=context)
    right = compile_expression(expr.right, scope=scope, context=context)

    if left.dtype != right.dtype:
        raise Exception(
            f"Type error: cannot compare {left.dtype} to {left.dtype}"
        )

    if left.shape != right.shape:
        raise AssertionError("Shape mismatch error")

    return ir.EqualToExpression(
        dtype=ir.DType.BOOL, shape=left.shape, source_a=left, source_b=right
    )


@singledispatch
def table_expression_name(expr: n.TableExpression) -> str:
    return ""


@table_expression_name.register(n.TableReferenceExpression)
def table_reference_expression_name(expr: n.TableReferenceExpression) -> str:
    return expr.name


@singledispatch
def compile_table_expression(
    expr: n.TableExpression, *, context: Context
) -> Scope:
    raise NotImplementedError(
        f"compile_table_expression not implemented for {type(expr).__name__}"
    )


@compile_table_expression.register(n.TableReferenceExpression)
def compile_reference_table_expression(
    expr: n.TableReferenceExpression, *, context: Context
) -> Scope:
    src_scope = context.bindings[expr.name]

    result = Scope(
        columns=[
            ScopeColumn(
                name=src_column.name,
                namespaces={None},
                expression=src_column.expression,
            )
            for src_column in src_scope.columns
        ],
    )

    context.trace(
        result,
        ast_node=expr,
        level=ir.Level.TABLE_EXPRESSION,
    )

    return result


@singledispatch
def compile_column_binding(
    binding: n.ColumnBinding,
    *,
    scope: Scope,
    context: Context,
) -> Iterable[ScopeColumn]:
    raise NotImplementedError()


@compile_column_binding.register(n.WildcardColumnBinding)
def compile_wildcard_column_binding(
    binding: n.WildcardColumnBinding,
    *,
    scope: Scope,
    context: Context,
) -> Iterable[ScopeColumn]:
    for column in scope.columns:
        yield ScopeColumn(
            name=column.name, namespaces={None}, expression=column.expression
        )


@compile_column_binding.register(n.ImplicitColumnBinding)
def compile_plain_column_binding(
    binding: n.ImplicitColumnBinding,
    *,
    scope: Scope,
    context: Context,
) -> Iterable[ScopeColumn]:
    column_expr = compile_expression(
        binding.expression,
        context=context,
        scope=scope,
    )

    name = expression_name(binding.expression)

    yield ScopeColumn(name=name, namespaces={None}, expression=column_expr)


@compile_column_binding.register(n.AliasedColumnBinding)
def compile_aliased_column_binding(
    binding: n.AliasedColumnBinding,
    *,
    scope: Scope,
    context: Context,
) -> Iterable[ScopeColumn]:
    column_expr = compile_expression(
        binding.expression,
        context=context,
        scope=scope,
    )

    name = binding.alias.column_name

    yield ScopeColumn(name=name, namespaces={None}, expression=column_expr)


@compile_table_expression.register(n.SelectExpression)
def compile_select_table_expression(
    expr: n.SelectExpression, *, context: Context
) -> Scope:
    column_expr: ir.ArrayExpression

    src_scope = compile_table_expression(
        expr.source.source.expression, context=context
    )
    if expr.source.source.alias is not None:
        src_name = expr.source.source.alias.name
    else:
        src_name = table_expression_name(expr.source.source.expression)

    columns = []
    for src_column in src_scope.columns:
        column = ScopeColumn(
            name=src_column.name,
            namespaces={None, src_name, *src_column.namespaces},
            expression=src_column.expression,
        )
        columns.append(column)
    # Not traced because same expression should already have been traced above
    # in the call to `compile_table_expression`.
    src_scope = Scope(columns=columns)

    for join_clause in expr.join:
        join_scope = compile_table_expression(
            join_clause.table.expression, context=context
        )

        if join_clause.table.alias is not None:
            join_name = join_clause.table.alias.name
        else:
            join_name = table_expression_name(join_clause.table.expression)

        # Build table to act as environment for predicate.
        shape_a = src_scope.columns[0].expression.shape
        shape_b = join_scope.columns[0].expression.shape
        shape_full = ir.JoinShapeExpression(shape_a=shape_a, shape_b=shape_b)

        left_index_full = ir.JoinLeftExpression(
            dtype=ir.DType.INDEX,
            shape=shape_full,
            shape_a=shape_a,
            shape_b=shape_b,
        )
        right_index_full = ir.JoinRightExpression(
            dtype=ir.DType.INDEX,
            shape=shape_full,
            shape_a=shape_a,
            shape_b=shape_b,
        )

        # Create a scope to use as the context for the predicate.
        columns = []
        for column in src_scope.columns:
            column_expr = ir.PickExpression(
                dtype=column.expression.dtype,
                shape=shape_full,
                source=column.expression,
                indexes=left_index_full,
            )
            new_column = ScopeColumn(
                name=column.name,
                namespaces={*column.namespaces},
                expression=column_expr,
            )
            columns.append(new_column)

        for column in join_scope.columns:
            column_expr = ir.PickExpression(
                dtype=column.expression.dtype,
                shape=shape_full,
                source=column.expression,
                indexes=right_index_full,
            )
            new_column = ScopeColumn(
                name=column.name,
                namespaces={join_name, *column.namespaces},
                expression=column_expr,
            )
            columns.append(new_column)

        # Do not trace the full join scope!  We'd like to be able to optimise it
        # away.  For tracing, we should generate a new table containing only the
        # rows which evaluate to true.
        join_scope_full = Scope(columns=columns)

        if not isinstance(join_clause.constraint, n.JoinOnConstraint):
            raise NotImplementedError()

        mask_expression = compile_expression(
            join_clause.constraint.predicate,
            context=context,
            scope=join_scope_full,
        )

        # Apply the mask to create the output table.
        shape = ir.WhereShapeExpression(
            mask=mask_expression,
        )

        left_index = ir.WhereExpression(
            dtype=ir.DType.INDEX,
            shape=shape,
            source=left_index_full,
            mask=mask_expression,
        )
        right_index = ir.WhereExpression(
            dtype=ir.DType.INDEX,
            shape=shape,
            source=right_index_full,
            mask=mask_expression,
        )

        columns = []
        for column in src_scope.columns:
            column_expr = ir.PickExpression(
                dtype=column.expression.dtype,
                shape=shape,
                source=column.expression,
                indexes=left_index,
            )
            new_column = ScopeColumn(
                name=column.name,
                namespaces={*column.namespaces},
                expression=column_expr,
            )
            columns.append(new_column)

        for column in join_scope.columns:
            column_expr = ir.PickExpression(
                dtype=column.expression.dtype,
                shape=shape,
                source=column.expression,
                indexes=right_index,
            )
            new_column = ScopeColumn(
                name=column.name,
                namespaces={join_name, *column.namespaces},
                expression=column_expr,
            )
            columns.append(new_column)

        src_scope = Scope(columns=columns)
        context.trace(src_scope, ast_node=join_clause, level=ir.Level.INTERNAL)

    if expr.where is not None:
        condition_expr = compile_expression(
            expr.where.predicate,
            context=context,
            scope=src_scope,
        )
        # TODO inject trace table.

        shape = ir.WhereShapeExpression(mask=condition_expr)

        columns = []
        for src_column in src_scope.columns:
            column_expr = ir.WhereExpression(
                dtype=src_column.expression.dtype,
                shape=shape,
                source=src_column.expression,
                mask=condition_expr,
            )

            column = ScopeColumn(
                name=src_column.name,
                namespaces=src_column.namespaces,
                expression=column_expr,
            )
            columns.append(column)
        src_scope = Scope(columns=columns)

    if expr.group_by is not None:
        raise NotImplementedError()

    columns_by_name = {}
    for column_binding in expr.columns:
        for column in compile_column_binding(
            column_binding, scope=src_scope, context=context
        ):
            columns_by_name[column.name] = column
    columns = list(columns_by_name.values())

    scope = Scope(columns=columns)
    context.trace(scope, ast_node=expr, level=ir.Level.STATEMENT)

    return scope


def _arrow_type_to_ir_type(arrow_type: pa.DataType) -> ir.DType:
    return {
        pa.bool_(): ir.DType.BOOL,
        pa.int32(): ir.DType.INT32,
        pa.int64(): ir.DType.INT64,
        pa.float32(): ir.DType.DOUBLE,
        pa.float64(): ir.DType.DOUBLE,
        pa.string(): ir.DType.TEXT,
        pa.binary(): ir.DType.BYTES,
    }[arrow_type]


@compile_table_expression.register(n.ImportExpression)
def compile_import_table_expression(
    expr: n.ImportExpression, *, context: Context
) -> Scope:
    location = expr.location.value

    schema = context.importer.import_schema(location)

    shape = ir.ImportShapeExpression(location=location)

    columns = []
    for column_name in schema.names:
        column_type = schema.field(column_name).type

        column_expr = ir.ImportExpression(
            dtype=_arrow_type_to_ir_type(column_type),
            shape=shape,
            location=location,
            name=column_name,
        )
        column = ScopeColumn(
            name=column_name,
            namespaces={None},
            expression=column_expr,
        )
        columns.append(column)

    new_scope = Scope(
        columns=columns,
    )

    context.trace(new_scope, ast_node=expr, level=ir.Level.TABLE_EXPRESSION)

    return new_scope


@singledispatch
def compile_statement(stmt: n.Statement, *, context: Context) -> None:
    raise NotImplementedError()


@compile_statement.register(n.AssignmentStatement)
def compile_assignment_statement(
    stmt: n.AssignmentStatement, *, context: Context
) -> None:
    expr_table = compile_table_expression(stmt.expression, context=context)

    stmt_scope = Scope(
        columns=_strip_namespaces(expr_table.columns),
    )
    context.trace(stmt_scope, ast_node=stmt, level=ir.Level.STATEMENT)

    context.bindings[stmt.target.name] = stmt_scope


@compile_statement.register(n.ExportStatement)
def compile_export_statement(
    stmt: n.ExportStatement, *, context: Context
) -> None:
    expr_table = compile_table_expression(stmt.expression, context=context)

    stmt_scope = Scope(
        columns=_strip_namespaces(expr_table.columns),
    )

    context.trace(
        stmt_scope,
        ast_node=stmt,
        level=ir.Level.EXPORT,
    )
    context.export(stmt_scope, name=stmt.location.value)


def compile_ast_to_ir(
    source: n.StatementList, *, importer: Importer
) -> ir.Program:
    context = Context(importer=importer)

    for statement in source.statements:
        compile_statement(statement, context=context)

    program = ir.Program(tables=context.tables)

    return program
