from __future__ import annotations

import dataclasses
from functools import singledispatch
from typing import Dict, List

from dtl import ir as ir
from dtl import nodes as n


def _strip_namespaces(columns: List[ir.Column]) -> List[ir.Column]:
    output = []
    for column in columns:
        if None not in column.namespaces:
            raise Exception("compilation error")
        output.append(
            ir.Column(
                name=column.name,
                namespaces={None},
                expression=column.expression,
            )
        )
    return output


@dataclasses.dataclass(frozen=True)
class Context:
    inputs: Dict[str, ir.Table]
    globals: Dict[str, ir.Table]


@singledispatch
def expression_name(expr: n.Expression) -> str:
    raise Exception("no name could be derived for expression")


@expression_name.register(n.ColumnReferenceExpression)
def column_reference_expression_name(expr: n.ColumnReferenceExpression) -> str:
    # TODO qualified column names.
    return expr.name.column_name


@singledispatch
def compile_expression(
    expr: n.Expression,
    *,
    scope: ir.Table,
    program: ir.Program,
    context: Context,
) -> ir.Expression:
    raise NotImplementedError(
        f"compile_expression not implemented for {type(expr).__name__}"
    )


@compile_expression.register(n.ColumnReferenceExpression)
def compile_column_reference_expression(
    expr: n.ColumnReferenceExpression,
    *,
    scope: ir.Table,
    program: ir.Program,
    context: Context,
) -> ir.Expression:
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

    raise Exception(f"could not find {expr.name.column_name}")


@singledispatch
def table_expression_name(expr: n.TableExpression) -> str:
    return ""


@table_expression_name.register(n.TableReferenceExpression)
def table_reference_expression_name(expr: n.TableReferenceExpression) -> str:
    return expr.name


@singledispatch
def compile_table_expression(
    expr: n.TableExpression, *, program: ir.Program, context: Context
) -> ir.Table:
    raise NotImplementedError(
        f"compile_table_expression not implemented for {type(expr).__name__}"
    )


@compile_table_expression.register(n.TableReferenceExpression)
def compile_reference_table_expression(
    expr: n.TableReferenceExpression, *, program: ir.Program, context: Context
) -> ir.Table:
    src_table = context.globals[expr.name]

    table = ir.Table(
        ast_node=expr,
        level=ir.Level.TABLE_EXPRESSION,
        columns=[
            ir.Column(
                name=src_column.name,
                namespaces={None},
                expression=src_column.expression,
            )
            for src_column in src_table.columns
        ],
    )
    program.tables.append(table)

    return table


@compile_table_expression.register(n.SelectExpression)
def compile_select_table_expression(
    expr: n.SelectExpression, *, program: ir.Program, context: Context
) -> ir.Table:
    src_table = compile_table_expression(
        expr.source.source.expression, program=program, context=context
    )
    if expr.source.source.alias is not None:
        src_name = expr.source.source.alias
    else:
        src_name = table_expression_name(expr.source.source.expression)

    columns = []
    for src_column in src_table.columns:
        column = ir.Column(
            name=src_column.name,
            namespaces={None, src_name, *src_column.namespaces},
            expression=src_column.expression,
        )
        columns.append(column)
    src_table = ir.Table(
        ast_node=None, level=ir.Level.INTERNAL, columns=columns
    )
    program.tables.append(src_table)

    for join_expr in expr.join:
        # src_table = environment.push_table(
        #    ir.Join()
        raise NotImplementedError()

    if expr.where is not None:
        condition_expr = compile_expression(
            expr.where,
            program=program,
            context=context,
            scope=src_table,
        )
        # TODO inject trace table.

        columns = []
        for src_column in src_table.columns:
            column_expr = ir.Where(
                source=src_column.expression, mask=condition_expr
            )

            column = ir.Column(
                name=src_column.name,
                namespaces=src_column.namespaces,
                expression=column_expr,
            )
            columns.append(column)
        src_table = ir.Table(
            ast_node=None, level=ir.Level.INTERNAL, columns=columns
        )

    if expr.group_by is not None:
        raise NotImplementedError()

    columns = []
    for column_binding in expr.columns:
        column_expr = compile_expression(
            column_binding.expression,
            program=program,
            context=context,
            scope=src_table,
        )
        # TODO inject trace table.

        if column_binding.alias is not None:
            name = column_binding.alias.column_name
        else:
            name = expression_name(column_binding.expression)

        column = ir.Column(
            name=name, namespaces={None}, expression=column_expr
        )
        columns.append(column)

    table = ir.Table(ast_node=expr, level=ir.Level.STATEMENT, columns=columns)
    program.tables.append(table)

    return table


@compile_table_expression.register(n.ImportExpression)
def compile_import_table_expression(
    expr: n.ImportExpression, *, program: ir.Program, context: Context
) -> ir.Table:
    location = expr.location.value
    input = context.inputs[location]

    columns = []
    for src_column in input.columns:
        column = ir.Column(
            name=src_column.name,
            namespaces={None},
            expression=src_column.expression,
        )

        columns.append(column)

    table = ir.Table(
        ast_node=expr,
        level=ir.Level.TABLE_EXPRESSION,
        columns=columns,
    )
    program.tables.append(table)

    return context.inputs[expr.location.value]


@singledispatch
def compile_statement(
    stmt: n.Statement, *, program: ir.Program, context: Context
) -> None:
    raise NotImplementedError()


@compile_statement.register(n.AssignmentStatement)
def compile_assignment_statement(
    stmt: n.AssignmentStatement, *, program: ir.Program, context: Context
) -> None:
    expr_table = compile_table_expression(
        stmt.expression, program=program, context=context
    )

    stmt_table = ir.Table(
        ast_node=stmt,
        level=ir.Level.STATEMENT,
        columns=_strip_namespaces(expr_table.columns),
    )
    program.tables.append(stmt_table)

    context.globals[stmt.target.name] = stmt_table


@compile_statement.register(n.ExportStatement)
def compile_export_statement(
    stmt: n.ExportStatement, *, program: ir.Program, context: Context
) -> None:
    expr_table = compile_table_expression(
        stmt.expression, program=program, context=context
    )

    stmt_table = ir.Table(
        ast_node=stmt,
        level=ir.Level.EXPORT,
        columns=_strip_namespaces(expr_table.columns),
    )
    program.tables.append(stmt_table)

    program.exports[stmt.location.value] = stmt_table


def compile_ast_to_ir(
    source: n.StatementList, *, input_types: Dict[str, List[str]]
) -> ir.Program:
    program = ir.Program(expressions=[], tables=[], exports={})
    context = Context(inputs={}, globals={})

    for location, column_names in input_types.items():
        columns = []
        for name in column_names:
            column_expr = ir.Import(
                location=location,
                name=name,
            )
            program.expressions.append(column_expr)
            column = ir.Column(
                name=name,
                namespaces={None},
                expression=column_expr,
            )
            columns.append(column)
        table = ir.Table(
            ast_node=None, level=ir.Level.INTERNAL, columns=columns
        )
        program.tables.append(table)
        context.inputs[location] = table

    for statement in source.statements:
        compile_statement(statement, program=program, context=context)

    return program