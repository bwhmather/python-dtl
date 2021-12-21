from __future__ import annotations

import dataclasses
from functools import singledispatch
from typing import Dict, Optional

import pyarrow as pa

from dtl import nodes as n
from dtl.lexer import tokenize
from dtl.parser import parse


@dataclasses.dataclass(frozen=True)
class Environment:
    inputs: Dict[str, pa.Table]
    globals: Dict[str, pa.Table]
    outputs: Dict[str, pa.Table]


@dataclasses.dataclass(frozen=True)
class Scope:
    parent: Optional[Scope]
    locals: Dict[str, pa.Table]


@singledispatch
def expression_name(expr: n.Expression) -> str:
    raise Exception("no name could be derived for expression")


@expression_name.register(n.ColumnReferenceExpression)
def column_reference_expression_name(expr: n.ColumnReferenceExpression) -> str:
    # TODO qualified column names.
    return expr.name.column_name


@singledispatch
def eval_expression(
    expr: n.Expression, *, scope: Scope, environment: Environment
) -> pa.Array:
    raise NotImplementedError(
        f"eval_expression not implemented for {type(expr).__name__}"
    )


@eval_expression.register(n.ColumnReferenceExpression)
def eval_column_reference_expression(
    expr: n.ColumnReferenceExpression,
    *,
    scope: Scope,
    environment: Environment,
) -> pa.Array:
    if isinstance(expr.name, n.UnqualifiedColumnName):
        while scope:
            for table in scope.locals.values():
                if expr.name.column_name in table.column_names:
                    return table[expr.name.column_name]
            scope = scope.parent
        raise Exception(f"could not find {expr.name.column_name}")

    elif isinstance(expr.name, n.QualifiedColumnName):
        while scope:
            if expr.name.table_name in scope.locals:
                return scope.locals[expr.name.table_name][
                    expr.name.column_name
                ]
        raise Exception()

    else:
        raise AssertionError()


@singledispatch
def table_expression_name(expr: n.TableExpression) -> str:
    return ""


@table_expression_name.register(n.TableReferenceExpression)
def table_reference_expression_name(expr: n.TableReferenceExpression) -> str:
    return expr.name


@singledispatch
def eval_table_expression(
    expr: n.TableExpression, *, environment: Environment
) -> pa.Table:
    raise NotImplementedError(
        f"eval_table_expression not implemented for {type(expr).__name__}"
    )


@eval_table_expression.register(n.TableReferenceExpression)
def eval_reference_table_expression(
    expr: n.TableReferenceExpression, *, environment: Environment
) -> pa.Table:
    return environment.globals[expr.name]


@eval_table_expression.register(n.SelectExpression)
def eval_select_table_expression(
    expr: n.SelectExpression, *, environment: Environment
) -> pa.Table:
    source_table = eval_table_expression(
        expr.source.source.expression, environment=environment
    )
    if expr.source.source.alias is not None:
        source_name = expr.source.source.alias
    else:
        source_name = table_expression_name(expr.source.source.expression)

    if expr.join:
        raise NotImplementedError()

    if expr.where is not None:
        raise NotImplementedError()

    if expr.group_by is not None:
        raise NotImplementedError()

    columns = {}
    scope = Scope(parent=None, locals={source_name: source_table})
    for column_binding in expr.columns:
        column = eval_expression(
            column_binding.expression, scope=scope, environment=environment
        )
        if column_binding.alias is not None:
            name = column_binding.alias.column_name
        else:
            name = expression_name(column_binding.expression)
        columns[name] = column
    return pa.table(columns)


@eval_table_expression.register(n.ImportExpression)
def eval_import_table_expression(
    expr: n.ImportExpression, *, environment: Environment
) -> pa.Table:
    return environment.inputs[expr.location.value]


@singledispatch
def eval_statement(stmt: n.Statement, *, environment: Environment) -> None:
    raise NotImplementedError()


@eval_statement.register(n.AssignmentStatement)
def eval_assignment_statement(
    stmt: n.AssignmentStatement, *, environment: Environment
) -> None:
    environment.globals[stmt.target.name] = eval_table_expression(
        stmt.expression, environment=environment
    )


@eval_statement.register(n.ExportStatement)
def eval_export_statement(
    stmt: n.ExportStatement, *, environment: Environment
) -> None:
    environment.outputs[stmt.location.value] = eval_table_expression(
        stmt.expression, environment=environment
    )


def evaluate(source: str, inputs: Dict[str, pa.Table]) -> Dict[str, pa.Table]:
    ast = parse(tokenize(source))
    environment = Environment(inputs=inputs, globals={}, outputs={})
    for stmt in ast.statements:
        eval_statement(stmt, environment=environment)
    return environment.outputs
