from __future__ import annotations

from dataclasses import dataclass
from functools import singledispatch
from typing import Dict

import pyarrow as pa
import pyarrow.compute as pac

from dtl import ir as ir
from dtl.ast_to_ir import compile_ast_to_ir
from dtl.lexer import tokenize
from dtl.parser import parse


@dataclass(frozen=True)
class Context:
    shapes: Dict[ir.Shape, int]
    results: Dict[ir.Expression, pa.Array]
    inputs: Dict[str, pa.Table]


@singledispatch
def eval_expression(expression: ir.Expression, context: Context) -> pa.Array:
    raise NotImplementedError(
        f"eval_expression not implemented for {type(expression).__name__}"
    )


@eval_expression.register(ir.ImportExpression)
def eval_import_expression(
    expression: ir.ImportExpression, context: Context
) -> pa.Array:
    return context.inputs[expression.location][expression.name]


@eval_expression.register(ir.WhereExpression)
def eval_where_expression(
    expression: ir.WhereExpression, context: Context
) -> pa.Array:
    raise NotImplementedError()

@eval_expression.register(ir.PickExpression)
def eval_pick_expression(
    expression: ir.PickExpression, context: Context
) -> pa.Array:
    source = context.results[expression.source]
    indexes = context.results[expression.indexes]
    return source[indexes]


@eval_expression.register(ir.JoinLeftExpression)
def eval_join_left_expression(
    expression: ir.JoinLeftExpression, context: Context
) -> pa.Array:
    len_a = context.shapes[expression.shape_a]
    len_b = context.shapes[expression.shape_b]
    return pa.chunked_array([[x for x in range(len_a) for _ in range(len_b)]])


@eval_expression.register(ir.JoinRightExpression)
def eval_join_right_expression(
    expression: ir.JoinRightExpression, context: Context
) -> pa.Array:
    len_a = context.shapes[expression.shape_a]
    len_b = context.shapes[expression.shape_b]
    return pa.chunked_array([[x for _ in range(len_a) for x in range(len_b)]])


@eval_expression.register(ir.AddExpression)
def eval_add_expression(
    expression: ir.AddExpression, context: Context
) -> pa.Array:
    a = context.results[expression.source_a]
    b = context.results[expression.source_b]
    return pac.add(a, b)


@eval_expression.register(ir.SubtractExpression)
def eval_subtract_expression(
    expression: ir.SubtractExpression, context: Context
) -> pa.Array:
    a = context.results[expression.source_a]
    b = context.results[expression.source_b]
    return pac.subtract(a, b)


@eval_expression.register(ir.MultiplyExpression)
def eval_multiply_expression(
    expression: ir.MultiplyExpression, context: Context
) -> pa.Array:
    a = context.results[expression.source_a]
    b = context.results[expression.source_b]
    return pac.multiply(a, b)


@eval_expression.register(ir.DivideExpression)
def eval_divide_expression(
    expression: ir.DivideExpression, context: Context
) -> pa.Array:
    a = context.results[expression.source_a]
    b = context.results[expression.source_b]
    return pac.divide(a, b)


def evaluate(source: str, inputs: Dict[str, pa.Table]) -> Dict[str, pa.Table]:
    ast = parse(tokenize(source))

    program = compile_ast_to_ir(
        ast,
        input_types={
            name: list(table.column_names) for name, table in inputs.items()
        },
    )

    context = Context(shapes={}, results={}, inputs=inputs)
    roots = {
        column.expression
        for table in program.tables
        for column in table.columns
    }

    for expression in ir.traverse_depth_first(roots):
        result = eval_expression(expression, context=context)
        context.results[expression] = result
        if expression.shape in context.shapes:
            assert context.shapes[expression.shape] == len(result)
        context.shapes[expression.shape] = len(result)

    print(context)

    exports = {}
    for name, table in program.exports.items():
        exports[name] = pa.table(
            {
                column.name: context.results[column.expression]
                for column in table.columns
            }
        )

    return exports
