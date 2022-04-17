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
    shapes: Dict[ir.ShapeExpression, int]
    results: Dict[ir.ArrayExpression, pa.Array]
    inputs: Dict[str, pa.Table]


@singledispatch
def eval_expression(expression: ir.Expression, context: Context) -> None:
    raise NotImplementedError(
        f"eval_expression not implemented for {type(expression).__name__}"
    )


@eval_expression.register(ir.ImportShapeExpression)
def eval_import_shape_expression(
    expression: ir.ImportShapeExpression, context: Context
) -> None:
    result = len(context.inputs[expression.location])
    context.shapes[expression] = result


@eval_expression.register(ir.WhereShapeExpression)
def eval_where_shape_expression(
    expression: ir.WhereShapeExpression, context: Context
) -> None:
    result = pac.sum(context.results[expression.mask]).as_py()
    context.shapes[expression] = result


@eval_expression.register(ir.JoinShapeExpression)
def eval_join_shape_expression(
    expression: ir.JoinShapeExpression, context: Context
) -> None:
    shape_a = context.shapes[expression.shape_a]
    shape_b = context.shapes[expression.shape_b]
    result = shape_a * shape_b
    context.shapes[expression] = result


@eval_expression.register(ir.BooleanLiteralExpression)
def eval_boolean_literal_expression(
    expression: ir.BooleanLiteralExpression, context: Context
) -> None:
    shape = context.shapes[expression.shape]
    result = pa.array([expression.value] * shape, type=pa.bool())
    context.results[expression] = result


@eval_expression.register(ir.IntegerLiteralExpression)
def eval_integer_literal_expression(
    expression: ir.IntegerLiteralExpression, context: Context
) -> None:
    shape = context.shapes[expression.shape]
    result = pa.array([expression.value] * shape, type=pa.int64())
    context.results[expression] = result


@eval_expression.register(ir.FloatLiteralExpression)
def eval_float_literal_expression(
    expression: ir.FloatLiteralExpression, context: Context
) -> None:
    shape = context.shapes[expression.shape]
    result = pa.array([expression.value] * shape, type=pa.float64())
    context.results[expression] = result


@eval_expression.register(ir.TextLiteralExpression)
def eval_text_literal_expression(
    expression: ir.TextLiteralExpression, context: Context
) -> None:
    shape = context.shapes[expression.shape]
    result = pa.array([expression.value] * shape, type=pa.text())
    context.results[expression] = result


@eval_expression.register(ir.BytesLiteralExpression)
def eval_bytes_literal_expression(
    expression: ir.BytesLiteralExpression, context: Context
) -> None:
    shape = context.shapes[expression.shape]
    result = pa.array([expression.value] * shape, type=pa.bytes())
    context.results[expression] = result


@eval_expression.register(ir.ImportExpression)
def eval_import_expression(
    expression: ir.ImportExpression, context: Context
) -> None:
    result = context.inputs[expression.location][expression.name]
    context.results[expression] = result


@eval_expression.register(ir.WhereExpression)
def eval_where_expression(
    expression: ir.WhereExpression, context: Context
) -> None:
    source = context.results[expression.source]
    mask = context.results[expression.mask]
    result = pac.filter(source, mask)
    context.results[expression] = result


@eval_expression.register(ir.PickExpression)
def eval_pick_expression(
    expression: ir.PickExpression, context: Context
) -> None:
    source = context.results[expression.source]
    indexes = context.results[expression.indexes]
    result = pac.take(source, indexes)
    context.results[expression] = result


@eval_expression.register(ir.JoinLeftExpression)
def eval_join_left_expression(
    expression: ir.JoinLeftExpression, context: Context
) -> None:
    len_a = context.shapes[expression.shape_a]
    len_b = context.shapes[expression.shape_b]
    result = pa.chunked_array(
        [[x for x in range(len_a) for _ in range(len_b)]]
    )
    context.results[expression] = result


@eval_expression.register(ir.JoinRightExpression)
def eval_join_right_expression(
    expression: ir.JoinRightExpression, context: Context
) -> None:
    len_a = context.shapes[expression.shape_a]
    len_b = context.shapes[expression.shape_b]
    result = pa.chunked_array(
        [[x for _ in range(len_a) for x in range(len_b)]]
    )
    context.results[expression] = result


@eval_expression.register(ir.AddExpression)
def eval_add_expression(
    expression: ir.AddExpression, context: Context
) -> None:
    a = context.results[expression.source_a]
    b = context.results[expression.source_b]
    result = pac.add(a, b)
    context.results[expression] = result


@eval_expression.register(ir.SubtractExpression)
def eval_subtract_expression(
    expression: ir.SubtractExpression, context: Context
) -> None:
    a = context.results[expression.source_a]
    b = context.results[expression.source_b]
    result = pac.subtract(a, b)
    context.results[expression] = result


@eval_expression.register(ir.MultiplyExpression)
def eval_multiply_expression(
    expression: ir.MultiplyExpression, context: Context
) -> None:
    a = context.results[expression.source_a]
    b = context.results[expression.source_b]
    result = pac.multiply(a, b)
    context.results[expression] = result


@eval_expression.register(ir.DivideExpression)
def eval_divide_expression(
    expression: ir.DivideExpression, context: Context
) -> None:
    a = context.results[expression.source_a]
    b = context.results[expression.source_b]
    result = pac.divide(a, b)
    context.results[expression] = result


@eval_expression.register(ir.EqualToExpression)
def eval_equal_to_expression(
    expression: ir.EqualToExpression, context: Context
) -> None:
    a = context.results[expression.source_a]
    b = context.results[expression.source_b]
    result = pac.equal(a, b)
    context.results[expression] = result


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
        eval_expression(expression, context=context)

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
