from __future__ import annotations

from dataclasses import dataclass
from functools import singledispatch
from typing import Dict

import pyarrow as pa
import pyarrow.compute as pac

from dtl import cmd, ir
from dtl.ast_to_ir import compile_ast_to_ir
from dtl.io import Exporter, Importer, InMemoryExporter, InMemoryImporter
from dtl.ir_to_cmd import compile_ir_to_cmd
from dtl.lexer import tokenize
from dtl.parser import parse


@dataclass(frozen=True)
class Context:
    arrays: Dict[ir.ArrayExpression, pa.Array]
    shapes: Dict[ir.ShapeExpression, int]
    importer: Importer
    exporter: Exporter


@singledispatch
def eval_expression(expression: ir.Expression, context: Context) -> None:
    raise NotImplementedError(
        f"eval_expression not implemented for {type(expression).__name__}"
    )


@eval_expression.register(ir.ImportShapeExpression)
def eval_import_shape_expression(
    expression: ir.ImportShapeExpression, context: Context
) -> None:
    importer = context.importer
    table = importer.import_table(expression.location)
    result = len(table)
    context.shapes[expression] = result


@eval_expression.register(ir.WhereShapeExpression)
def eval_where_shape_expression(
    expression: ir.WhereShapeExpression, context: Context
) -> None:
    result = pac.sum(context.arrays[expression.mask]).as_py()
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
    context.arrays[expression] = result


@eval_expression.register(ir.IntegerLiteralExpression)
def eval_integer_literal_expression(
    expression: ir.IntegerLiteralExpression, context: Context
) -> None:
    shape = context.shapes[expression.shape]
    result = pa.array([expression.value] * shape, type=pa.int64())
    context.arrays[expression] = result


@eval_expression.register(ir.FloatLiteralExpression)
def eval_float_literal_expression(
    expression: ir.FloatLiteralExpression, context: Context
) -> None:
    shape = context.shapes[expression.shape]
    result = pa.array([expression.value] * shape, type=pa.float64())
    context.arrays[expression] = result


@eval_expression.register(ir.TextLiteralExpression)
def eval_text_literal_expression(
    expression: ir.TextLiteralExpression, context: Context
) -> None:
    shape = context.shapes[expression.shape]
    result = pa.array([expression.value] * shape, type=pa.text())
    context.arrays[expression] = result


@eval_expression.register(ir.BytesLiteralExpression)
def eval_bytes_literal_expression(
    expression: ir.BytesLiteralExpression, context: Context
) -> None:
    shape = context.shapes[expression.shape]
    result = pa.array([expression.value] * shape, type=pa.bytes())
    context.arrays[expression] = result


@eval_expression.register(ir.ImportExpression)
def eval_import_expression(
    expression: ir.ImportExpression, context: Context
) -> None:
    importer = context.importer
    table = importer.import_table(expression.location)
    result = table[expression.name]
    context.arrays[expression] = result


@eval_expression.register(ir.WhereExpression)
def eval_where_expression(
    expression: ir.WhereExpression, context: Context
) -> None:
    source = context.arrays[expression.source]
    mask = context.arrays[expression.mask]
    result = pac.filter(source, mask)
    context.arrays[expression] = result


@eval_expression.register(ir.PickExpression)
def eval_pick_expression(
    expression: ir.PickExpression, context: Context
) -> None:
    source = context.arrays[expression.source]
    indexes = context.arrays[expression.indexes]
    result = pac.take(source, indexes)
    context.arrays[expression] = result


@eval_expression.register(ir.JoinLeftExpression)
def eval_join_left_expression(
    expression: ir.JoinLeftExpression, context: Context
) -> None:
    len_a = context.shapes[expression.shape_a]
    len_b = context.shapes[expression.shape_b]
    result = pa.chunked_array(
        [[x for x in range(len_a) for _ in range(len_b)]]
    )
    context.arrays[expression] = result


@eval_expression.register(ir.JoinRightExpression)
def eval_join_right_expression(
    expression: ir.JoinRightExpression, context: Context
) -> None:
    len_a = context.shapes[expression.shape_a]
    len_b = context.shapes[expression.shape_b]
    result = pa.chunked_array(
        [[x for _ in range(len_a) for x in range(len_b)]]
    )
    context.arrays[expression] = result


@eval_expression.register(ir.AddExpression)
def eval_add_expression(
    expression: ir.AddExpression, context: Context
) -> None:
    a = context.arrays[expression.source_a]
    b = context.arrays[expression.source_b]
    result = pac.add(a, b)
    context.arrays[expression] = result


@eval_expression.register(ir.SubtractExpression)
def eval_subtract_expression(
    expression: ir.SubtractExpression, context: Context
) -> None:
    a = context.arrays[expression.source_a]
    b = context.arrays[expression.source_b]
    result = pac.subtract(a, b)
    context.arrays[expression] = result


@eval_expression.register(ir.MultiplyExpression)
def eval_multiply_expression(
    expression: ir.MultiplyExpression, context: Context
) -> None:
    a = context.arrays[expression.source_a]
    b = context.arrays[expression.source_b]
    result = pac.multiply(a, b)
    context.arrays[expression] = result


@eval_expression.register(ir.DivideExpression)
def eval_divide_expression(
    expression: ir.DivideExpression, context: Context
) -> None:
    a = context.arrays[expression.source_a]
    b = context.arrays[expression.source_b]
    result = pac.divide(a, b)
    context.arrays[expression] = result


@eval_expression.register(ir.EqualToExpression)
def eval_equal_to_expression(
    expression: ir.EqualToExpression, context: Context
) -> None:
    a = context.arrays[expression.source_a]
    b = context.arrays[expression.source_b]
    result = pac.equal(a, b)
    context.arrays[expression] = result


@singledispatch
def eval_command(command: cmd.Command, context: Context) -> None:
    raise NotImplementedError(
        f"eval_command not implemented for {type(command).__name__}"
    )


@eval_command.register(cmd.EvaluateArrayCommand)
def _eval_evaluate_array_command(
    command: cmd.EvaluateArrayCommand, context: Context
) -> None:
    eval_expression(command.expression, context=context)


@eval_command.register(cmd.EvaluateShapeCommand)
def _eval_evaluate_shape_command(
    command: cmd.EvaluateShapeCommand, context: Context
) -> None:
    eval_expression(command.expression, context=context)


@eval_command.register(cmd.CollectArrayCommand)
def _eval_collect_array_command(
    command: cmd.CollectArrayCommand, context: Context
) -> None:
    context.arrays.pop(command.expression)


@eval_command.register(cmd.TraceArrayCommand)
def _eval_trace_array_command(
    command: cmd.TraceArrayCommand, context: Context
) -> None:
    # TODO
    pass


@eval_command.register(cmd.ExportTableCommand)
def _eval_export_table_command(
    command: cmd.ExportTableCommand, context: Context
) -> None:
    exporter = context.exporter
    table = pa.table(
        {
            name: context.arrays[expression]
            for name, expression in command.columns.items()
        }
    )
    exporter.export_table(command.name, table)


def evaluate(source: str, inputs: Dict[str, pa.Table]) -> Dict[str, pa.Table]:
    importer = InMemoryImporter(inputs)
    exporter = InMemoryExporter()

    ast = parse(tokenize(source))

    program = compile_ast_to_ir(ast, importer=importer)

    roots: set[ir.Expression] = {
        column.expression
        for table in program.tables
        for column in table.columns
    }

    commands = compile_ir_to_cmd(roots)
    for table in program.tables:
        if table.export_as is None:
            continue

        commands.append(
            cmd.ExportTableCommand(
                name=table.export_as,
                columns={
                    column.name: column.expression for column in table.columns
                },
            )
        )

    print(commands)

    context = Context(
        shapes={}, arrays={}, importer=importer, exporter=exporter
    )

    for command in commands:
        eval_command(command, context=context)

    return exporter.results()
