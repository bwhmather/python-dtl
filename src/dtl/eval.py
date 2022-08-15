from __future__ import annotations

from dataclasses import dataclass
from functools import singledispatch
from typing import Dict, Iterable, Optional
from uuid import UUID, uuid4

import pyarrow as pa
import pyarrow.compute as pac

import dtl.manifest
from dtl import cmd, ir
from dtl.ast_to_ir import compile_ast_to_ir
from dtl.io import (
    Exporter,
    Importer,
    InMemoryExporter,
    InMemoryImporter,
    Tracer,
)
from dtl.ir_to_cmd import compile_ir_to_cmd
from dtl.lexer import tokenize
from dtl.parser import parse


@dataclass(frozen=True)
class Context:
    arrays: Dict[ir.ArrayExpression, pa.Array]
    shapes: Dict[ir.ShapeExpression, int]
    importer: Importer
    exporter: Exporter
    tracer: Optional[Tracer]


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
    tracer = context.tracer
    assert tracer is not None
    array = context.arrays[command.expression]
    tracer.write_array(command.uuid, array)


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


@dataclass
class Mapping:
    pass


def extract_trace_tables(
    tables: list[ir.Table], /, *, level: ir.Level
) -> list[ir.TraceTable]:
    return [
        table
        for table in tables
        if isinstance(table, ir.TraceTable)  # TODO and table.level >= level
    ]


def extract_export_tables(
    tables: Iterable[ir.Table], /
) -> list[ir.ExportTable]:
    return [table for table in tables if isinstance(table, ir.ExportTable)]


def get_mapping_roots(mappings: Iterable[Mapping], /) -> list[ir.Expression]:
    if mappings:
        raise NotImplementedError()
    return []


def get_table_roots(tables: Iterable[ir.Table], /) -> list[ir.Expression]:
    return list(
        {column.expression for table in tables for column in table.columns}
    )


def create_manifest(
    *,
    source: str,
    mappings: list[Mapping],
    snapshots: list[ir.TraceTable],
    names: dict[ir.Expression, UUID],
) -> dtl.manifest.Manifest:
    return dtl.manifest.Manifest(
        source=source,
        snapshots=[
            dtl.manifest.Snapshot(
                start=table.ast_node.start,
                end=table.ast_node.end,
                columns=[
                    dtl.manifest.Column(
                        name=column.name, array=names[column.expression]
                    )
                    for column in table.columns
                ],
            )
            for table in snapshots
        ],
        mappings=[],
    )


def run(
    source: str,
    /,
    *,
    importer: Importer,
    exporter: Exporter,
    tracer: Optional[Tracer],
) -> None:
    # === Parse Source Code ====================================================
    tokens = tokenize(source)
    ast = parse(tokens)

    # === Compile AST to list of tables referencing IR expressions =============
    program = compile_ast_to_ir(ast, importer=importer)

    trace_tables = extract_trace_tables(
        program.tables, level=ir.Level.COLUMN_EXPRESSION
    )
    export_tables = extract_export_tables(program.tables)
    # all_tables = list(set(*trace_tables, *export_tables))

    # === Optimise IR ==========================================================
    # Optimise joins.
    # TODO

    # Deduplicate IR expressions.
    # TODO.

    # After this point, the expression graph is frozen.  We no longer need to
    # update mappings.

    # === Generate Mappings ====================================================
    # Generate initial mappings for all reachable expression pairs.
    # TODO

    # Merge mappings between expressions that aren't in the roots list.
    # TODO

    mappings: list[Mapping] = []

    # === Compile Reachable Expressions to Command List ========================
    # Find reachable expressions.
    roots: set[ir.Expression] = set()
    roots.update(get_table_roots(trace_tables))
    roots.update(get_mapping_roots(mappings))
    roots.update(get_table_roots(export_tables))

    # Compile to command list.
    commands = compile_ir_to_cmd(roots)

    # === Inject Commands to Export Tables =====================================
    for table in export_tables:
        commands.append(
            cmd.ExportTableCommand(
                name=table.export_as,
                columns={
                    column.name: column.expression for column in table.columns
                },
            )
        )

    # === Setup Tracing ========================================================
    if tracer is not None:
        # Generate identifiers for arrays referenced by trace tables and
        # mappings and inject commands to export them.
        names: dict[ir.Expression, UUID] = {}
        for expression in get_table_roots(trace_tables) + get_mapping_roots(
            mappings
        ):
            assert isinstance(expression, ir.ArrayExpression)

            names[expression] = uuid4()
            # TODO these commands should be injected immediately after where the
            # array is defined.
            commands.append(
                cmd.TraceArrayCommand(
                    expression=expression,
                    uuid=names[expression],
                )
            )

        # Write trace manifest.
        manifest = create_manifest(
            source=source,
            snapshots=trace_tables,
            mappings=mappings,
            names=names,
        )

        tracer.write_manifest(manifest)

    # === Inject Commands to Collect Arrays After Use ==========================
    # TODO

    # === Evaluate the command list ===========================================
    context = Context(
        shapes={},
        arrays={},
        importer=importer,
        exporter=exporter,
        tracer=tracer,
    )
    for command in commands:
        eval_command(command, context=context)


def run_simple(
    source: str, /, *, inputs: Dict[str, pa.Table]
) -> Dict[str, pa.Table]:
    importer = InMemoryImporter(inputs)
    exporter = InMemoryExporter()

    run(source, importer=importer, exporter=exporter, tracer=None)

    return exporter.results()
