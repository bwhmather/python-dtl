from __future__ import annotations

from dataclasses import dataclass
from functools import singledispatch
from typing import Dict

import pyarrow as pa

from dtl import ir as ir
from dtl.ast_to_ir import compile_ast_to_ir
from dtl.lexer import tokenize
from dtl.parser import parse


@dataclass(frozen=True)
class Context:
    results: Dict[ir.Expression, pa.Array]
    inputs: Dict[str, pa.Table]


@singledispatch
def eval_expression(expression: ir.Expression, context: Context) -> pa.Array:
    raise NotImplementedError()


@eval_expression.register(ir.Import)
def eval_import_expression(
    expression: ir.Import, context: Context
) -> pa.Array:
    return context.inputs[expression.location][expression.name]


@eval_expression.register(ir.Where)
def eval_where_expression(expression: ir.Where, context: Context) -> pa.Array:
    raise NotImplementedError


def evaluate(source: str, inputs: Dict[str, pa.Table]) -> Dict[str, pa.Table]:
    ast = parse(tokenize(source))

    program = compile_ast_to_ir(
        ast,
        input_types={
            name: list(table.column_names) for name, table in inputs.items()
        },
    )

    context = Context(results={}, inputs=inputs)
    for expression in program.expressions:
        context.results[expression] = eval_expression(
            expression, context=context
        )

    print(program.expressions)
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
