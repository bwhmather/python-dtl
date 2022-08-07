import dataclasses
from functools import singledispatch
from uuid import UUID

from dtl import ir


@dataclasses.dataclass(frozen=True, eq=False)
class Command:
    pass


@dataclasses.dataclass(frozen=True, eq=False)
class EvaluateArrayCommand(Command):
    expression: ir.ArrayExpression


@dataclasses.dataclass(frozen=True, eq=False)
class EvaluateShapeCommand(Command):
    expression: ir.ShapeExpression


@dataclasses.dataclass(frozen=True, eq=False)
class CollectArrayCommand(Command):
    expression: ir.ArrayExpression


@dataclasses.dataclass(frozen=True, eq=False)
class TraceArrayCommand(Command):
    expression: ir.ArrayExpression
    uuid: UUID


@dataclasses.dataclass(frozen=True, eq=False)
class ExportTableCommand(Command):
    name: str
    columns: dict[str, ir.ArrayExpression]


@singledispatch
def provides(command: Command) -> set[ir.Expression]:
    return set()


@provides.register(EvaluateArrayCommand)
def _get_evaluate_array_command_provides(
    command: EvaluateArrayCommand,
) -> set[ir.Expression]:
    return {command.expression}


@provides.register(EvaluateShapeCommand)
def _get_evaluate_shape_command_provides(
    command: EvaluateShapeCommand,
) -> set[ir.Expression]:
    return {command.expression}


@singledispatch
def dependencies(command: Command) -> set[ir.Expression]:
    raise NotImplementedError()


@dependencies.register(EvaluateArrayCommand)
def _get_evaluate_array_command_dependencies(
    command: EvaluateArrayCommand,
) -> set[ir.Expression]:
    return set(ir.dependencies(command.expression))


@dependencies.register(EvaluateShapeCommand)
def _get_evaluate_shape_command_dependencies(
    command: EvaluateShapeCommand,
) -> set[ir.Expression]:
    return set(ir.dependencies(command.expression))


@dependencies.register(CollectArrayCommand)
def _get_collect_array_command_dependencies(
    command: CollectArrayCommand,
) -> set[ir.Expression]:
    return {command.expression}


@dependencies.register(TraceArrayCommand)
def _get_trace_array_command_dependencies(
    command: TraceArrayCommand,
) -> set[ir.Expression]:
    return {command.expression}


@dependencies.register(ExportTableCommand)
def _get_export_table_dependencies(
    command: ExportTableCommand,
) -> set[ir.Expression]:
    return set(command.columns.values())
