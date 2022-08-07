from dtl import cmd, ir


def compile_ir_to_cmd(roots: set[ir.Expression]) -> list[cmd.Command]:
    command_list: list[cmd.Command] = []
    for expression in ir.traverse_depth_first(roots):
        if isinstance(expression, ir.ShapeExpression):
            command_list.append(
                cmd.EvaluateShapeCommand(expression=expression)
            )
        elif isinstance(expression, ir.ArrayExpression):
            command_list.append(
                cmd.EvaluateArrayCommand(expression=expression)
            )
        else:
            raise AssertionError()
    return command_list
