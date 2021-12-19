import functools

from dtl import lexer
from dtl import nodes as n
from dtl import parser
from dtl.types import Location


def _parse_statement(string):
    tokens = lexer.tokenize(string)
    statement_list = parser.parse(tokens)

    assert isinstance(statement_list, n.StatementList)
    assert len(statement_list.statements) == 1

    return statement_list.statements[0]


class L:
    def __eq__(self, other):
        if isinstance(other, Location):
            return True
        return NotImplemented


class NodeModuleWrapper:
    def __getattr__(self, name):
        node_cls = getattr(n, name)

        @functools.wraps(node_cls)
        def wrapper(**kwargs):
            kwargs.setdefault("start", L())
            kwargs.setdefault("end", L())
            return node_cls(**kwargs)

        return wrapper


nw = NodeModuleWrapper()


def test_assign_select():
    statement = _parse_statement(
        "WITH variable AS SELECT column_a, column_b FROM table"
    )

    assert statement == nw.AssignmentStatement(
        target=nw.TableName(name="variable", end=L()),
        expression=nw.SelectExpression(
            distinct=None,
            columns=[
                nw.ColumnBinding(
                    expression=nw.ColumnRefExpr(
                        name=nw.UnqualifiedColumnName(
                            column_name="column_a",
                        ),
                    ),
                    alias=None,
                ),
                nw.ColumnBinding(
                    expression=nw.ColumnRefExpr(
                        name=nw.UnqualifiedColumnName(
                            column_name="column_b",
                        ),
                    ),
                    alias=None,
                ),
            ],
            source=nw.FromClause(
                source=nw.TableBinding(
                    expression=nw.TableRefExpr(
                        name=nw.TableName(
                            name="table",
                        ),
                    ),
                    alias=None,
                ),
            ),
            join=[],
            where=None,
            group_by=None,
        ),
    )
