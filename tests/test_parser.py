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


def test_select_one_column():
    statement = _parse_statement("SELECT column FROM table")

    assert statement == n.ExpressionStatement(
        expression=n.SelectExpression(
            distinct=None,
            columns=[
                n.ColumnBinding(
                    expression=n.ColumnRefExpr(
                        name=n.UnqualifiedColumnName(
                            column_name="column",
                            start=L(),
                            end=L(),
                        ),
                        start=L(),
                        end=L(),
                    ),
                    alias=None,
                    start=L(),
                    end=L(),
                ),
            ],
            source=n.FromClause(
                source=n.TableBinding(
                    expression=n.TableRefExpr(
                        name=n.TableName(
                            name="table",
                            start=L(),
                            end=L(),
                        ),
                        start=L(),
                        end=L(),
                    ),
                    alias=None,
                    start=L(),
                    end=L(),
                ),
                start=L(),
                end=L(),
            ),
            join=[],
            where=None,
            group_by=None,
            start=L(),
            end=L(),
        ),
        start=L(),
        end=L(),
    )
