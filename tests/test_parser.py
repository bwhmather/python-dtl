from dtl import lexer
from dtl import nodes as n
from dtl import parser


def _parse_statement(string):
    tokens = lexer.tokenize(string)
    statement_list = parser.parse(tokens)

    assert isinstance(statement_list, n.StatementList)
    assert len(statement_list.statements) == 1

    return statement_list.statements[0]


def test_select_one_column():
    statement = _parse_statement("SELECT column FROM table")

    assert statement == n.ExpressionStatement(
        expression=n.SelectExpression(
            distinct=None,
            columns=[
                n.ColumnBinding(
                    expression=n.ColumnRefExpr(
                        name=n.UnqualifiedColumnName("column")
                    ),
                    alias=None,
                )
            ],
            source=n.FromClause(
                n.TableBinding(
                    expression=n.TableRefExpr(name=n.TableName("table")),
                    alias=None,
                )
            ),
            join=[],
            where=None,
            group_by=None,
        )
    )
