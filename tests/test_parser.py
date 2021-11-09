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
            columns=[n.UnqualifiedColumnName("column")],
            source=n.FromClause(n.TableName("table")),
            join=[],
            where=None,
            group_by=None,
        )
    )
