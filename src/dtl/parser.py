from typing import List

import dtl.nodes as n
import dtl.tokens as t
from dtl.parser_generator import ParserGenerator

_generator = ParserGenerator(node_type=n.Node, token_type=t.Token)


def _reduce_ident(token: t.Name) -> str:
    return token.text


_generator.register(n.BeginStatement, [t.Begin, t.String])
_generator.register(n.SelectStatement, [t.Select, t.Name, t.From, t.Name])
_generator.register(n.StatementList, [List[n.Statement]])
_parser = _generator.parser(target=n.StatementList)


def _filter_tokens(tokens):
    for token in tokens:
        if isinstance(token, t.LineComment):
            continue

        if isinstance(token, t.BlockComment):
            continue

        if isinstance(token, t.Whitespace):
            continue

        yield token


def parse(tokens):
    return _parser.parse(_filter_tokens(tokens))
