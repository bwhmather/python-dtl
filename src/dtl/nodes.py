from dataclasses import dataclass
from typing import List

import dtl.tokens as t


@dataclass(frozen=True)
class Node:
    pass


@dataclass(frozen=True)
class Statement(Node):
    pass


@dataclass(frozen=True)
class SelectStatement(Statement):
    pass


@dataclass(frozen=True)
class BeginStatement(Statement):
    text: t.String


@dataclass(frozen=True)
class StatementList(Node):
    statements: List[Statement]
