from typing import Iterator, Optional, Type

import dtl.tokens as t

_KEYWORD_TOKEN_CLASSES = {
    "BEGIN": t.Begin,
    "UPDATE": t.Update,
    "SELECT": t.Select,
    "DISTINCT": t.Distinct,
    "CONSECUTIVE": t.Consecutive,
    "AS": t.As,
    "FROM": t.From,
    "JOIN": t.Join,
    "ON": t.On,
    "WHERE": t.Where,
    "WITH": t.With,
    "IMPORT": t.Import,
    "EXPORT": t.Export,
    "TO": t.To,
    "TRUE": t.True_,
    "FALSE": t.False_,
}


def _is_whitespace(c: Optional[str]) -> bool:
    return c in (" ", "\n", "\t")


def _is_id_start(c: Optional[str]) -> bool:
    if c is None:
        return False
    if "a" <= c <= "z":
        return True
    if "A" <= c <= "Z":
        return True
    if c == "_":
        return True
    return False


def _is_id_continue(c: Optional[str]) -> bool:
    if c is None:
        return False
    if "a" <= c <= "z":
        return True
    if "A" <= c <= "Z":
        return True
    if "0" <= c <= "9":
        return True
    if c == "_":
        return True
    if c == "-":
        return True
    return False


class _Tokenizer:
    def __init__(self, string: str):
        self._string = string
        self._cursor = 0
        self._lineno = 0
        self._column = 0

    def _peek(self) -> Optional[str]:
        if self._cursor >= len(self._string):
            return None
        return self._string[self._cursor]

    def _bump(self) -> str:
        assert self._cursor < len(self._string)
        if self._string[self._cursor] == "\n":
            self._lineno += 1
            self._column = 0
        self._cursor += 1
        self._column += 1
        return self._string[self._cursor - 1]

    def _next_class(self) -> Type[t.Token]:
        curr = self._bump()

        if _is_whitespace(curr):
            while _is_whitespace(self._peek()):
                self._bump()
            return t.Whitespace

        if curr == "/":
            if self._peek() == "/":
                self._bump()

                # Consume everything up to the end of the line.
                while self._peek() not in ("\n", None):
                    self._bump()

                return t.LineComment

            elif self._peek() == "*":
                while True:
                    curr = self._bump()
                    if curr is None:
                        return t.Unknown

                    if curr == "*" and self._peek() == "/":
                        self._bump()
                        return t.BlockComment

            else:
                return t.Slash

        if curr == '"':
            while True:
                curr = self._bump()
                if curr == "\\":
                    self._bump()
                if curr == '"':
                    return t.QuotedName

        if curr == "'":
            while True:
                curr = self._bump()
                if curr == "\\":
                    self._bump()
                if curr == "'":
                    return t.String

        if "0" <= curr <= "9":
            # TODO octal/hex
            while True:
                n = self._peek()
                if n is None:
                    break
                if "0" > n or n > "9":
                    break
                self._bump()

            if self._peek() == ".":
                self._bump()
                while True:
                    n = self._peek()
                    if n is None:
                        break
                    if "0" > n or n > "9":
                        break
                    self._bump()
                return t.Float

            else:
                return t.Integer

        if _is_id_start(curr):
            token = curr

            while _is_id_continue(self._peek()):
                token += self._bump()

            if token in _KEYWORD_TOKEN_CLASSES:
                return _KEYWORD_TOKEN_CLASSES[token]

            if token[0].isupper():
                return t.Type

            return t.Name

        if curr == ";":
            return t.Semicolon

        if curr == ",":
            return t.Comma

        if curr == ".":
            return t.Dot

        if curr == "(":
            return t.OpenParen

        if curr == ")":
            return t.CloseParen

        if curr == "{":
            return t.OpenBrace

        if curr == "}":
            return t.CloseBrace

        if curr == "[":
            return t.OpenBracket

        if curr == "]":
            return t.CloseBracket

        if curr == "@":
            return t.At

        if curr == "#":
            return t.Pound

        if curr == "~":
            return t.Tilde

        if curr == "?":
            return t.Question

        if curr == ":":
            return t.Colon

        if curr == "$":
            return t.Dollar

        if curr == "=":
            return t.Eq

        if curr == "!":
            if self._peek() == "=":
                self._bump()
                return t.NotEqual
            return t.Not

        if curr == "<":
            if self._peek() == "=":
                self._bump()
                return t.LessThanEqual
            return t.LessThan

        if curr == ">":
            if self._peek() == "=":
                self._bump()
                return t.GreaterThanEqual
            return t.GreaterThan

        if curr == "-":
            if self._peek() == "=":
                self._bump()
                return t.MinusEqual
            return t.Minus

        if curr == "&":
            return t.And

        if curr == "|":
            return t.Or

        if curr == "+":
            if self._peek() == "+":
                self._bump()
                return t.PlusEqual
            return t.Plus

        if curr == "*":
            if self._peek() == "=":
                self._bump()
                return t.StarEqual
            return t.Star

        if curr == "/":
            if self._peek() == "=":
                self._bump()
                return t.SlashEqual
            return t.Slash

        if curr == "^":
            return t.Caret

        if curr == "%":
            return t.Percent

        return t.Unknown

    def next_token(self) -> t.Token:
        start = t.Location(
            offset=self._cursor, lineno=self._lineno, column=self._column
        )

        token_class = self._next_class()

        end = t.Location(
            offset=self._cursor, lineno=self._lineno, column=self._column
        )

        return token_class(
            text=self._string[start.offset : end.offset], start=start, end=end
        )

    def is_eof(self) -> bool:
        return self._cursor >= len(self._string)


def tokenize(string: str) -> Iterator[t.Token]:
    tokenizer = _Tokenizer(string)

    while not tokenizer.is_eof():
        yield tokenizer.next_token()
