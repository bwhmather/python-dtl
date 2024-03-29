from dataclasses import dataclass

from dtl.types import Location


@dataclass(frozen=True)
class Token:
    text: str
    start: Location
    end: Location

    def __repr__(self) -> str:
        return f"t.{type(self).__name__}({self.text!r})"


# --- Literals ---


class Integer(Token):
    pass


class Float(Token):
    pass


class Char(Token):
    pass


class Byte(Token):
    pass


class String(Token):
    pass


class ByteString(Token):
    pass


class RawString(Token):
    pass


class RawByteString(Token):
    pass


# --- Keywords ---


class Begin(Token):
    pass


class Update(Token):
    pass


class Select(Token):
    pass


class Distinct(Token):
    pass


class Consecutive(Token):
    pass


class As(Token):
    pass


class From(Token):
    pass


class Join(Token):
    pass


class On(Token):
    pass


class Where(Token):
    pass


class Group(Token):
    pass


class By(Token):
    pass


class With(Token):
    pass


class Import(Token):
    pass


class Export(Token):
    pass


class To(Token):
    pass


class Using(Token):
    pass


class True_(Token):
    pass


class False_(Token):
    pass


# --- Symbols ---


class Semicolon(Token):
    pass


class Comma(Token):
    pass


class Dot(Token):
    pass


class OpenParen(Token):
    pass


class CloseParen(Token):
    pass


class OpenBrace(Token):
    pass


class CloseBrace(Token):
    pass


class OpenBracket(Token):
    pass


class CloseBracket(Token):
    pass


class At(Token):
    pass


class Pound(Token):
    pass


class Tilde(Token):
    pass


class Question(Token):
    pass


class Colon(Token):
    pass


class Dollar(Token):
    pass


class Eq(Token):
    pass


class Not(Token):
    pass


class NotEqual(Token):
    pass


class LessThan(Token):
    pass


class LessThanEqual(Token):
    pass


class GreaterThan(Token):
    pass


class GreaterThanEqual(Token):
    pass


class Minus(Token):
    pass


class MinusEqual(Token):
    pass


class And(Token):
    pass


class Or(Token):
    pass


class Plus(Token):
    pass


class PlusEqual(Token):
    pass


class Star(Token):
    pass


class StarEqual(Token):
    pass


class Slash(Token):
    pass


class SlashEqual(Token):
    pass


class Caret(Token):
    pass


class Percent(Token):
    pass


# --- Identifiers ---


class Type(Token):
    pass


class Name(Token):
    pass


class QuotedName(Token):
    pass


# --- Special ---


class Unknown(Token):
    pass


# --- Blanks ---


class LineComment(Token):
    pass


class BlockComment(Token):
    pass


class Whitespace(Token):
    pass


__all__ = [
    "LineComment",
    "BlockComment",
    "Whitespace",
    "Integer",
    "Float",
    "Char",
    "Byte",
    "String",
    "ByteString",
    "RawString",
    "RawByteString",
    "Update",
    "Select",
    "Distinct",
    "As",
    "Join",
    "On",
    "Where",
    "True_",
    "False_",
    "Semicolon",
    "Comma",
    "Dot",
    "OpenParen",
    "CloseParen",
    "OpenBrace",
    "CloseBrace",
    "OpenBracket",
    "CloseBracket",
    "At",
    "Pound",
    "Tilde",
    "Question",
    "Colon",
    "Dollar",
    "Eq",
    "Not",
    "NotEqual",
    "LessThan",
    "LessThanEqual",
    "GreaterThan",
    "GreaterThanEqual",
    "Minus",
    "MinusEqual",
    "And",
    "Or",
    "Plus",
    "PlusEqual",
    "Star",
    "StarEqual",
    "Slash",
    "SlashEqual",
    "Caret",
    "Percent",
    "Type",
    "Name",
    "Unknown",
]
