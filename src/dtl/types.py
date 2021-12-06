from dataclasses import dataclass


@dataclass(frozen=True, order=True)
class Location:
    offset: int
    lineno: int
    column: int
