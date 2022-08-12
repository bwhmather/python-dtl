import dataclasses
from uuid import UUID

from dtl.types import Location


@dataclasses.dataclass(frozen=True)
class Column:
    name: str
    array: UUID


@dataclasses.dataclass(frozen=True)
class Snapshot:
    start: Location
    end: Location
    columns: list[Column]


@dataclasses.dataclass(frozen=True)
class Mapping:
    # The identifiers of the two arrays that this mapping connects.
    src_array: UUID
    tgt_array: UUID

    # TODO more efficient representations for common cases.
    # Pair of arrays forming a table that maps indexes in the source array into
    # indexes in the target array.
    src_index_array: UUID
    tgt_index_array: UUID


@dataclasses.dataclass(frozen=True)
class Manifest:
    source: str
    snapshots: list[Snapshot]
    mappings: list[Mapping]


def _location_to_json(location: Location) -> dict:
    return {
        "lineno": location.lineno,
        "column": location.column,
    }


def _column_to_json(column: Column) -> dict:
    return {
        "name": column.name,
        "array": str(column.array),
    }


def _snapshot_to_json(snapshot: Snapshot) -> dict:
    return {
        "start": _location_to_json(snapshot.start),
        "end": _location_to_json(snapshot.end),
        "columns": [_column_to_json(column) for column in snapshot.columns],
    }


def _mapping_to_json(mapping: Mapping) -> dict:
    return {
        "src_array": str(mapping.src_array),
        "tgt_array": str(mapping.tgt_array),
        "src_index_array": str(mapping.src_index_array),
        "tgt_index_array": str(mapping.tgt_index_array),
    }


def to_json(manifest: Manifest) -> dict:
    return {
        "source": manifest.source,
        "snapshots": [
            _snapshot_to_json(snapshot) for snapshot in manifest.snapshots
        ],
        "mappings": [
            _mapping_to_json(mapping) for mapping in manifest.mappings
        ],
    }
