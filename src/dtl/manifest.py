import dataclasses
import functools
from uuid import UUID

from dtl.types import Location

Json = dict[str, "Json"] | list["Json"] | str | float | bool


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


@dataclasses.dataclass(frozen=True)
class IdentityMapping(Mapping):
    pass


@dataclasses.dataclass(frozen=True)
class ManyToOneMapping(Mapping):
    # Array mapping from rows in the source to indexes of rows in the target.
    tgt_index_array: UUID


@dataclasses.dataclass(frozen=True)
class OneToManyMapping(Mapping):
    # Array mapping from rows in the target to indexes of rows in the source.
    src_index_array: UUID


@dataclasses.dataclass(frozen=True)
class ManyToManyMapping(Mapping):
    # Pair of arrays forming a table that maps indexes in the source array into
    # indexes in the target array, and vice versa.
    src_index_array: UUID
    tgt_index_array: UUID


@dataclasses.dataclass(frozen=True)
class Manifest:
    source: str
    snapshots: list[Snapshot]
    mappings: list[Mapping]


def _location_to_json(location: Location) -> Json:
    return {
        "lineno": location.lineno,
        "column": location.column,
    }


def _column_to_json(column: Column) -> Json:
    return {
        "name": column.name,
        "array": str(column.array),
    }


def _snapshot_to_json(snapshot: Snapshot) -> Json:
    return {
        "start": _location_to_json(snapshot.start),
        "end": _location_to_json(snapshot.end),
        "columns": [_column_to_json(column) for column in snapshot.columns],
    }


@functools.singledispatch
def _mapping_to_json(mapping: Mapping) -> Json:
    raise NotImplementedError()


@_mapping_to_json.register(IdentityMapping)
def _identity_mapping_to_json(mapping: IdentityMapping) -> Json:
    return {
        "src_array": str(mapping.src_array),
        "tgt_array": str(mapping.tgt_array),
    }


@_mapping_to_json.register(ManyToOneMapping)
def _many_to_one_mapping_to_json(mapping: ManyToOneMapping) -> Json:
    return {
        "src_array": str(mapping.src_array),
        "tgt_array": str(mapping.tgt_array),
        "tgt_index_array": str(mapping.tgt_index_array),
    }


@_mapping_to_json.register(OneToManyMapping)
def _one_to_many_mapping_to_json(mapping: OneToManyMapping) -> Json:
    return {
        "src_array": str(mapping.src_array),
        "tgt_array": str(mapping.tgt_array),
        "src_index_array": str(mapping.src_index_array),
    }


@_mapping_to_json.register(ManyToManyMapping)
def _many_to_many_mapping_to_json(mapping: ManyToManyMapping) -> Json:
    return {
        "src_array": str(mapping.src_array),
        "tgt_array": str(mapping.tgt_array),
        "src_index_array": str(mapping.src_index_array),
        "tgt_index_array": str(mapping.tgt_index_array),
    }


def to_json(manifest: Manifest) -> Json:
    return {
        "source": manifest.source,
        "snapshots": [
            _snapshot_to_json(snapshot) for snapshot in manifest.snapshots
        ],
        "mappings": [
            _mapping_to_json(mapping) for mapping in manifest.mappings
        ],
    }
