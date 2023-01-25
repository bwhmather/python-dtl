import json
import pathlib
from typing import Optional
from uuid import UUID

import pyarrow as pa
import pyarrow.parquet as pq

import dtl.manifest


class Importer:
    def import_schema(self, name: str, /) -> pa.Schema:
        raise NotImplementedError()

    def import_table(self, name: str, /) -> pa.Table:
        raise NotImplementedError()


class InMemoryImporter(Importer):
    def __init__(self, tables: dict[str, pa.Table], /) -> None:
        self.__tables = dict(tables)

    def import_schema(self, name: str, /) -> pa.Schema:
        return self.__tables[name].schema

    def import_table(self, name: str, /) -> pa.Table:
        return self.__tables[name]


class FileSystemImporter(Importer):
    def __init__(self, root: pathlib.Path) -> None:
        self.__root = root
        self.__cache: dict[str, pa.Table] = {}

    def import_schema(self, name: str, /) -> pa.Schema:
        return self.import_table(name).schema

    def import_table(self, name: str, /) -> pa.Table:
        if name not in self.__cache:
            self.__cache[name] = pq.read_table(self.__root / f"{name}.parquet")
        return self.__cache[name]


class Exporter:
    def export_table(self, name: str, table: pa.Table, /) -> None:
        raise NotImplementedError()


class NoopExporter(Exporter):
    def export_table(self, name: str, table: pa.Table, /) -> None:
        pass


class InMemoryExporter(Exporter):
    def __init__(self) -> None:
        self.__tables: dict[str, pa.Table] = {}

    def export_table(self, name: str, table: pa.Table, /) -> None:
        assert name not in self.__tables
        self.__tables[name] = table

    def results(self) -> dict[str, pa.Table]:
        return dict(self.__tables)


class FileSystemExporter(Exporter):
    def __init__(self, root: pathlib.Path) -> None:
        self.__root = root

    def export_table(self, name: str, table: pa.Table, /) -> None:
        pq.write_table(table, self.__root / f"{name}.parquet")


class Tracer:
    def write_manifest(self, manifest: dtl.manifest.Manifest, /) -> None:
        raise NotImplementedError()

    def write_array(self, array_id: UUID, array: pa.Array, /) -> None:
        raise NotImplementedError()


class NoopTracer(Tracer):
    def write_manifest(self, manifest: dtl.manifest.Manifest, /) -> None:
        pass

    def write_array(self, array_id: UUID, array: pa.Array, /) -> None:
        pass


class InMemoryTracer(Tracer):
    def __init__(self) -> None:
        self.manifest: Optional[dtl.manifest.Manifest] = None
        self.arrays: dict[UUID, pa.Array] = {}

    def write_manifest(self, manifest: dtl.manifest.Manifest, /) -> None:
        assert self.manifest is None
        self.manifest = manifest

    def write_array(self, array_id: UUID, array: pa.Array, /) -> None:
        assert self.manifest is not None
        self.arrays[array_id] = array


class FileSystemTracer(Tracer):
    def __init__(self, root: pathlib.Path) -> None:
        self.__root = root

    def write_manifest(self, manifest: dtl.manifest.Manifest, /) -> None:
        self.__root.mkdir(parents=True, exist_ok=True)

        trace_path = self.__root / "trace.json"
        trace_path.write_text(
            json.dumps(dtl.manifest.to_json(manifest), indent=4)
        )

    def write_array(self, array_id: UUID, array: pa.Array, /) -> None:
        array_dir = self.__root / "arrays"
        array_dir.mkdir(parents=True, exist_ok=True)

        array_path = array_dir / f"{array_id}.parquet"
        pq.write_table(pa.table({"values": array}), array_path)
