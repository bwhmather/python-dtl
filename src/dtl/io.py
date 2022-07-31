import pathlib
from typing import Any
from uuid import UUID

import pyarrow as pa
import pyarrow.parquet as pq

from dtl.types import Location


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
        self.__cache = {}

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
    def __init__(self):
        self.__tables = {}

    def export_table(self, name: str, table: pa.Table, /) -> None:
        assert name not in self.__tables
        self.__tables[name] = table

    def results(self):
        return dict(self.__tables)


class FilesystemExporter(Exporter):
    def __init__(self, root: pathlib.Path) -> None:
        self.__root = root

    def export_table(self, name: str, table: pa.Table, /) -> None:
        pq.write_table(table, self.__root / f"{name}.parquet")
