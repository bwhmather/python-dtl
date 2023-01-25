from pathlib import Path

import pytest

import dtl
import dtl.io

_undefined = object()


@pytest.fixture
def run_dtl(tmp_path):
    def _run_dtl(source, /, *, data, exporter=_undefined, tracer=_undefined):
        if isinstance(data, dtl.io.Importer):
            importer = data
        elif isinstance(data, dict):
            importer = dtl.io.InMemoryImporter(data)
        elif isinstance(data, Path):
            importer = dtl.io.FileSystemImporter(data)
        else:
            raise TypeError("Expected dict, path or importer")

        unpack = False
        if exporter is _undefined:
            exporter = dtl.io.InMemoryExporter()
            unpack = True

        if tracer is _undefined:
            tracer = dtl.io.FileSystemTracer(tmp_path / "trace")

        dtl.run(source, importer=importer, exporter=exporter, tracer=tracer)

        if unpack:
            return exporter.results()

    return _run_dtl
