from pathlib import Path

import dtl
import dtl.io


def run(source, /, *, tracer=None):
    importer = dtl.io.FileSystemImporter(Path("tests/data/faa/"))
    exporter = dtl.io.InMemoryExporter()

    dtl.run(source, importer=importer, exporter=exporter, tracer=tracer)


def test_import_aircraft():
    run(
        """
        WITH aircraft AS IMPORT 'aircraft';
        """
    )


def test_import_aircraft_models():
    run(
        """
        WITH aircraft_models AS IMPORT 'aircraft_models';
        """
    )


def test_import_airports():
    run(
        """
        WITH airports AS IMPORT 'airports';
        """
    )


def test_import_carriers():
    run(
        """
        WITH carriers AS IMPORT 'carriers';
        """
    )


def test_import_flights():
    run(
        """
        WITH flights AS IMPORT 'flights';
        """
    )
