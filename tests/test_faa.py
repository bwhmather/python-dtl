from pathlib import Path

import dtl
import dtl.io


def test_import_aircraft(run_dtl):
    run_dtl(
        """
        WITH aircraft AS IMPORT 'aircraft';
        """,
        data=Path("tests/data/faa/"),
    )


def test_import_aircraft_models(run_dtl):
    run_dtl(
        """
        WITH aircraft_models AS IMPORT 'aircraft_models';
        """,
        data=Path("tests/data/faa/"),
    )


def test_import_airports(run_dtl):
    run_dtl(
        """
        WITH airports AS IMPORT 'airports';
        """,
        data=Path("tests/data/faa/"),
    )


def test_import_carriers(run_dtl):
    run_dtl(
        """
        WITH carriers AS IMPORT 'carriers';
        """,
        data=Path("tests/data/faa/"),
    )


def test_import_flights(run_dtl):
    run_dtl(
        """
        WITH flights AS IMPORT 'flights';
        """,
        data=Path("tests/data/faa/"),
    )
