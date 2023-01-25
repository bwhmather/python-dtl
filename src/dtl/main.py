import argparse
import sys
from pathlib import Path

import dtl
from dtl.io import FileSystemExporter, FileSystemImporter, FileSystemTracer


def main() -> None:
    parser = argparse.ArgumentParser(description="Run a DTL script")
    parser.add_argument(
        "script", nargs="?", type=argparse.FileType("r"), default=sys.stdin
    )

    parser.add_argument(
        "--input-dir",
        metavar="PATH",
        type=Path,
        help="directory containing parquet files to process",
    )
    parser.add_argument(
        "--output-dir",
        metavar="PATH",
        type=Path,
        help="directory into which result parquet files should be written",
    )
    parser.add_argument(
        "--trace-dir",
        metavar="PATH",
        type=Path,
        default=None,
        help="directory into which trace should be written",
    )

    args = parser.parse_args()

    importer = FileSystemImporter(args.input_dir)
    exporter = FileSystemExporter(args.output_dir)

    tracer = None
    if args.trace_dir is not None:
        tracer = FileSystemTracer(args.trace_dir)

    source = args.script.read()

    dtl.run(source, importer=importer, exporter=exporter, tracer=tracer)
