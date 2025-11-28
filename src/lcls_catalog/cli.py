"""Command-line interface for LCLS data catalog."""

import argparse
import os
import sys

from .catalog import Catalog


def parse_size(size_str: str) -> int:
    """Parse a human-readable size string (e.g., '1GB', '500MB') to bytes."""
    size_str = size_str.strip().upper()
    units = {"B": 1, "KB": 1024, "MB": 1024**2, "GB": 1024**3, "TB": 1024**4}

    for unit, multiplier in units.items():
        if size_str.endswith(unit):
            number = float(size_str[: -len(unit)])
            return int(number * multiplier)

    return int(size_str)


def get_catalog(path: str, format: str = None):
    """Get the appropriate catalog class based on format or path."""
    # Auto-detect format if not specified
    if format is None:
        if os.path.isdir(path) or not path.endswith(".db"):
            format = "parquet"
        else:
            format = "sqlite"

    if format == "parquet":
        try:
            from .parquet_catalog import ParquetCatalog
            return ParquetCatalog(path)
        except ImportError:
            print("Error: Parquet support requires 'pyarrow' and 'duckdb'.")
            print("Install with: pip install lcls-catalog[parquet]")
            sys.exit(1)
    else:
        return Catalog(path)


def cmd_snapshot(args):
    """Handle the snapshot command."""
    if args.format == "parquet":
        try:
            from .parquet_catalog import ParquetCatalog
        except ImportError:
            print("Error: Parquet support requires 'pyarrow' and 'duckdb'.")
            print("Install with: pip install lcls-catalog[parquet]")
            sys.exit(1)

        with ParquetCatalog(args.output) as cat:
            count = cat.snapshot(
                args.path,
                experiment=args.experiment,
                compute_checksum=args.checksum,
                purge_date=args.purge_date,
                workers=args.workers,
                batch_size=args.batch_size,
            )
            print(f"Cataloged {count} files to {args.output}/ (parquet, {args.workers} workers)")
    else:
        with Catalog(args.output) as cat:
            count = cat.snapshot(
                args.path,
                experiment=args.experiment,
                compute_checksum=args.checksum,
                purge_date=args.purge_date,
            )
            print(f"Cataloged {count} files to {args.output}")


def cmd_ls(args):
    """Handle the ls command."""
    with get_catalog(args.db) as cat:
        if args.dirs:
            dirs = cat.ls_dirs(args.path)
            if not dirs:
                print(f"No subdirectories found under {args.path}")
                return
            for d in dirs:
                print(f"{d.dirname:40} {d.file_count:>8} files  {d.size_human:>10}")
        else:
            files = cat.ls(args.path)
            if not files:
                print(f"No files found in {args.path}")
                return
            for f in files:
                print(f"{f.filename:40} {f.size_human:>15}")


def cmd_find(args):
    """Handle the find command."""
    size_gt = parse_size(args.size_gt) if args.size_gt else None
    size_lt = parse_size(args.size_lt) if args.size_lt else None

    with get_catalog(args.db) as cat:
        results = cat.find(
            args.pattern,
            size_gt=size_gt,
            size_lt=size_lt,
            experiment=args.experiment,
        )
        if not results:
            print(f"No files matching '{args.pattern}'")
            return
        for f in results:
            print(f"{f.path:60} {f.size_human:>15}")


def cmd_tree(args):
    """Handle the tree command."""
    with get_catalog(args.db) as cat:
        output = cat.tree(args.path, depth=args.depth)
        print(output)


def cmd_stats(args):
    """Handle the stats command."""
    with get_catalog(args.db) as cat:
        count = cat.count()
        total = cat.total_size()

        # Format total size
        size = total
        for unit in ["B", "KB", "MB", "GB", "TB"]:
            if abs(size) < 1024:
                size_str = f"{size:.1f} {unit}"
                break
            size /= 1024
        else:
            size_str = f"{size:.1f} PB"

        print(f"Total files: {count:,}")
        print(f"Total size:  {size_str}")


def main():
    """Main entry point for the CLI."""
    parser = argparse.ArgumentParser(
        prog="lcls-catalog",
        description="LCLS Data Catalog - Browse and search purged file metadata",
    )
    subparsers = parser.add_subparsers(dest="command", required=True)

    # snapshot command
    snapshot_parser = subparsers.add_parser(
        "snapshot", help="Capture metadata from a directory tree"
    )
    snapshot_parser.add_argument("path", help="Directory to snapshot")
    snapshot_parser.add_argument(
        "-o", "--output", default="catalog.db", help="Output database file or directory"
    )
    snapshot_parser.add_argument(
        "-e", "--experiment", help="Experiment identifier"
    )
    snapshot_parser.add_argument(
        "--checksum", action="store_true", help="Compute SHA-256 checksums"
    )
    snapshot_parser.add_argument(
        "--purge-date", help="Purge date (YYYY-MM-DD), defaults to today"
    )
    snapshot_parser.add_argument(
        "--format", choices=["sqlite", "parquet"], default="sqlite",
        help="Storage format (default: sqlite)"
    )
    snapshot_parser.add_argument(
        "--workers", type=int, default=1,
        help="Number of parallel workers (parquet only, default: 1)"
    )
    snapshot_parser.add_argument(
        "--batch-size", type=int, default=10000,
        help="Files per batch for streaming writes (parquet only, default: 10000)"
    )
    snapshot_parser.set_defaults(func=cmd_snapshot)

    # ls command
    ls_parser = subparsers.add_parser("ls", help="List files in a directory")
    ls_parser.add_argument("db", help="Catalog database file or directory")
    ls_parser.add_argument("path", help="Directory path to list")
    ls_parser.add_argument(
        "-d", "--dirs", action="store_true", help="List subdirectories with stats"
    )
    ls_parser.set_defaults(func=cmd_ls)

    # find command
    find_parser = subparsers.add_parser("find", help="Search for files by pattern")
    find_parser.add_argument("db", help="Catalog database file or directory")
    find_parser.add_argument("pattern", help="Filename pattern (SQL LIKE syntax, use %% as wildcard)")
    find_parser.add_argument("--size-gt", help="Minimum size (e.g., 1GB, 500MB)")
    find_parser.add_argument("--size-lt", help="Maximum size (e.g., 1GB, 500MB)")
    find_parser.add_argument("-e", "--experiment", help="Filter by experiment")
    find_parser.set_defaults(func=cmd_find)

    # tree command
    tree_parser = subparsers.add_parser("tree", help="Show directory tree")
    tree_parser.add_argument("db", help="Catalog database file or directory")
    tree_parser.add_argument("path", help="Root path for tree")
    tree_parser.add_argument(
        "--depth", type=int, default=2, help="Maximum depth (default: 2)"
    )
    tree_parser.set_defaults(func=cmd_tree)

    # stats command
    stats_parser = subparsers.add_parser("stats", help="Show catalog statistics")
    stats_parser.add_argument("db", help="Catalog database file or directory")
    stats_parser.set_defaults(func=cmd_stats)

    args = parser.parse_args()
    args.func(args)


if __name__ == "__main__":
    main()
