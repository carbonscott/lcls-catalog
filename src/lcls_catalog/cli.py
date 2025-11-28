"""Command-line interface for LCLS data catalog."""

import argparse
import sys

from .parquet_catalog import ParquetCatalog


def parse_size(size_str: str) -> int:
    """Parse a human-readable size string (e.g., '1GB', '500MB') to bytes."""
    size_str = size_str.strip().upper()
    units = {"B": 1, "KB": 1024, "MB": 1024**2, "GB": 1024**3, "TB": 1024**4}

    for unit, multiplier in units.items():
        if size_str.endswith(unit):
            number = float(size_str[: -len(unit)])
            return int(number * multiplier)

    return int(size_str)


def cmd_snapshot(args):
    """Handle the snapshot command."""
    with ParquetCatalog(args.output) as cat:
        added, modified, removed = cat.snapshot(
            args.path,
            experiment=args.experiment,
            compute_checksum=args.checksum,
            workers=args.workers,
            batch_size=args.batch_size,
        )

        if added == 0 and modified == 0 and removed == 0:
            print(f"No changes detected in {args.path}")
        else:
            parts = []
            if added > 0:
                parts.append(f"{added} added")
            if modified > 0:
                parts.append(f"{modified} modified")
            if removed > 0:
                parts.append(f"{removed} removed")
            print(f"Cataloged {args.path}: {', '.join(parts)}")


def cmd_ls(args):
    """Handle the ls command."""
    with ParquetCatalog(args.db) as cat:
        if args.dirs:
            dirs = cat.ls_dirs(args.path, on_disk_only=args.on_disk)
            if not dirs:
                print(f"No subdirectories found under {args.path}")
                return
            for d in dirs:
                print(f"{d.dirname:40} {d.file_count:>8} files  {d.size_human:>10}")
        else:
            files = cat.ls(args.path, on_disk_only=args.on_disk)
            if not files:
                print(f"No files found in {args.path}")
                return
            for f in files:
                print(f"{f.filename:40} {f.size_human:>15}")


def cmd_find(args):
    """Handle the find command."""
    size_gt = parse_size(args.size_gt) if args.size_gt else None
    size_lt = parse_size(args.size_lt) if args.size_lt else None

    with ParquetCatalog(args.db) as cat:
        results = cat.find(
            args.pattern,
            size_gt=size_gt,
            size_lt=size_lt,
            experiment=args.experiment,
            exclude=args.exclude if args.exclude else None,
            on_disk_only=args.on_disk,
            removed_only=args.removed,
        )
        if not results:
            print(f"No files matching '{args.pattern}'")
            return
        for f in results:
            status = "" if not args.show_status else (" [removed]" if hasattr(f, 'on_disk') and not f.on_disk else "")
            print(f"{f.path:60} {f.size_human:>15}{status}")


def cmd_tree(args):
    """Handle the tree command."""
    with ParquetCatalog(args.db) as cat:
        output = cat.tree(args.path, depth=args.depth)
        print(output)


def cmd_stats(args):
    """Handle the stats command."""
    with ParquetCatalog(args.db) as cat:
        total_count = cat.count()
        on_disk_count = cat.count(on_disk_only=True)
        total_size = cat.total_size()
        on_disk_size = cat.total_size(on_disk_only=True)

        # Format sizes
        def format_size(size):
            for unit in ["B", "KB", "MB", "GB", "TB"]:
                if abs(size) < 1024:
                    return f"{size:.1f} {unit}"
                size /= 1024
            return f"{size:.1f} PB"

        print(f"Total files indexed:  {total_count:,}")
        print(f"  Currently on disk:  {on_disk_count:,}")
        print(f"  Removed:            {total_count - on_disk_count:,}")
        print(f"Total size indexed:   {format_size(total_size)}")
        print(f"  Currently on disk:  {format_size(on_disk_size)}")


def cmd_query(args):
    """Handle the query command."""
    with ParquetCatalog(args.db) as cat:
        rows = cat.query(args.sql)
        if not rows:
            print("No results")
            return
        for row in rows:
            print("\t".join(str(x) if x is not None else "" for x in row))


def cmd_consolidate(args):
    """Handle the consolidate command."""
    with ParquetCatalog(args.db) as cat:
        stats = cat.consolidate(archive_dir=args.archive)

        if stats["experiments"] == 0:
            print("Nothing to consolidate (no experiments with multiple files)")
        else:
            print(f"Consolidated {stats['experiments']} experiments")
            if args.archive:
                print(f"  Archived {stats['files_archived']} old files to {args.archive}")
            else:
                print(f"  Removed {stats['files_removed']} old files")


def cmd_snapshots(args):
    """Handle the snapshots command."""
    with ParquetCatalog(args.db) as cat:
        snapshots = cat.list_snapshots(exp_hash=args.experiment)

        if not snapshots:
            print("No snapshots found")
            return

        # Group by experiment
        by_exp = {}
        for s in snapshots:
            exp = s["experiment"]
            if exp not in by_exp:
                by_exp[exp] = []
            by_exp[exp].append(s)

        for exp, snaps in sorted(by_exp.items()):
            print(f"\n{exp}/")
            for s in snaps:
                size_kb = s["size_bytes"] / 1024
                print(f"  {s['type']:5} {s['timestamp']}  {s['record_count']:>8} records  {size_kb:>10.1f} KB")


def main():
    """Main entry point for the CLI."""
    parser = argparse.ArgumentParser(
        prog="lcls-catalog",
        description="LCLS Data Catalog - Persistent file metadata tracking with incremental updates",
    )
    subparsers = parser.add_subparsers(dest="command", required=True)

    # snapshot command
    snapshot_parser = subparsers.add_parser(
        "snapshot", help="Capture metadata from a directory tree (creates base or delta)"
    )
    snapshot_parser.add_argument("path", help="Directory to snapshot")
    snapshot_parser.add_argument(
        "-o", "--output", default="catalog", help="Output catalog directory"
    )
    snapshot_parser.add_argument(
        "-e", "--experiment", help="Experiment identifier"
    )
    snapshot_parser.add_argument(
        "--checksum", action="store_true", help="Compute SHA-256 checksums"
    )
    snapshot_parser.add_argument(
        "--workers", type=int, default=1,
        help="Number of parallel workers (default: 1)"
    )
    snapshot_parser.add_argument(
        "--batch-size", type=int, default=10000,
        help="Files per batch for processing (default: 10000)"
    )
    snapshot_parser.set_defaults(func=cmd_snapshot)

    # ls command
    ls_parser = subparsers.add_parser("ls", help="List files in a directory")
    ls_parser.add_argument("db", help="Catalog directory")
    ls_parser.add_argument("path", help="Directory path to list")
    ls_parser.add_argument(
        "-d", "--dirs", action="store_true", help="List subdirectories with stats"
    )
    ls_parser.add_argument(
        "--on-disk", action="store_true", help="Only show files currently on disk"
    )
    ls_parser.set_defaults(func=cmd_ls)

    # find command
    find_parser = subparsers.add_parser("find", help="Search for files by pattern")
    find_parser.add_argument("db", help="Catalog directory")
    find_parser.add_argument("pattern", help="Filename pattern (SQL LIKE syntax, use %% as wildcard)")
    find_parser.add_argument("--size-gt", help="Minimum size (e.g., 1GB, 500MB)")
    find_parser.add_argument("--size-lt", help="Maximum size (e.g., 1GB, 500MB)")
    find_parser.add_argument("-e", "--experiment", help="Filter by experiment")
    find_parser.add_argument(
        "--exclude", action="append", default=[],
        help="Exclude paths matching pattern (can be repeated)"
    )
    find_parser.add_argument(
        "--on-disk", action="store_true", help="Only show files currently on disk"
    )
    find_parser.add_argument(
        "--removed", action="store_true", help="Only show files that have been removed"
    )
    find_parser.add_argument(
        "--show-status", action="store_true", help="Show [removed] status for removed files"
    )
    find_parser.set_defaults(func=cmd_find)

    # tree command
    tree_parser = subparsers.add_parser("tree", help="Show directory tree")
    tree_parser.add_argument("db", help="Catalog directory")
    tree_parser.add_argument("path", help="Root path for tree")
    tree_parser.add_argument(
        "--depth", type=int, default=2, help="Maximum depth (default: 2)"
    )
    tree_parser.set_defaults(func=cmd_tree)

    # stats command
    stats_parser = subparsers.add_parser("stats", help="Show catalog statistics")
    stats_parser.add_argument("db", help="Catalog directory")
    stats_parser.set_defaults(func=cmd_stats)

    # query command
    query_parser = subparsers.add_parser(
        "query", help="Run raw SQL query (use 'files' as table name)"
    )
    query_parser.add_argument("db", help="Catalog directory")
    query_parser.add_argument("sql", help="SQL query (use 'files' as table name)")
    query_parser.set_defaults(func=cmd_query)

    # consolidate command
    consolidate_parser = subparsers.add_parser(
        "consolidate", help="Merge base + deltas into new base files"
    )
    consolidate_parser.add_argument("db", help="Catalog directory")
    consolidate_parser.add_argument(
        "--archive", help="Archive old files to this directory instead of deleting"
    )
    consolidate_parser.set_defaults(func=cmd_consolidate)

    # snapshots command
    snapshots_parser = subparsers.add_parser(
        "snapshots", help="List all snapshot files in the catalog"
    )
    snapshots_parser.add_argument("db", help="Catalog directory")
    snapshots_parser.add_argument(
        "-e", "--experiment", help="Filter by experiment hash"
    )
    snapshots_parser.set_defaults(func=cmd_snapshots)

    args = parser.parse_args()
    args.func(args)


if __name__ == "__main__":
    main()
