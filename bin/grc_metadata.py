#!/usr/bin/env python3
# Copyright (C) 2023 Genome Surveillance Unit/Genome Research Ltd.
# Copyright (C) 2026 GenReMekong Core Team

import argparse
import csv
import sys


def parse_args():
    parser = argparse.ArgumentParser(
        description="Merge a metadata file into a GRC file by matching on identifier columns."
    )
    parser.add_argument("--grc_file",  "-g", required=True,      help="Path to the GRC file (TSV).")
    parser.add_argument("--grc_id",         default="ID",        help="Identifier column in the GRC file. Default: 'ID'")
    parser.add_argument("--meta_file", "-m", required=True,      help="Path to the metadata file (TSV).")
    parser.add_argument("--meta_id",        default="sample_id", help="Identifier column in the metadata file. Default: 'sample_id'")
    parser.add_argument("--output",    "-o", required=True,      help="Path to the output file.")
    parser.add_argument(
        "--mismatch",
        choices=["error", "inner", "left"],
        default="error",
        help=(
            "How to handle ID mismatches between GRC and metadata.\n"
            "  error – abort if the ID sets are not identical (default)\n"
            "  inner – keep only IDs present in both files\n"
            "  left  – keep all GRC IDs; metadata fields blank for unmatched rows"
        )
    )
    parser.add_argument(
        "--exclude_meta_cols",
        nargs="+",
        default=[],
        metavar="COL",
        help="Metadata columns to exclude before merging (e.g. columns that differ across duplicate rows)."
    )
    parser.add_argument(
        "--allow_duplicates",
        action="store_true",
        help=(
            "Backward-compat flag: suppress duplicate ID errors in the metadata file.\n"
            "When set, duplicate rows that are identical (after any --exclude_meta_cols) are\n"
            "silently deduplicated; rows that differ keep the LAST occurrence and log a warning."
        )
    )
    return parser.parse_args()


def read_tsv(path, id_column, exclude_cols=None, allow_duplicates=False):
    exclude_cols = set(exclude_cols or [])

    try:
        fh = open(path, newline="", encoding="utf-8")
    except FileNotFoundError:
        sys.exit(f"[ERROR] File not found: {path}")
    except PermissionError:
        sys.exit(f"[ERROR] Permission denied: {path}")

    with fh:
        reader = csv.DictReader(fh, delimiter="\t")

        if not reader.fieldnames:
            sys.exit(f"[ERROR] File is empty or missing a header: {path}")

        if id_column not in reader.fieldnames:
            sys.exit(
                f"[ERROR] Column '{id_column}' not found in '{path}'.\n"
                f"        Available columns: {', '.join(reader.fieldnames)}"
            )

        unknown_excl = exclude_cols - set(reader.fieldnames)
        if unknown_excl:
            sys.exit(
                f"[ERROR] --exclude_meta_cols column(s) not found in '{path}': "
                f"{', '.join(sorted(unknown_excl))}\n"
                f"        Available columns: {', '.join(reader.fieldnames)}"
            )

        records = {}
        for lineno, row in enumerate(reader, start=2):
            key = row.get(id_column, "").strip()

            if not key:
                print(f"[WARN]  Line {lineno} in '{path}': empty identifier, skipping.", file=sys.stderr)
                continue

            # Drop excluded columns from the row before storing
            row = {k: v for k, v in row.items() if k not in exclude_cols}

            if key in records:
                if not allow_duplicates:
                    sys.exit(
                        f"[ERROR] Duplicate identifier '{key}' on line {lineno} of '{path}'.\n"
                        f"        Use --allow_duplicates to suppress this error (last row wins),\n"
                        f"        or --exclude_meta_cols to ignore differing columns first."
                    )

                existing = records[key]
                if existing == row:
                    # Identical row — silently skip
                    continue
                else:
                    # Rows differ — warn and keep last occurrence
                    differing = [k for k in row if row[k] != existing.get(k)]
                    print(
                        f"[WARN]  Duplicate ID '{key}' on line {lineno} of '{path}' has "
                        f"conflicting values in: {', '.join(differing)}. Last row kept.",
                        file=sys.stderr,
                    )

            records[key] = row

    return records


PREVIEW = 5


def check_id_overlap(grc_ids, meta_ids, mismatch):
    grc_only  = grc_ids  - meta_ids
    meta_only = meta_ids - grc_ids

    if not grc_only and not meta_only:
        return grc_ids, meta_ids

    def _preview(id_set):
        sample = sorted(id_set)[:PREVIEW]
        suffix = f" ... (+{len(id_set) - PREVIEW} more)" if len(id_set) > PREVIEW else ""
        return ", ".join(sample) + suffix

    if grc_only:
        print(
            f"[INFO]  IDs in GRC but not in metadata: {len(grc_only):,}\n"
            f"        e.g. grc:{_preview(grc_only)}",
            file=sys.stderr,
        )
    if meta_only:
        print(
            f"[INFO]  IDs in metadata but not in GRC: {len(meta_only):,}\n"
            f"        e.g. meta:{_preview(meta_only)}",
            file=sys.stderr,
        )

    if mismatch == "error":
        sys.exit(
            "[ERROR] ID sets are not identical. Use --mismatch inner or left to proceed anyway."
        )
    elif mismatch == "inner":
        shared = grc_ids & meta_ids
        print(f"[INFO]  --mismatch inner: keeping {len(shared):,} shared IDs.", file=sys.stderr)
        return shared, shared
    else:  # left
        print(
            f"[INFO]  --mismatch left: keeping all {len(grc_ids):,} GRC IDs; "
            f"{len(grc_only):,} rows will have empty metadata fields.",
            file=sys.stderr,
        )
        return grc_ids, meta_ids


def merge(grc, meta, keep_grc_ids):
    merged = {}
    for record_id in keep_grc_ids:
        merged_row = dict(grc[record_id])
        merged_row.update(meta.get(record_id, {}))
        merged[record_id] = merged_row
    return merged


def write_tsv(path, fieldnames, rows):
    try:
        with open(path, "w", newline="", encoding="utf-8") as fh:
            writer = csv.DictWriter(
                fh, delimiter="\t", extrasaction="ignore", fieldnames=fieldnames
            )
            writer.writeheader()
            writer.writerows(rows.values())
    except PermissionError:
        sys.exit(f"[ERROR] Permission denied writing to: {path}")


def main():
    args = parse_args()

    print(f"[INFO]  Reading GRC:      {args.grc_file}  (id='{args.grc_id}')")
    grc = read_tsv(args.grc_file, args.grc_id)
    print(f"[INFO]  {len(grc):,} records loaded from GRC.")

    print(f"[INFO]  Reading metadata: {args.meta_file}  (id='{args.meta_id}')")
    if args.exclude_meta_cols:
        print(f"[INFO]  Excluding metadata columns: {', '.join(args.exclude_meta_cols)}")
    meta = read_tsv(
        args.meta_file,
        args.meta_id,
        exclude_cols=args.exclude_meta_cols,
        allow_duplicates=args.allow_duplicates,
    )
    print(f"[INFO]  {len(meta):,} records loaded from metadata.")

    keep_grc_ids, _ = check_id_overlap(set(grc), set(meta), args.mismatch)

    merged = merge(grc, meta, keep_grc_ids)
    print(f"[INFO]  {len(merged):,} records after merge.")

    if not merged:
        print("[WARN]  Nothing to write — output will be header-only.", file=sys.stderr)

    fieldnames = list(next(iter(merged.values())).keys()) if merged else []
    write_tsv(args.output, fieldnames, merged)
    print(f"[INFO]  Written to: {args.output}")


if __name__ == "__main__":
    main()