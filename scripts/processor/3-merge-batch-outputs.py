"""
Standalone merge script: concatenates multiple CSVs sharing the same header into one file.

Used to merge each main batch's already-complete output (1-main-parallel-script.py writes one
merged_output.csv per batch, see ExecutionWorkspace.batch_paths in execution_processor.py) into
the execution's single final output, once every batch has finished. Mirrors the split-worker
merge 1-main-parallel-script.py already does internally for the non-batch path (pd.concat +
to_csv) — kept as its own script, invoked via subprocess, so this CSV-merging logic lives in
scripts/ rather than being reimplemented in the service layer.

Usage:
    python scripts/processor/3-merge-batch-outputs.py \
        --input-csv path/to/batch_000/merged_output.csv \
        --input-csv path/to/batch_001/merged_output.csv \
        --output-csv path/to/final_output.csv
"""
import argparse

import pandas as pd


def _parse_args():
    parser = argparse.ArgumentParser(
        description="Concatenate multiple CSVs sharing the same header into one output CSV."
    )
    parser.add_argument(
        "--input-csv",
        required=True,
        action="append",
        help="Path to a source CSV to merge; repeat in the order files should be concatenated.",
    )
    parser.add_argument("--output-csv", required=True, help="Path to write the merged CSV")
    return parser.parse_args()


def merge_csv_files(input_csvs: list[str], output_csv: str) -> int:
    """Concatenate input_csvs (in the given order) into output_csv, one file at a time so
    memory use is bounded to a single batch rather than every batch loaded simultaneously.
    Returns total rows written."""
    total_rows = 0
    with open(output_csv, "w", newline="", encoding="utf-8") as out_f:
        for i, csv_path in enumerate(input_csvs):
            df = pd.read_csv(csv_path)
            df.to_csv(out_f, index=False, header=(i == 0))
            total_rows += len(df)
    return total_rows


if __name__ == "__main__":
    args = _parse_args()
    total_rows = merge_csv_files(args.input_csv, args.output_csv)
    print(f"✅ Merged {len(args.input_csv)} files into {args.output_csv} ({total_rows} rows)")
