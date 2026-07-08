"""
Standalone cleanup script: removes rows with no AI-evaluation result from a processed output CSV.

A row ends up with an empty 'Task evidence Q and A' / 'Task evidence Q and A Reason' when the
processor never got a usable AI answer for it — see 1-main-parallel-script.py's skip paths: no
question found for the task ("User-Owned"), the relevant-evidence cap was reached ("Capped",
tag=notValidated), or the AI response was invalid ("Failed"). notValidated rows are identified
explicitly by the Relevance Tag column; blank Q&A catches the remaining skip paths
(Failed/User-Owned) that carry no tag. Both conditions are checked and counted separately so
the summary shows how many rows were capped vs how many had other missing data.

Usage:
    python scripts/processor/2-remove-nonvalidated-and-empty-evidences.py \
        --input-csv path/to/merged_output.csv \
        --output-csv path/to/cleaned_output.csv
"""
import argparse
import csv
import os
import re
import sys
from pathlib import Path

# Allow importing from the service package (core/, etc.) — mirrors 1-main-parallel-script.py
SERVICE_ROOT = Path(__file__).resolve().parents[2]
if str(SERVICE_ROOT) not in sys.path:
    sys.path.insert(0, str(SERVICE_ROOT))
from core.constants import RELEVANCE_TAG_NOT_VALIDATED

CHECK_COLUMNS = ("Task evidence Q and A", "Task evidence Q and A Reason")
URL_COLUMN = "Task Evidence"
RELEVANCE_TAG_COLUMN = "Relevance Tag"
URL_PATTERN = re.compile(r'https?://[^\s<>"{}|\\^`\[\]]+')


def _parse_args():
    parser = argparse.ArgumentParser(
        description="Remove rows with no AI-evaluation result (notValidated tag or blank Q&A) from a processed output CSV."
    )
    parser.add_argument("--input-csv", required=True, help="Path to the processed output CSV to clean")
    parser.add_argument("--output-csv", required=True, help="Path to write the cleaned CSV")
    return parser.parse_args()


def remove_not_validated(input_csv: str, output_csv: str) -> tuple[int, int, int, int]:
    """Write input_csv to output_csv with invalid rows removed.

    A row is invalid if its Relevance Tag is 'notValidated' (relevant-evidence cap reached)
    or if either Q&A column is blank (Failed/User-Owned skip paths).

    Returns (total_rows, removed_rows, not_validated_count, extracted_url_count):
      - not_validated_count: rows explicitly tagged notValidated
      - removed_rows: all removed rows (notValidated + blank-Q&A)
      - extracted_url_count: count of Task Evidence values from removed rows — the values
        themselves are never logged (may carry cloud-storage signed-URL tokens); the
        uploaded unfiltered CSV is the audit trail for what was actually removed.
    """
    output_dir = os.path.dirname(output_csv)
    if output_dir:
        os.makedirs(output_dir, exist_ok=True)

    total = 0
    removed = 0
    not_validated_count = 0
    extracted_url_count = 0
    with open(input_csv, newline="", encoding="utf-8") as infile:
        reader = csv.DictReader(infile)
        if not reader.fieldnames:
            raise ValueError(f"'{input_csv}' has no header row.")
        missing = [col for col in (*CHECK_COLUMNS, URL_COLUMN, RELEVANCE_TAG_COLUMN) if col not in reader.fieldnames]
        if missing:
            raise ValueError(f"'{input_csv}' is missing column(s): {', '.join(missing)}")

        with open(output_csv, "w", newline="", encoding="utf-8") as outfile:
            writer = csv.DictWriter(outfile, fieldnames=reader.fieldnames)
            writer.writeheader()
            for row in reader:
                total += 1
                is_not_validated = (row.get(RELEVANCE_TAG_COLUMN) or "").strip() == RELEVANCE_TAG_NOT_VALIDATED
                is_blank_qa = any(not (row.get(col) or "").strip() for col in CHECK_COLUMNS)
                is_invalid = is_not_validated or is_blank_qa
                if is_invalid:
                    removed += 1
                    if is_not_validated:
                        not_validated_count += 1
                    evidence_value = (row.get(URL_COLUMN) or "").strip()
                    if evidence_value:
                        found = URL_PATTERN.findall(evidence_value)
                        extracted_url_count += len(found) if found else 1
                    continue
                writer.writerow(row)

    return total, removed, not_validated_count, extracted_url_count


if __name__ == "__main__":
    args = _parse_args()

    print(f"📖 Reading: {args.input_csv}")
    total, removed, not_validated_count, extracted_url_count = remove_not_validated(args.input_csv, args.output_csv)
    kept = total - removed
    blank_qa_count = removed - not_validated_count

    print(f"\n{'=' * 60}")
    print(f"{'INVALID ROWS CLEANUP SUMMARY':^60}")
    print(f"{'=' * 60}")
    print(f"Total rows read:               {total}")
    print(f"Removed (notValidated tag):    {not_validated_count}")
    print(f"Removed (blank Q&A / other):   {blank_qa_count}")
    print(f"Removed (total):               {removed}")
    print(f"Kept:                          {kept}")
    print(f"Output written to:             {args.output_csv}")
    print(f"{'=' * 60}")

    if extracted_url_count:
        print(f"\nEvidence values from removed rows: {extracted_url_count} (values omitted from logs)")
