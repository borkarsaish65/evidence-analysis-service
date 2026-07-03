import os
import sys
import csv
import json
import math
import argparse
import re
from urllib.parse import urlparse
from tqdm import tqdm  # Import tqdm for the progress bar
from dotenv import load_dotenv
from pathlib import Path

# Load environment variables from .env file (look in parent directory)
env_path = Path(__file__).parent.parent / '.env'
load_dotenv(dotenv_path=env_path)

# Allow importing from the service package (core/, etc.) — mirrors 1-main-parallel-script.py
SERVICE_ROOT = Path(__file__).resolve().parents[2]
if str(SERVICE_ROOT) not in sys.path:
    sys.path.insert(0, str(SERVICE_ROOT))
from core.constants import EVIDENCE_TYPE_EXTENSIONS

def str2bool(val):
    return str(val).lower() in ("1", "true", "yes")

def _parse_args():
    parser = argparse.ArgumentParser(description="Pre-process execution CSV input.")
    parser.add_argument("--input-csv", default=None, help="Input CSV path")
    parser.add_argument("--question-csv", default=None, help="Questions CSV path")
    parser.add_argument(
        "--question-task-column",
        default=None,
        help="Configured task column name in questions CSV (from CSV config)",
    )
    parser.add_argument("--filter-csv", default=None, help="Optional school filter CSV path")
    parser.add_argument("--output-dir", default=None, help="Output directory path")
    parser.add_argument("--split-files", default=None, choices=["yes", "no"], help="Split output files or not")
    parser.add_argument("--rows-per-file", default=None, type=int, help="Rows per split file")
    parser.add_argument("--use-school-filter", default=None, help="Enable school filtering (true/false)")
    parser.add_argument(
        "--evidence-types",
        default=None,
        help="Comma list of allowed evidence types (image,pdf,excel); absent or empty = all",
    )
    parser.add_argument("--main-batch-rows", default=None, type=int, help="Target rows per main batch")
    parser.add_argument("--max-main-batches", default=None, type=int, help="Hard cap on number of main batches")
    parser.add_argument(
        "--skip-batch-cut",
        action="store_true",
        help="Run the normal filter/sort/split path even if MAIN_FILE_SPLIT is on — "
        "used for the per-batch sub-invocations, whose input is already one main batch.",
    )
    parser.add_argument("--max-relevant-per-user-task", default=None, type=int, help="Per-(UUID, task) relevant-evidence cap; enables group-aware splitting when set")
    return parser.parse_args()

ARGS = _parse_args()

# === Configuration ===
DEFAULT_INPUT_CSV = "/home/dell/workspace/EVIDENCE_ANALYSIS/evidence-analysis-multithreaded/input/input.csv"
DEFAULT_QUESTION_CSV = "/home/dell/workspace/EVIDENCE_ANALYSIS/evidence-analysis-multithreaded/input/question.csv"
DEFAULT_FILTER_CSV = "/home/dell/workspace/EVIDENCE_ANALYSIS/evidence-analysis-multithreaded/input/school_list.csv"

INPUT_CSV = ARGS.input_csv or os.getenv("PREPROCESS_INPUT_CSV") or DEFAULT_INPUT_CSV
QUESTION_CSV = ARGS.question_csv or os.getenv("PREPROCESS_QUESTION_CSV") or DEFAULT_QUESTION_CSV
TASK_MATCH_COLUMN_CONFIG = (
    (ARGS.question_task_column or "").strip()
    or (os.getenv("PREPROCESS_QUESTION_TASK_COLUMN", "") or "").strip()
)
FILTER_CSV = ARGS.filter_csv or os.getenv("PREPROCESS_FILTER_CSV") or DEFAULT_FILTER_CSV
OUTPUT_DIR = ARGS.output_dir or os.getenv("PREPROCESS_OUTPUT_DIR") or "output-pre-processor"
DEFAULT_QUESTION_TASK_COLUMN = "TASK NAME"
DEFAULT_INPUT_TASK_COLUMN = "Tasks"

use_school_filter_value = (
    ARGS.use_school_filter
    if ARGS.use_school_filter is not None
    else os.getenv("PREPROCESS_USE_SCHOOL_FILTER", os.getenv("USE_SCHOOL_FILTER", False))
)
USE_SCHOOL_FILTER = str2bool(use_school_filter_value)  # Set True to filter by school_list.csv

evidence_types_value = ARGS.evidence_types or os.getenv("PREPROCESS_EVIDENCE_TYPES", "")
ALLOWED_EVIDENCE_TYPES = {
    t.strip().lower() for t in evidence_types_value.split(",") if t.strip()
} or None  # None = no restriction, all types allowed

# === SPLIT CONFIGURATION ===
SPLIT_FILES = ARGS.split_files or os.getenv("PREPROCESS_SPLIT_FILES") or os.getenv("SPLIT_FILES", "yes")
ROWS_PER_FILE = ARGS.rows_per_file or int(os.getenv("PREPROCESS_ROWS_PER_FILE", os.getenv("ROWS_PER_FILE", "15000")))

# Group-aware splitting: enabled when the relevant-evidence cap is active (cap value
# passed via --max-relevant-per-user-task). When on, every (UUID, task) group is kept
# inside one split file so the processor's per-worker cap counts stay correct.
# Off by default → original size-only splitting.
GROUP_AWARE_SPLIT = ARGS.max_relevant_per_user_task is not None

# === MAIN-BATCH CONFIGURATION ===
# MAIN_FILE_SPLIT is the on/off switch (mirrors the sister repo's SPLIT_FILES=yes/no
# convention): "true" cuts the filtered+sorted rows into sequential main batches before
# the normal per-file split runs on each one; "no"/absent processes everything as a
# single main file, today's behavior, unchanged.
MAIN_FILE_SPLIT = str2bool(os.getenv("MAIN_FILE_SPLIT", "no"))
# Sizing target (rows per batch) — batch COUNT is derived from this, not set directly,
# same shape as ROWS_PER_FILE deriving the fine-split file count above.
MAIN_BATCH_ROWS_PER_BATCH = ARGS.main_batch_rows or int(os.getenv("MAIN_BATCH_ROWS_PER_BATCH", "10000"))
# Safety ceiling, not a target: only overrides the derived count above if it would
# otherwise exceed this many batches (then the effective batch size grows instead).
MAX_MAIN_BATCHES = ARGS.max_main_batches or int(os.getenv("MAX_MAIN_BATCHES", "200"))

# Debug: Print loaded configuration
print(f"🔧 Configuration Loaded:")
print(f"   SPLIT_FILES: {SPLIT_FILES}")
print(f"   ROWS_PER_FILE: {ROWS_PER_FILE}")
print(f"   GROUP_AWARE_SPLIT: {GROUP_AWARE_SPLIT}")
print(f"   MAIN_FILE_SPLIT: {MAIN_FILE_SPLIT}")
print(f"   MAIN_BATCH_ROWS_PER_BATCH: {MAIN_BATCH_ROWS_PER_BATCH}")
print(f"   MAX_MAIN_BATCHES: {MAX_MAIN_BATCHES}")
print(f"   USE_SCHOOL_FILTER: {USE_SCHOOL_FILTER}")
print(f"   ALLOWED_EVIDENCE_TYPES: {sorted(ALLOWED_EVIDENCE_TYPES) if ALLOWED_EVIDENCE_TYPES else 'all'}")
print(f"   TASK_MATCH_COLUMN_CONFIG: {TASK_MATCH_COLUMN_CONFIG or '(missing)'}")
print(f"   QUESTION_TASK_COLUMN_FALLBACK: {DEFAULT_QUESTION_TASK_COLUMN}")
print(f"   INPUT_TASK_COLUMN_FALLBACK: {DEFAULT_INPUT_TASK_COLUMN}")
print()

# === EVIDENCE FORMATS ===
IMAGE_FORMATS = set(EVIDENCE_TYPE_EXTENSIONS["image"])
PDF_FORMATS = set(EVIDENCE_TYPE_EXTENSIONS["pdf"])
EXCEL_FORMATS = set(EVIDENCE_TYPE_EXTENSIONS["excel"])
ALL_VALID_FORMATS = IMAGE_FORMATS | PDF_FORMATS | EXCEL_FORMATS

# Create output directory
os.makedirs(OUTPUT_DIR, exist_ok=True)

# Counters for skipped rows
skip_task_not_in_questions = 0
skip_evidence_null = 0
skip_school_mismatch = 0
skip_invalid_evidence = 0  # Renamed from skip_non_image to handle all invalid evidence types
skip_evidence_type_excluded = 0  # Evidence type valid but not in ALLOWED_EVIDENCE_TYPES
total_input_rows = 0 # This will be set correctly below

# === Step 1: Load FILTER_CSV school codes into a set ===
valid_school_codes = set()
if USE_SCHOOL_FILTER and os.path.exists(FILTER_CSV):
    with open(FILTER_CSV, newline='', encoding="utf-8") as f:
        reader = csv.DictReader(f)
        for row in reader:
            school_code = row.get("UDISE+ SCHOOL CODE", "").strip()
            if school_code:
                valid_school_codes.add(school_code)
    print(f"✅ Loaded {len(valid_school_codes)} school codes from '{FILTER_CSV}'")
elif USE_SCHOOL_FILTER and not os.path.exists(FILTER_CSV):
    print(f"⚠️  USE_SCHOOL_FILTER is True but FILTER_CSV '{FILTER_CSV}' not found. Skipping school filter.")
    USE_SCHOOL_FILTER = False
else:
    print(f"⏭️  School filtering disabled (USE_SCHOOL_FILTER={USE_SCHOOL_FILTER})")

# === Helper function for cleaning cell values ===
def clean_cell(value):
    """Strips whitespace AND common quote characters from the ends."""
    if not isinstance(value, str):
        return ""
    # Strip whitespace, then strip both single and double quotes
    return value.strip().strip("'\"")

# === Header resolver helpers ===
def _normalize_header_name(name):
    if not isinstance(name, str):
        return ""
    return " ".join(name.strip().lower().split())


def _resolve_header_name(fieldnames, candidates):
    normalized_to_actual = {
        _normalize_header_name(field): field
        for field in (fieldnames or [])
        if isinstance(field, str) and field.strip()
    }
    for candidate in candidates:
        resolved = normalized_to_actual.get(_normalize_header_name(candidate))
        if resolved:
            return resolved
    return None


def _is_valid_column_name(column_name):
    if not isinstance(column_name, str):
        return False
    normalized = column_name.strip()
    if not normalized:
        return False
    # Keep validation strict to catch malformed config quickly.
    return bool(re.fullmatch(r"[A-Za-z0-9 _().+\-]+", normalized))


def _resolve_questions_task_column(fieldnames, configured_column):
    normalized_to_actual = {
        _normalize_header_name(field): field
        for field in (fieldnames or [])
        if isinstance(field, str) and field.strip()
    }

    configured = (configured_column or "").strip()
    if configured:
        if not _is_valid_column_name(configured):
            print(
                "⚠️  Invalid configured task column name in CSV config: "
                f"'{configured}'. Falling back to '{DEFAULT_QUESTION_TASK_COLUMN}'."
            )
        else:
            resolved = normalized_to_actual.get(_normalize_header_name(configured))
            if resolved:
                print(f"✅ Using configured task column from CSV config: '{resolved}'")
                return resolved
            print(
                "⚠️  Configured task column "
                f"'{configured}' not found in Questions CSV. "
                f"Falling back to '{DEFAULT_QUESTION_TASK_COLUMN}'."
            )
    else:
        print(
            "⚠️  Missing task column in CSV config. "
            f"Falling back to '{DEFAULT_QUESTION_TASK_COLUMN}'."
        )

    fallback = normalized_to_actual.get(_normalize_header_name(DEFAULT_QUESTION_TASK_COLUMN))
    if fallback:
        print(f"✅ Using fallback task column: '{fallback}'")
        return fallback

    raise ValueError(
        "Could not resolve task column in Questions CSV. "
        f"Configured column: '{configured or '(missing)'}'; "
        f"fallback '{DEFAULT_QUESTION_TASK_COLUMN}' was also not found. "
        f"Detected columns: {list(fieldnames or [])}"
    )


def _resolve_input_task_column(fieldnames, configured_column):
    normalized_to_actual = {
        _normalize_header_name(field): field
        for field in (fieldnames or [])
        if isinstance(field, str) and field.strip()
    }

    configured = (configured_column or "").strip()
    if configured:
        if not _is_valid_column_name(configured):
            print(
                "⚠️  Invalid configured input-match column in CSV config: "
                f"'{configured}'. Falling back to '{DEFAULT_INPUT_TASK_COLUMN}'."
            )
        else:
            resolved = normalized_to_actual.get(_normalize_header_name(configured))
            if resolved:
                print(f"✅ Using configured input task column: '{resolved}'")
                return resolved
            print(
                "⚠️  Configured input-match column "
                f"'{configured}' not found in Input CSV. "
                f"Falling back to '{DEFAULT_INPUT_TASK_COLUMN}'."
            )
    else:
        print(
            "⚠️  Missing input-match column in CSV config. "
            f"Falling back to '{DEFAULT_INPUT_TASK_COLUMN}'."
        )

    fallback = normalized_to_actual.get(_normalize_header_name(DEFAULT_INPUT_TASK_COLUMN))
    if fallback:
        print(f"✅ Using fallback input task column: '{fallback}'")
        return fallback

    raise ValueError(
        "Could not resolve input task column for matching. "
        f"Configured column: '{configured or '(missing)'}'; "
        f"fallback '{DEFAULT_INPUT_TASK_COLUMN}' was also not found. "
        f"Detected columns: {list(fieldnames or [])}"
    )

# === Robust task name normalizer (lookup-only — never written to CSV) ===
def normalize_task_name(name):
    """
    Normalize a task name ONLY for question-lookup purposes (never written to CSV).

    Problem: the same task arrives with many stray-quote / spacing variants:
      '4. \'Conduct Quiz Activity\''  (quote right after number prefix + trailing)
      '4. Conduct Quiz Activity\''    (trailing quote only)
      '4.Conduct Quiz Activity'        (no space after dot)
      ' 4. Conduct Quiz Activity  '    (extra spaces)

    All of the above must resolve to the SAME lookup key.

    Rules (number prefix is KEPT, only stray quotes / spaces cleaned):
      1. Strip surrounding whitespace
      2. Strip leading/trailing single & double quotes
      3. Remove stray quote(s) directly after 'N. ' — e.g. '4. \'' → '4. '
      4. Strip trailing quotes / periods / whitespace
      5. Collapse consecutive internal spaces to one
      6. Lowercase
    """
    import re
    if not isinstance(name, str) or not name:
        return ""
    s = name.strip()
    s = s.strip("'\"")
    # Remove stray quote(s) immediately after a numeric prefix like '4. '
    s = re.sub(r"^(\d+\.\s*)['\"]+(\s*)", r"\1", s)
    # Normalize number prefix to always have exactly one space after the dot
    # e.g. "4.Conduct" → "4. Conduct",  "4.  Conduct" → "4. Conduct"
    s = re.sub(r'^(\d+\.)\s*', r'\1 ', s).strip()
    # Strip any remaining trailing quotes / periods / spaces
    s = s.rstrip("'.\" ").strip()
    # Collapse internal whitespace
    s = re.sub(r'\s+', ' ', s)
    # NFC normalization: critical for Devanagari text where the same glyph can be
    # stored as precomposed (NFC) or decomposed (NFD) codepoints across files.
    import unicodedata
    s = unicodedata.normalize("NFC", s)
    return s.lower()

# === Helper function to determine evidence type ===
def get_evidence_type(url):
    """Determine the evidence type from URL. Returns: 'image', 'pdf', 'excel', or None"""
    url = clean_cell(url) # Clean the URL string first for *checking*
    if not url or url.lower() == "null":
        return None
    try:
        parsed = urlparse(url)
        path = parsed.path.lower()
        for ext in IMAGE_FORMATS:
            if path.endswith(ext):
                return "image"
        for ext in PDF_FORMATS:
            if path.endswith(ext):
                return "pdf"
        for ext in EXCEL_FORMATS:
            if path.endswith(ext):
                return "excel"
    except:
        pass
    return None

def is_valid_evidence_url(url):
    """Check if URL points to a valid evidence file (image, PDF, or Excel)"""
    return get_evidence_type(url) is not None

# === Step 2: Load QUESTION_CSV into dictionary (TASK NAME → Refined Question) ===
# Keys stored in THREE forms so we match any surface variant from input data:
#   1. raw cleaned form  (e.g. "4. 'Conduct Quiz Activity")
#   2. rstrip-only form  (e.g. "4. 'Conduct Quiz Activity"  → same here, but covers others)
#   3. fully-normalized  (e.g. "conduct quiz activity")
lookup_dict = {}         # raw/cleaned key  → question
lookup_dict_norm = {}    # normalized key   → question
with open(QUESTION_CSV, newline='', encoding="utf-8") as f:
    reader = csv.DictReader(f)
    task_column = _resolve_questions_task_column(
        reader.fieldnames,
        TASK_MATCH_COLUMN_CONFIG,
    )
    question_column = _resolve_header_name(
        reader.fieldnames,
        [
            "Refined questions using tool and webpage",
            "Question",
            "QUESTION",
            "Questions",
            "QUESTIONS FOR METRICS",
            "Evidence Criteria",
        ],
    )
    if not question_column:
        print(
            "⚠️  Could not resolve question CSV headers. "
            f"Detected columns: {reader.fieldnames or []}"
        )

    for row in reader:
        task_name_raw = row.get(task_column, "") if task_column else ""
        task_name = clean_cell(task_name_raw)
        task_norm  = normalize_task_name(task_name_raw)
        refined_question = (
            row.get(question_column, "").strip() if question_column else ""
        )
        if task_name and refined_question:
            lookup_dict[task_name] = refined_question
        if task_norm and refined_question:
            lookup_dict_norm[task_norm] = refined_question

print(f"✅ Loaded {len(lookup_dict_norm)} unique normalized task keys from question.csv")

# === District Renaming Map ===
DISTRICT_REPLACEMENTS = {
    "W Champaran": "West Champaran",
    "E. Champaran": "East Champaran",
    "Kaimur (Bhabua)": "Kaimur",
    "Aurangabad (Bihar)": "Aurangabad"
}

# === Step 3: Load INPUT_CSV and filter ===

# --- NEW: Load all rows into a list first to get the *correct* count ---
print(f"Loading data from {INPUT_CSV}...")
all_rows = []
try:
    with open(INPUT_CSV, newline='', encoding="utf-8") as infile:
        reader = csv.DictReader(infile)
        all_rows = list(reader)
        total_input_rows = len(all_rows) # This is the CORRECT row count
        header = reader.fieldnames
except FileNotFoundError:
    print(f"Error: INPUT_CSV '{INPUT_CSV}' not found.")
    exit()
except Exception as e:
    print(f"Error reading {INPUT_CSV}: {e}")
    exit()

if header is None:
    print("Error: CSV Header is empty. Cannot proceed.")
    exit()
print(f"Loaded {total_input_rows} data rows to process.")
# --- END NEW ---

input_task_column = _resolve_input_task_column(header, TASK_MATCH_COLUMN_CONFIG)

filtered_rows = []

# Add new columns
new_columns = [
    "Task Evidence Question",
    "Task evidence Q and A",
    "Task evidence Q and A Reason",
    "Relevance Tag",
    "Image Preview",
    "Evidence Type"  # NEW: Track evidence type (image, pdf, excel)
]

final_header = list(header)
for col in new_columns:
    if col not in final_header:
        final_header.append(col)

# --- NEW: Iterate over the list 'all_rows' instead of the 'reader' object ---
for row in tqdm(all_rows, total=total_input_rows, desc="Processing input CSV"):
    
    school_id = row.get("School ID", "").strip()
    task_raw = row.get(input_task_column, "")
    task = clean_cell(task_raw)           # Clean task for exact lookup
    task_norm = normalize_task_name(task_raw)  # Normalized for fuzzy fallback lookup
    evidence = row.get("Task Evidence", "") # Get raw evidence

    # Rule 0: Skip if School ID not in FILTER_CSV (only if USE_SCHOOL_FILTER is enabled)
    if USE_SCHOOL_FILTER and school_id not in valid_school_codes:
        skip_school_mismatch += 1
        continue

    # Rule 1: Skip if task has no matching question in the Question CSV.
    # Valid tasks are derived dynamically from the Question CSV — no task numbers hardcoded.
    _q = lookup_dict.get(task) or lookup_dict_norm.get(task_norm)
    if not _q:
        skip_task_not_in_questions += 1
        continue

    # Rule 2: Skip if evidence is empty or "null" (after cleaning for check)
    cleaned_evidence = clean_cell(evidence)
    if cleaned_evidence == "" or cleaned_evidence.lower() == "null":
        skip_evidence_null += 1
        continue

    # Rule 3: Skip if evidence URL is not a valid format (image, PDF, or Excel)
    evidence_type = get_evidence_type(evidence)  # Get evidence type for valid URLs
    if evidence_type is None:
        skip_invalid_evidence += 1
        continue

    # Rule 3b: Skip if evidence type is valid but excluded by the execution's evidence-type filter
    if ALLOWED_EVIDENCE_TYPES is not None and evidence_type not in ALLOWED_EVIDENCE_TYPES:
        skip_evidence_type_excluded += 1
        continue

    # === Step 4: Fill additional columns & Clean District ===
    # _q already resolved above in Rule 1 — reuse directly.
    row["Task Evidence Question"] = _q
    row["Task evidence Q and A"] = ""
    row["Task evidence Q and A Reason"] = ""
    row["Relevance Tag"] = ""
    row["Image Preview"] = ""
    row["Evidence Type"] = evidence_type  # NEW: Store evidence type
    
    # Apply District replacement
    current_district = row.get("District", "")
    row["District"] = DISTRICT_REPLACEMENTS.get(current_district, current_district)

    # Row passes all checks
    filtered_rows.append([row.get(h, "") for h in final_header])

# === Step 4b: Group-aware ordering for the relevant-evidence cap ===
# Sort rows so every (UUID, <task>) pair is contiguous. This lets Step 5 split files only
# at group boundaries, keeping each pair inside one file (required for the processor's
# per-worker cap to count correctly). Skipped unless group-aware splitting is enabled, so
# default runs keep their original row order untouched.
_uuid_idx = final_header.index("UUID") if "UUID" in final_header else None
_task_idx = final_header.index(input_task_column) if input_task_column in final_header else None
_group_aware_active = GROUP_AWARE_SPLIT and _uuid_idx is not None and _task_idx is not None
if GROUP_AWARE_SPLIT and not _group_aware_active:
    print("⚠️  group-aware splitting requested but UUID/task column missing — falling back to size-only splitting.")
if _group_aware_active:
    filtered_rows.sort(key=lambda r: (str(r[_uuid_idx]), str(r[_task_idx])))
    print(f"✅ Sorted {len(filtered_rows)} rows by (UUID, {input_task_column}) for group-aware splitting.")

# === Step 4c: Main-batch cut (sequential processing of very large uploads) ===
# Runs once, before the normal Step 5 split. Cuts filtered_rows into N main batches —
# written as their own filtered CSVs, NOT split_*.csv — which the service then feeds back
# through this same script one at a time (with --skip-batch-cut) to get each batch's normal
# fine-grained split_*.csv files. Skipped entirely for the per-batch sub-invocations.
if MAIN_FILE_SPLIT and not ARGS.skip_batch_cut:
    num_batches = math.ceil(len(filtered_rows) / MAIN_BATCH_ROWS_PER_BATCH) if filtered_rows else 0
    if num_batches > MAX_MAIN_BATCHES:
        print(f"⚠️  Natural batch count {num_batches} exceeds MAX_MAIN_BATCHES ({MAX_MAIN_BATCHES}) — "
              f"capping batch count and growing effective batch size instead.")
        num_batches = MAX_MAIN_BATCHES
    effective_batch_rows = math.ceil(len(filtered_rows) / num_batches) if num_batches else len(filtered_rows)

    # Same accumulator + group-boundary technique as the fine split below, just at the
    # coarser main-batch grain — a (UUID, task) group must never span two main batches,
    # otherwise it could also end up split across two DIFFERENT fine split files later.
    if _group_aware_active:
        batches = []
        current = []
        for j, r in enumerate(filtered_rows):
            current.append(r)
            at_target = len(current) >= effective_batch_rows
            is_last = j == len(filtered_rows) - 1
            this_key = (str(r[_uuid_idx]), str(r[_task_idx]))
            next_key = None if is_last else (
                str(filtered_rows[j + 1][_uuid_idx]), str(filtered_rows[j + 1][_task_idx])
            )
            at_boundary = is_last or next_key != this_key
            if at_target and at_boundary:
                batches.append(current)
                current = []
        if current:
            batches.append(current)
    else:
        batches = [
            filtered_rows[i * effective_batch_rows:(i + 1) * effective_batch_rows]
            for i in range(math.ceil(len(filtered_rows) / effective_batch_rows))
        ] if filtered_rows else []

    padding_width = max(1, len(str(len(batches))))
    actual_rows_written = 0
    for i, batch in enumerate(batches):
        batch_file = os.path.join(OUTPUT_DIR, f"filtered_batch_{str(i+1).zfill(padding_width)}.csv")
        with open(batch_file, "w", newline='', encoding="utf-8") as outfile:
            writer = csv.writer(outfile)
            writer.writerow(final_header)
            writer.writerows(batch)
        actual_rows_written += len(batch)
        print(f"✅ Created: {batch_file} ({len(batch)} rows)")
        if _group_aware_active and len(batch) > 2 * effective_batch_rows:
            print(f"⚠️  {os.path.basename(batch_file)} has {len(batch)} rows "
                  f"(>2× target {effective_batch_rows}) — one (UUID, task) group is oversized.")

    batch_manifest = {
        "total_batches": len(batches),
        "rows_per_batch_target": effective_batch_rows,
        "total_rows": len(filtered_rows),
        "actual_rows_written": actual_rows_written,
        "group_aware": _group_aware_active,
        "max_main_batches": MAX_MAIN_BATCHES,
    }
    with open(os.path.join(OUTPUT_DIR, "batch_manifest.json"), "w", encoding="utf-8") as mf:
        json.dump(batch_manifest, mf, indent=2)

    if actual_rows_written != len(filtered_rows):
        print(f"⚠️  WARNING: Row count mismatch! Expected {len(filtered_rows)}, wrote {actual_rows_written}")
    else:
        print(f"✅ Validated: All {actual_rows_written} rows written across {len(batches)} main batches")
    print(f"Mode: Main-batch cut into {len(batches)} batches (~{effective_batch_rows} rows each) — "
          f"exiting before fine-grained split; each batch is split separately.")
    exit()

# === Step 5: Output - Single file or Multiple files based on configuration ===
if SPLIT_FILES.lower() == "no":
    # Single file output
    output_file = os.path.join(OUTPUT_DIR, "preprocessed_data.csv")
    with open(output_file, "w", newline='', encoding="utf-8") as outfile:
        writer = csv.writer(outfile)
        writer.writerow(final_header)
        writer.writerows(filtered_rows)
    
    print(f"✅ Created: {output_file} ({len(filtered_rows)} rows)")
    print(f"Mode: Single file output")
    
    # Create manifest for single file mode
    manifest = {
        "total_splits": 1,
        "rows_per_file": len(filtered_rows),
        "total_rows": len(filtered_rows),
        "split_enabled": False
    }
    manifest_file = os.path.join(OUTPUT_DIR, "split_manifest.json")
    with open(manifest_file, "w", encoding="utf-8") as mf:
        json.dump(manifest, mf, indent=2)
    print(f"✅ Created manifest: {manifest_file}")
    
else:
    # Split into multiple files.
    # Compute chunks first: group-aware when the cap is active (never cut a (UUID, task)
    # group across files), otherwise the original fixed-size slicing (behavior unchanged).
    if _group_aware_active:
        chunks = []
        current = []
        for j, r in enumerate(filtered_rows):
            current.append(r)
            at_target = len(current) >= ROWS_PER_FILE
            is_last = j == len(filtered_rows) - 1
            this_key = (str(r[_uuid_idx]), str(r[_task_idx]))
            next_key = None if is_last else (
                str(filtered_rows[j + 1][_uuid_idx]), str(filtered_rows[j + 1][_task_idx])
            )
            # Only close the current file at a group boundary, so a (UUID, task) pair
            # never straddles two files.
            at_boundary = is_last or next_key != this_key
            if at_target and at_boundary:
                chunks.append(current)
                current = []
        if current:
            chunks.append(current)
    else:
        chunks = [
            filtered_rows[i * ROWS_PER_FILE:(i + 1) * ROWS_PER_FILE]
            for i in range(math.ceil(len(filtered_rows) / ROWS_PER_FILE))
        ]

    total_files = len(chunks)

    # Calculate padding width for filenames (e.g., 3 digits for up to 999 files)
    padding_width = max(1, len(str(total_files)))

    actual_rows_written = 0
    rows_before = 0

    for i, chunk in enumerate(chunks):
        # Zero-padded filename (e.g., split_001.csv, split_002.csv)
        output_file = os.path.join(OUTPUT_DIR, f"split_{str(i+1).zfill(padding_width)}.csv")
        with open(output_file, "w", newline='', encoding="utf-8") as outfile:
            writer = csv.writer(outfile)
            writer.writerow(final_header)
            writer.writerows(chunk)

        actual_rows_written += len(chunk)
        print(f"✅ Created: {output_file} (rows {rows_before+1}-{rows_before+len(chunk)}, {len(chunk)} rows)")
        if _group_aware_active and len(chunk) > 2 * ROWS_PER_FILE:
            print(f"⚠️  {os.path.basename(output_file)} has {len(chunk)} rows (>2× target {ROWS_PER_FILE}) — one (UUID, task) group is oversized.")
        rows_before += len(chunk)

    # Create split manifest
    manifest = {
        "total_splits": total_files,
        "rows_per_file": ROWS_PER_FILE,
        "total_rows": len(filtered_rows),
        "split_enabled": True,
        "actual_rows_written": actual_rows_written,
        "group_aware": _group_aware_active
    }
    manifest_file = os.path.join(OUTPUT_DIR, "split_manifest.json")
    with open(manifest_file, "w", encoding="utf-8") as mf:
        json.dump(manifest, mf, indent=2)

    # Validate no data loss
    if actual_rows_written != len(filtered_rows):
        print(f"⚠️  WARNING: Row count mismatch! Expected {len(filtered_rows)}, wrote {actual_rows_written}")
    else:
        print(f"✅ Validated: All {actual_rows_written} rows written across {total_files} splits")

    print(f"✅ Created manifest: {manifest_file}")
    print(f"Mode: Split into {total_files} files (~{ROWS_PER_FILE} rows per file)")

# === Summary ===
print(f"\n{'='*70}")
print(f"{'PREPROCESSING SUMMARY':^70}")
print(f"{'='*70}")
# --- This 'total_input_rows' variable is now CORRECT ---
print(f"\nTotal CSV rows: {total_input_rows}") 
print(f"\n{'Filter Stage':<50} {'Removed':<10} {'Remaining'}")
print(f"{'-'*70}")

remaining_after_school = total_input_rows - skip_school_mismatch
if USE_SCHOOL_FILTER:
    print(f"{'School ID not in filter list':<50} {skip_school_mismatch:<10} {remaining_after_school}")
else:
    print(f"{'School ID filtering':<50} {'SKIPPED':<10} {remaining_after_school}")

remaining_after_task = remaining_after_school - skip_task_not_in_questions
print(f"{'Task not found in Question CSV':<50} {skip_task_not_in_questions:<10} {remaining_after_task}")

remaining_after_evidence = remaining_after_task - skip_evidence_null
print(f"{'Task Evidence empty or null':<50} {skip_evidence_null:<10} {remaining_after_evidence}")

remaining_after_invalid = remaining_after_evidence - skip_invalid_evidence
print(f"{'Task Evidence is not valid (not image/pdf/excel)':<50} {skip_invalid_evidence:<10} {remaining_after_invalid}")

remaining_after_type_excluded = remaining_after_invalid - skip_evidence_type_excluded
if ALLOWED_EVIDENCE_TYPES is not None:
    print(f"{'Evidence type excluded by execution filter':<50} {skip_evidence_type_excluded:<10} {remaining_after_type_excluded}")
else:
    print(f"{'Evidence type filtering':<50} {'SKIPPED':<10} {remaining_after_type_excluded}")

print(f"\n{'='*70}")
print(f"Final output CSV rows: {len(filtered_rows)}")
print(f"{'='*70}")

# === Script Checkpoints Section ===
print(f"\n{'='*70}")
print(f"{'SCRIPT CHECKPOINTS':^70}")
print(f"{'='*70}")
print("This script performed the following actions:")

print("\n--- 1. PRE-LOADING ---")
print(f"✅ Loaded valid school codes from '{FILTER_CSV}'")
print(f"✅ Loaded task/question map from '{QUESTION_CSV}'")
print("✅ Defined district name replacements (e.g., 'W. Champaran' -> 'West Champaran')")

print("\n--- 2. MAIN PROCESSING (Row-by-Row) ---")
print(f"✅ Loaded all {total_input_rows} rows from '{INPUT_CSV}' (This is the correct count).")
print("✅ Iterated through all rows with a progress bar.")
print("\n  For EACH row, the following filters were applied (in order):")
print("  ➡️ 1. SKIPPED if 'School ID' was not in the valid school list.")
print("  ➡️ 2. SKIPPED if 'Tasks' value has no matching question in the Question CSV.")
print("  ➡️ 3. SKIPPED if 'Task Evidence' (after cleaning) was empty or 'null'.")
print("  ➡️ 4. SKIPPED if 'Task Evidence' URL was not valid (not image/pdf/excel).")

print("\n  For EACH row that PASSED all filters:")
print("  ➡️ Cleaned and matched 'Tasks' to populate 'Task Evidence Question'.")
print("  ➡️ Cleaned 'District' names (e.g., 'Kaimur (Bhabua)' -> 'Kaimur').")
print("  ➡️ Set the 'Image Preview' column to be empty.")
print("  ➡️ Kept the original 'Task Evidence' value.")
print("  ➡️ Added row to the final output list.")

print("\n--- 3. FINAL OUTPUT ---")
print(f"✅ Wrote {len(filtered_rows)} passed rows to the final CSV file.")
print("✅ Printed the final summary report with skip/remaining counts.")
print(f"{'='*70}")





# import os
# import csv
# import math
# from urllib.parse import urlparse
# from tqdm import tqdm 

# # === Configuration ===
# INPUT_CSV = "/Users/user/Documents/AI/parallel-process/input/017F35E575D87A3FB5ED3D90A3E69355_20250904.csv"
# QUESTION_CSV = "/Users/user/Documents/AI/parallel-process/input/aug_sample_questions.csv"
# FILTER_CSV = "/Users/user/Documents/AI/parallel-process/input/school_list.csv"
# OUTPUT_DIR = "pre_split_csvs"

# # === SPLIT CONFIGURATION ===
# SPLIT_FILES = "no"  # Set to "yes" to split into multiple files, "no" for single file
# ROWS_PER_FILE = 10000  # Only used if SPLIT_FILES = "yes"

# # === IMAGE FORMATS ===
# IMAGE_FORMATS = {".jpg", ".jpeg", ".png", ".gif", ".bmp", ".webp"}

# # Create output directory
# os.makedirs(OUTPUT_DIR, exist_ok=True)

# # Counters for skipped rows
# skip_task_start = 0
# skip_evidence_null = 0
# skip_school_mismatch = 0
# skip_non_image = 0

# # Get total row count for progress bar
# print(f"Calculating total rows in {INPUT_CSV}...")
# try:
#     with open(INPUT_CSV, 'r', encoding="utf-8") as f:
#         # -1 to exclude the header row
#         total_input_rows = sum(1 for _ in f) - 1
# except FileNotFoundError:
#     print(f"Error: INPUT_CSV '{INPUT_CSV}' not found.")
#     exit()
# except Exception as e:
#     print(f"Error reading {INPUT_CSV}: {e}")
#     exit()
# print(f"Found {total_input_rows} data rows to process.")

# # === Step 1: Load FILTER_CSV school codes into a set ===
# valid_school_codes = set()
# with open(FILTER_CSV, newline='', encoding="utf-8") as f:
#     reader = csv.DictReader(f)
#     for row in reader:
#         school_code = row.get("UDISE+ SCHOOL CODE", "").strip()
#         if school_code:
#             valid_school_codes.add(school_code)

# # === Helper function for cleaning cell values ===
# def clean_cell(value):
#     """Strips whitespace AND common quote characters from the ends."""
#     if not isinstance(value, str):
#         return ""
#     # Strip whitespace, then strip both single and double quotes
#     return value.strip().strip("'\"")

# # === Helper function to check if URL is an image ===
# def is_image_url(url):
#     """Check if URL points to an image file"""
#     url = clean_cell(url) # Clean the URL string first for *checking*
#     if not url or url.lower() == "null":
#         return False
#     try:
#         parsed = urlparse(url)
#         path = parsed.path.lower()
#         return any(path.endswith(ext) for ext in IMAGE_FORMATS)
#     except:
#         return False

# # === Step 2: Load QUESTION_CSV into dictionary (TASK NAME → Refined Question) ===
# lookup_dict = {}
# with open(QUESTION_CSV, newline='', encoding="utf-8") as f:
#     reader = csv.DictReader(f)
#     for row in reader:
#         task_name = clean_cell(row.get("TASK NAME", ""))
#         refined_question = row.get("Refined questions using tool and webpage", "").strip()
#         if task_name:  # only add valid rows
#             lookup_dict[task_name] = refined_question

# # === District Renaming Map ===
# DISTRICT_REPLACEMENTS = {
#     "W. Champaran": "West Champaran",
#     "E. Champaran": "East Champaran",
#     "Kaimur (Bhabua)": "Kaimur",
#     "Aurangabad (Bihar)": "Aurangabad"
# }

# # === Step 3: Load INPUT_CSV and filter ===
# filtered_rows = []
# with open(INPUT_CSV, newline='', encoding="utf-8") as infile:
#     reader = csv.DictReader(infile)
    
#     header = reader.fieldnames
#     if header is None:
#         print("Error: CSV Header is empty. Cannot proceed.")
#         exit()

#     # Add new columns
#     new_columns = [
#         "Task Evidence Question",
#         "Task evidence Q and A",
#         "Task evidence Q and A Reason",
#         "Relevance Tag",
#         "Image Preview"
#     ]
    
#     final_header = list(header)
#     for col in new_columns:
#         if col not in final_header:
#             final_header.append(col)

#     # Wrap the reader with tqdm for the progress bar
#     for row in tqdm(reader, total=total_input_rows, desc="Processing input CSV"):
        
#         school_id = row.get("School ID", "").strip()
#         task = clean_cell(row.get("Tasks", "")) # Clean task for lookup
#         evidence = row.get("Task Evidence", "") # Get raw evidence

#         # Rule 0: Skip if School ID not in FILTER_CSV
#         if school_id not in valid_school_codes:
#             skip_school_mismatch += 1
#             continue

#         # Rule 1: Skip if task starts with 1 or 8
#         if task.startswith("1") or task.startswith("8"):
#             skip_task_start += 1
#             continue

#         # Rule 2: Skip if evidence is empty or "null" (after cleaning for check)
#         cleaned_evidence = clean_cell(evidence)
#         if cleaned_evidence == "" or cleaned_evidence.lower() == "null":
#             skip_evidence_null += 1
#             continue

#         # Rule 3: Skip if evidence URL is not an image format
#         if not is_image_url(evidence): # Send the raw evidence to be checked
#             skip_non_image += 1
#             continue

#         # === Step 4: Fill additional columns & Clean District ===
#         row["Task Evidence Question"] = lookup_dict.get(task, "Null")
#         row["Task evidence Q and A"] = ""
#         row["Task evidence Q and A Reason"] = ""
#         row["Relevance Tag"] = ""
#         row["Image Preview"] = ""
        
#         # --- NOTE: The "Task Evidence" column is NO longer overwritten ---

#         # Apply District replacement
#         current_district = row.get("District", "")
#         row["District"] = DISTRICT_REPLACEMENTS.get(current_district, current_district)

#         # Row passes all checks
#         filtered_rows.append([row.get(h, "") for h in final_header])

# # === Step 5: Output - Single file or Multiple files based on configuration ===
# if SPLIT_FILES.lower() == "no":
#     # Single file output
#     output_file = os.path.join(OUTPUT_DIR, "preprocessed_data.csv")
#     with open(output_file, "w", newline='', encoding="utf-8") as outfile:
#         writer = csv.writer(outfile)
#         writer.writerow(final_header)
#         writer.writerows(filtered_rows)
    
#     print(f"✅ Created: {output_file} ({len(filtered_rows)} rows)")
#     print(f"Mode: Single file output")
    
# else:
#     # Split into multiple files
#     total_files = math.ceil(len(filtered_rows) / ROWS_PER_FILE)
    
#     for i in range(total_files):
#         start_index = i * ROWS_PER_FILE
#         end_index = start_index + ROWS_PER_FILE
#         chunk = filtered_rows[start_index:end_index]

#         output_file = os.path.join(OUTPUT_DIR, f"split_{i+1}.csv")
#         with open(output_file, "w", newline='', encoding="utf-8") as outfile:
#             writer = csv.writer(outfile)
#             writer.writerow(final_header)
#             writer.writerows(chunk)

#         print(f"✅ Created: {output_file} ({len(chunk)} rows)")
    
#     print(f"Mode: Split into {total_files} files ({ROWS_PER_FILE} rows per file)")

# # === Summary ===
# print(f"\n{'='*70}")
# print(f"{'PREPROCESSING SUMMARY':^70}")
# print(f"{'='*70}")
# print(f"\nTotal CSV rows: {total_input_rows}")
# print(f"\n{'Filter Stage':<50} {'Removed':<10} {'Remaining'}")
# print(f"{'-'*70}")

# remaining_after_school = total_input_rows - skip_school_mismatch
# print(f"{'School ID not in filter list':<50} {skip_school_mismatch:<10} {remaining_after_school}")

# remaining_after_task = remaining_after_school - skip_task_start
# print(f"{'Task starts with 1 or 8':<50} {skip_task_start:<10} {remaining_after_task}")

# remaining_after_evidence = remaining_after_task - skip_evidence_null
# print(f"{'Task Evidence empty or null':<50} {skip_evidence_null:<10} {remaining_after_evidence}")

# remaining_after_non_image = remaining_after_evidence - skip_non_image
# print(f"{'Task Evidence is not an image (video/other)':<50} {skip_non_image:<10} {remaining_after_non_image}")

# print(f"\n{'='*70}")
# print(f"Final output CSV rows: {len(filtered_rows)}")
# print(f"{'='*70}")

# # === Script Checkpoints Section ===
# print(f"\n{'='*70}")
# print(f"{'SCRIPT CHECKPOINTS':^70}")
# print(f"{'='*70}")
# print("This script performed the following actions:")

# print("\n--- 1. PRE-LOADING ---")
# print(f"✅ Loaded valid school codes from '{FILTER_CSV}'")
# print(f"✅ Loaded task/question map from '{QUESTION_CSV}'")
# print("✅ Defined district name replacements (e.g., 'W. Champaran' -> 'West Champaran')")

# print("\n--- 2. MAIN PROCESSING (Row-by-Row) ---")
# print(f"✅ Iterated through all {total_input_rows} rows in '{INPUT_CSV}' with a progress bar.")
# print("\n  For EACH row, the following filters were applied (in order):")
# print("  ➡️ 1. SKIPPED if 'School ID' was not in the valid school list.")
# print("  ➡️ 2. SKIPPED if 'Tasks' value (after cleaning) started with '1' or '8'.")
# print("  ➡️ 3. SKIPPED if 'Task Evidence' (after cleaning) was empty or 'null'.")
# print("  ➡️ 4. SKIPPED if 'Task Evidence' URL was not an image (e.g., .mp4, .pdf).")

# print("\n  For EACH row that PASSED all filters:")
# print("  ➡️ Cleaned and matched 'Tasks' to populate 'Task Evidence Question'.")
# print("  ➡️ Cleaned 'District' names (e.g., 'Kaimur (Bhabua)' -> 'Kaimur').")
# print("  ➡️ Added some extra rows to the final output list.")

# print("\n--- 3. FINAL OUTPUT ---")
# print(f"✅ Wrote {len(filtered_rows)} passed rows to the final CSV file.")
# print("✅ Printed the final summary report with skip/remaining counts.")
# print(f"{'='*70}")
