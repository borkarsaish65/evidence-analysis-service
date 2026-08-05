import os
import sys
import argparse
import ast
import concurrent.futures
import pandas as pd
import json
import httpx
import base64
import typing_extensions as typing
import time
import mimetypes
import unicodedata
import random
from urllib.request import urlopen
from urllib.parse import urlparse
import re
import logging
import csv
import io  # For BytesIO when processing Excel files from URLs
from pathlib import Path
from dotenv import load_dotenv
# Load .env from service root explicitly so subprocess cwd doesn't matter.
SERVICE_ROOT = Path(__file__).resolve().parents[2]
load_dotenv(dotenv_path=SERVICE_ROOT / ".env")
# Allow importing from the service package (services/, core/, etc.)
if str(SERVICE_ROOT) not in sys.path:
    sys.path.insert(0, str(SERVICE_ROOT))
from core.constants import (
    PROVIDER_GEMINI,
    PROVIDER_OPENROUTER,
    OPENROUTER_MODELS_URL,
    RELEVANCE_TAG_RELEVANT,
    RELEVANCE_TAG_PARTIAL,
    RELEVANCE_TAG_IRRELEVANT,
    RELEVANCE_TAG_NOT_VALIDATED,
    EVIDENCE_TYPE_EXTENSIONS as DEFAULT_EVIDENCE_TYPE_EXTENSIONS,
    DEFAULT_EVIDENCE_COLUMN,
    DEFAULT_INPUT_SCHOOL_ID_COLUMN,
)
from utils.llm_provider import generate_content, _looks_like_placeholder
import threading
import time
from collections import deque
import hashlib


def _parse_args():
    parser = argparse.ArgumentParser(description="Run evidence processor pipeline.")
    parser.add_argument("--input-dir", default=None, help="Input directory containing split/preprocessed CSV files")
    parser.add_argument("--output-dir", default=None, help="Output directory for processed files")
    parser.add_argument("--final-output-file", default=None, help="Final merged output CSV path")
    parser.add_argument("--checkpoint-file", default=None, help="Checkpoint file path")
    parser.add_argument("--api-usage-log-file", default=None, help="API usage log CSV path")
    parser.add_argument("--questions-file", default=None, help="Questions CSV path")
    parser.add_argument("--question-task-column", default=None, help="Task column in questions CSV (from config)")
    parser.add_argument("--question-text-column", default=None, help="Question text column in questions CSV (from config)")
    parser.add_argument("--input-task-column", default=None, help="Task column in input CSV (from config)")
    parser.add_argument(
        "--input-task-question-column",
        default=None,
        help="Input CSV column that holds mapped question text",
    )
    parser.add_argument(
        "--evidence-column",
        default=None,
        help="Evidence-URL column in the input CSV (per-tenant, from "
        "CsvSourceType.evidence_columns[0].column); absent = core.constants default",
    )
    parser.add_argument(
        "--school-id-column",
        default=None,
        help="School-ID column in the input CSV (per-tenant, from "
        "CsvSourceType.column_mappings.geo.school_id); absent = core.constants default",
    )
    parser.add_argument(
        "--identity-column",
        default=None,
        help="Row-identity column in the input CSV, used for resume/dedup and the "
        "relevant-evidence cap (per-tenant, from CsvSourceType.column_mappings.identifier); "
        "absent = core.constants default (\"UUID\")",
    )
    parser.add_argument("--max-processed-rows", type=int, default=None, help="Row cap; <=0 means no cap")
    parser.add_argument(
        "--max-relevant-per-user-task",
        type=int,
        default=None,
        help="Per-(UUID, task) relevant-evidence cap; unset means no cap",
    )
    parser.add_argument(
        "--evidence-types-to-validate",
        default=None,
        help="JSON object mapping evidence type key -> list of file extensions "
        "(per-tenant, from CsvSourceType.evidence_types_config); absent = core.constants default",
    )
    return parser.parse_args()


ARGS = _parse_args()

# === Constants ===
# Per-tenant type->extension map, passed in by execution_processor.py from
# CsvSourceType.evidence_types_config; falls back to the core.constants default when this
# script is run standalone (no execution context to resolve tenant config from).
if ARGS.evidence_types_to_validate:
    EVIDENCE_TYPE_EXTENSIONS = {
        str(key): [str(ext).lower() for ext in exts]
        for key, exts in json.loads(ARGS.evidence_types_to_validate).items()
    }
else:
    EVIDENCE_TYPE_EXTENSIONS = DEFAULT_EVIDENCE_TYPE_EXTENSIONS

# Only IMAGE_FORMATS remains: used for Image Preview rendering. get_evidence_type()
# resolves types dynamically from EVIDENCE_TYPE_EXTENSIONS directly (see below).
IMAGE_FORMATS = set(EVIDENCE_TYPE_EXTENSIONS.get("image", []))

MAX_PROCESSED_ROWS = (
    ARGS.max_processed_rows
    if ARGS.max_processed_rows is not None
    else int(os.getenv("PROCESSOR_MAX_PROCESSED_ROWS", os.getenv("MAX_PROCESSED_ROWS", "0")))
)
INPUT_DIR = ARGS.input_dir or os.getenv("PROCESSOR_INPUT_DIR") or "../pre-processor/parallel_input_split_1_files"
OUTPUT_DIR = ARGS.output_dir or os.getenv("PROCESSOR_OUTPUT_DIR") or "../pre-processor/parallel_output_split_1_files"
FINAL_OUTPUT_FILE = (
    ARGS.final_output_file
    or os.getenv("PROCESSOR_FINAL_OUTPUT_FILE")
    or os.path.join(OUTPUT_DIR, "merged_output_1.csv")
)
CHECKPOINT_FILE = (
    ARGS.checkpoint_file
    or os.getenv("PROCESSOR_CHECKPOINT_FILE")
    or os.path.join(OUTPUT_DIR, ".processing_checkpoint.json")
)
API_USAGE_LOG_FILE = (
    ARGS.api_usage_log_file
    or os.getenv("PROCESSOR_API_USAGE_LOG_FILE")
    or os.path.join(OUTPUT_DIR, "api_usage_log.csv")
)

# === CHECKPOINT CONFIGURATION (from .env) ===
# Always resumes from an existing checkpoint / partial output when present — there is no
# from-scratch mode. A fresh run simply finds no checkpoint and starts with empty state.
CHECKPOINT_SAVE_FREQUENCY = int(os.getenv("CHECKPOINT_SAVE_FREQUENCY", "10"))
CHECKPOINT_CLEANUP_ON_SUCCESS = os.getenv("CHECKPOINT_CLEANUP_ON_SUCCESS", "True").lower() == "true"

# === STATE CONFIGURATION (from .env) ===
STATE_NAME = os.getenv("STATE_NAME", "HARYANA")  # Default: HARYANA

# === QUESTIONS FILE CONFIGURATION (from .env) ===
# Path to the CSV containing standard task names and their evaluation questions.
# Relative to the processor/ directory. Defaults to the Haryana question sheet.
QUESTIONS_FILE = ARGS.questions_file or os.getenv("PROCESSOR_QUESTIONS_FILE") or os.getenv("QUESTIONS_FILE", "../input/question.csv")
QUESTION_TASK_COLUMN = (
    (ARGS.question_task_column or "").strip()
    or (os.getenv("PROCESSOR_QUESTION_TASK_COLUMN", "") or "").strip()
)
QUESTION_TEXT_COLUMN = (
    (ARGS.question_text_column or "").strip()
    or (os.getenv("PROCESSOR_QUESTION_TEXT_COLUMN", "") or "").strip()
)
INPUT_TASK_COLUMN = (
    (ARGS.input_task_column or "").strip()
    or (os.getenv("PROCESSOR_INPUT_TASK_COLUMN", "") or "").strip()
    or "Tasks"
)
INPUT_TASK_QUESTION_COLUMN = (
    (ARGS.input_task_question_column or "").strip()
    or (os.getenv("PROCESSOR_INPUT_TASK_QUESTION_COLUMN", "") or "").strip()
    or "Evidence Question"
)
# Per-tenant column names for the input CSV's evidence-URL and school-ID columns,
# passed in by execution_processor.py from CsvSourceType.evidence_columns /
# column_mappings.geo.school_id (same env-var wiring as PROCESSOR_INPUT_TASK_COLUMN
# above — set once, applies to every subprocess invocation).
EVIDENCE_COLUMN = (
    (ARGS.evidence_column or "").strip()
    or (os.getenv("PROCESSOR_EVIDENCE_COLUMN", "") or "").strip()
    or DEFAULT_EVIDENCE_COLUMN
)
SCHOOL_ID_COLUMN = (
    (ARGS.school_id_column or "").strip()
    or (os.getenv("PROCESSOR_SCHOOL_ID_COLUMN", "") or "").strip()
    or DEFAULT_INPUT_SCHOOL_ID_COLUMN
)
# Row-identity column for resume/dedup and the relevant-evidence cap — "UUID" for CSV
# shapes where one row/UUID maps to one submission (e.g. project_report); a per-submission
# column (e.g. "Observation Submission Id") for shapes where the same UUID legitimately
# recurs across independent submissions. CLI-arg > env-var, same priority as EVIDENCE_COLUMN
# above, but no DEFAULT_IDENTITY_COLUMN fallback: identity is genuinely optional (see
# project_report_no_uuid), so unconfigured means "this CSV shape has none" — not "assume
# UUID". Both consumers below (resume_identity_col, the relevant-cap) already treat an
# IDENTITY_COLUMN absent from the data as "not available" and degrade gracefully.
IDENTITY_COLUMN = (
    (ARGS.identity_column or "").strip()
    or (os.getenv("PROCESSOR_IDENTITY_COLUMN", "") or "").strip()
)
DEFAULT_QUESTION_TASK_COLUMN = "TASK NAME"
DEFAULT_QUESTION_TEXT_COLUMN = "Refined questions using tool and webpage"

# === RELEVANCE SCORING CONFIGURATION ===
# BIHAR: Use "strict" mode (YES/NO answers only, descriptive content ignored)
# HARYANA: Use "mixed" mode (considers both YES/NO and descriptive quality)
# Options: "strict" (Bihar), "mixed" (Haryana), "descriptive" (only descriptive)
RELEVANCE_MODE = os.getenv("RELEVANCE_MODE", "mixed")  # Default: mixed for Haryana

# Thresholds for relevance scoring (configurable per state)
RELEVANT_THRESHOLD = float(os.getenv("RELEVANT_THRESHOLD", "0.7"))  # Score >= 0.7 = Relevant
PARTIALLY_RELEVANT_THRESHOLD = float(os.getenv("PARTIALLY_RELEVANT_THRESHOLD", "0.4"))  # Score >= 0.4 = Partially Relevant

# === RELEVANT-EVIDENCE CAP (per UUID+task; distinct from the scoring thresholds above) ===
# Set per execution by the service via CLI arg only. No env var fallback: the subprocess
# environment is a copy of the service process's own env (see _inject_llm_env(os.environ.copy())
# in execution_processor.py), so a globally-set env var would silently leak a cap into
# executions whose threshold_config explicitly requested none. Once a (UUID, task) pair
# accumulates MAX_RELEVANT_PER_USER_TASK "Relevant" tags, remaining rows for that pair are
# written as "notValidated" with NO API call. Relies on the pre-processor's group-aware
# splitting so each (UUID, task) group stays inside one worker's file. None means no cap,
# all rows processed.
# Mirrors execution_processor.py's `max_relevant > 0` guard — a cap of 0 (or negative)
# would make every row's `>= MAX_RELEVANT_PER_USER_TASK` check trivially true, silently
# capping out every row with zero API calls instead of behaving as "no cap".
MAX_RELEVANT_PER_USER_TASK = (
    ARGS.max_relevant_per_user_task if (ARGS.max_relevant_per_user_task or 0) > 0 else None
)

# === ANSWER FORMAT CONFIGURATION ===
# Set to True for descriptive answers, False for YES/NO answers
USE_DESCRIPTIVE_ANSWERS = os.getenv("USE_DESCRIPTIVE_ANSWERS", True)

# Create output directory if it doesn't exist
os.makedirs(OUTPUT_DIR, exist_ok=True)

# Configure logging
logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s [%(levelname)s] [%(threadName)s] %(message)s",
    handlers=[
        logging.FileHandler(f"{OUTPUT_DIR}/processing.log"),
        logging.StreamHandler()
    ]
)
logger = logging.getLogger(__name__)

# === Thread-safe checkpoint lock ===
checkpoint_lock = threading.Lock()

# === Thread-safe API usage tracking lock ===
api_usage_lock = threading.Lock()

# === API PRICING CONFIGURATION (per 1M tokens) ===
# Gemini 2.0 Flash pricing as of Jan 2026
GEMINI_PRICING = {
    "gemini-2.5-flash-lite": {
        "input_price_per_million": 0.10,
        "output_price_per_million": 0.40,
    },
    "gemini-2.5-flash": {
        "input_price_per_million": 0.15,
        "output_price_per_million": 0.60,
    },
    "gemini-2.0-flash": {
        "input_price_per_million": 0.075,   # $0.075 per 1M input tokens
        "output_price_per_million": 0.30,   # $0.30 per 1M output tokens
    },
    "gemini-1.5-flash": {
        "input_price_per_million": 0.075,
        "output_price_per_million": 0.30,
    },
    "gemini-1.5-pro": {
        "input_price_per_million": 1.25,
        "output_price_per_million": 5.00,
    }
}

GEMINI_MODEL_NAME = os.getenv("GEMINI_MODEL", "gemini-2.0-flash").strip() or "gemini-2.0-flash"

_LLM_PROVIDER_NAME = (os.getenv("LLM_PROVIDER") or PROVIDER_GEMINI).strip().lower()

if _LLM_PROVIDER_NAME == PROVIDER_OPENROUTER:
    OPENROUTER_MODEL_NAME = (os.getenv("OPENROUTER_MODEL") or "google/gemini-2.5-flash-lite").strip()
    LLM_MODEL_NAME = OPENROUTER_MODEL_NAME
elif _LLM_PROVIDER_NAME == PROVIDER_GEMINI:
    LLM_MODEL_NAME = GEMINI_MODEL_NAME
else:
    raise ValueError(f"Unsupported LLM_PROVIDER: {_LLM_PROVIDER_NAME!r}")

_OPENROUTER_PRICING_FETCH_ATTEMPTS = 3
_OPENROUTER_PRICING_FETCH_BACKOFF_SECONDS = 1.0


def _fetch_openrouter_model_pricing(model_name):
    """Fetch live per-token pricing for `model_name` from OpenRouter's /models endpoint.

    Retries a few times to ride out transient network errors. Returns a dict
    shaped like a GEMINI_PRICING entry (price per 1M tokens), or None if the
    model isn't listed or every attempt fails.
    """
    for attempt in range(1, _OPENROUTER_PRICING_FETCH_ATTEMPTS + 1):
        try:
            response = httpx.get(OPENROUTER_MODELS_URL, timeout=10)
            response.raise_for_status()
            for model in response.json().get("data", []):
                if model.get("id") == model_name:
                    pricing = model.get("pricing", {})
                    return {
                        "input_price_per_million": float(pricing.get("prompt", 0)) * 1_000_000,
                        "output_price_per_million": float(pricing.get("completion", 0)) * 1_000_000,
                    }
            logger.warning("openrouter_pricing_not_found  model=%s", model_name)
            return None
        except Exception as exc:
            logger.warning(
                "openrouter_pricing_fetch_failed  model=%s  attempt=%d/%d  error=%s",
                model_name, attempt, _OPENROUTER_PRICING_FETCH_ATTEMPTS, exc,
            )
            if attempt < _OPENROUTER_PRICING_FETCH_ATTEMPTS:
                time.sleep(_OPENROUTER_PRICING_FETCH_BACKOFF_SECONDS * attempt)
    return None


# Resolved once at startup so per-row cost calculations don't re-fetch.
_OPENROUTER_MODEL_PRICING = (
    _fetch_openrouter_model_pricing(LLM_MODEL_NAME)
    if _LLM_PROVIDER_NAME == PROVIDER_OPENROUTER
    else None
)


def _resolve_model_pricing(model_name):
    """Resolve pricing for `model_name` for the active LLM_PROVIDER.

    - openrouter: use pricing fetched live from OpenRouter's /models endpoint.
      If that lookup failed at startup, return None — the caller must record
      the cost as unavailable rather than substituting another model's rates.
    - gemini: look up the static GEMINI_PRICING table (no live pricing API
      exists for Gemini). If `model_name` isn't in the table, return None
      rather than silently substituting gemini-2.0-flash's rates.
    """
    if _LLM_PROVIDER_NAME == PROVIDER_OPENROUTER:
        return _OPENROUTER_MODEL_PRICING
    elif _LLM_PROVIDER_NAME == PROVIDER_GEMINI:
        pricing = GEMINI_PRICING.get(model_name)
        if pricing is None:
            logger.warning("gemini_pricing_not_found  model=%s", model_name)
        return pricing



# ===== CHECKPOINT MANAGEMENT FUNCTIONS =====

def generate_row_hash(row):
    """
    Generate unique hash for a row based on its key fields.
    Uses: School ID + Task + Task Evidence URL
    """
    try:
        school_id = str(row.get(SCHOOL_ID_COLUMN, "")).strip()
        task = str(row.get(INPUT_TASK_COLUMN, "")).strip()
        evidence = str(row.get(EVIDENCE_COLUMN, "")).strip()
        
        # Create unique string
        unique_str = f"{school_id}|{task}|{evidence}"
        
        # Generate hash
        return hashlib.md5(unique_str.encode('utf-8')).hexdigest()
    except Exception as e:
        logger.error(f"Error generating row hash: {e}")
        return None

def load_checkpoint():
    """
    Load checkpoint file if it exists.
    Returns: dict with file-level checkpoint data
    """
    if not os.path.exists(CHECKPOINT_FILE):
        logger.info("[Checkpoint] No existing checkpoint found. Starting fresh.")
        return {}
    
    try:
        with open(CHECKPOINT_FILE, 'r') as f:
            checkpoint_data = json.load(f)
        
        # Calculate statistics
        total_processed = sum(
            len(file_data.get('processed_ids', {})) 
            for file_data in checkpoint_data.values()
        )
        
        logger.info(f"[Checkpoint] ✓ Loaded checkpoint with {total_processed} processed rows across {len(checkpoint_data)} files")
        
        for file_name, file_data in checkpoint_data.items():
            count = len(file_data.get('processed_ids', {}))
            logger.info(f"[Checkpoint]   - {file_name}: {count} rows already processed")
        
        return checkpoint_data
    except Exception as e:
        logger.error(f"[Checkpoint] Error loading checkpoint: {e}. Starting fresh.")
        return {}

def save_checkpoint(checkpoint_data):
    """
    Save checkpoint data to file (thread-safe).
    """
    try:
        with checkpoint_lock:
            # Add metadata
            checkpoint_data['_metadata'] = {
                'last_updated': time.strftime('%Y-%m-%d %H:%M:%S'),
                'total_files': len([k for k in checkpoint_data.keys() if not k.startswith('_')]),
                'total_processed': sum(
                    len(v.get('processed_ids', {})) 
                    for k, v in checkpoint_data.items() 
                    if not k.startswith('_')
                )
            }
            
            # Write to temp file first, then rename (atomic operation)
            temp_file = CHECKPOINT_FILE + '.tmp'
            with open(temp_file, 'w') as f:
                json.dump(checkpoint_data, f, indent=2)
            
            # Atomic rename
            os.replace(temp_file, CHECKPOINT_FILE)
            
    except Exception as e:
        logger.error(f"[Checkpoint] Error saving checkpoint: {e}")

def mark_row_processed(file_name, row_hash, checkpoint_data, row_result=None):
    """
    Mark a row as processed in the checkpoint data.

    Note: the per-(UUID, task) relevant-evidence cap counters are rebuilt on resume by
    re-parsing the partial output CSV directly (see _read_resume_state), not from this
    checkpoint — this function only tracks row-level completion.
    """
    if not row_hash:
        return

    if file_name not in checkpoint_data:
        checkpoint_data[file_name] = {
            'processed_ids': {},
            'started_at': time.strftime('%Y-%m-%d %H:%M:%S'),
            'total_processed': 0
        }

    entry = {
        'status': 'success',
        'timestamp': time.strftime('%Y-%m-%d %H:%M:%S'),
        'result_summary': row_result if row_result else 'processed'
    }
    checkpoint_data[file_name]['processed_ids'][row_hash] = entry

    checkpoint_data[file_name]['total_processed'] = len(checkpoint_data[file_name]['processed_ids'])
    checkpoint_data[file_name]['last_updated'] = time.strftime('%Y-%m-%d %H:%M:%S')

def _read_resume_state(output_dir, input_filename, worker_id, identity_col=IDENTITY_COLUMN):
    """
    Read the partial output CSV for this worker to rebuild resume state.

    identity_col: column used as the per-row identity in processed_keys AND as the cap key
    below — IDENTITY_COLUMN when the input has it, else SCHOOL_ID_COLUMN (the same field
    the old hash-based checkpoint used), so datasets without an identity column still get
    row-skip resume. Callers must use the same identity_col when checking a row against the
    returned processed_keys.

    Returns:
        processed_keys: set of (identity, task, task_evidence) — rows to skip
        relevant_count_per_key: dict of (identity, task) -> int — cap counter state (only
        meaningful when identity_col == IDENTITY_COLUMN; the cap itself is disabled when
        IDENTITY_COLUMN is unavailable)

    Reading the output CSV (instead of the checkpoint JSON) means resume state
    survives service-level reruns: the partial output is already flushed to disk
    progressively, whereas the checkpoint JSON lives only in the temp workspace.
    """
    processed_keys = set()
    relevant_count_dict = {}
    # Output files are named "processed_<stem>.csv" — not "<stem>.csv"
    partial_output = os.path.join(output_dir, f"processed_{input_filename.split('.')[0]}.csv")
    if not os.path.isfile(partial_output):
        return processed_keys, relevant_count_dict
    try:
        needed = {identity_col, SCHOOL_ID_COLUMN, INPUT_TASK_COLUMN, EVIDENCE_COLUMN, "Relevance Tag"}
        df = pd.read_csv(
            partial_output,
            usecols=lambda c: c in needed,
            engine="python",
            on_bad_lines="skip",
        )
        if {identity_col, INPUT_TASK_COLUMN, EVIDENCE_COLUMN}.issubset(df.columns):
            for _, r in df.iterrows():
                ident = str(r[identity_col]).strip()
                task = str(r[INPUT_TASK_COLUMN]).strip()
                url  = str(r[EVIDENCE_COLUMN]).strip()
                if url and url.lower() not in ("nan", "null", "none", ""):
                    processed_keys.add((ident, task, url))
        if {identity_col, INPUT_TASK_COLUMN, "Relevance Tag"}.issubset(df.columns):
            rel_rows = df[df["Relevance Tag"] == RELEVANCE_TAG_RELEVANT]
            for _, r in rel_rows.iterrows():
                key = (str(r[identity_col]).strip(), str(r[INPUT_TASK_COLUMN]).strip())
                relevant_count_dict[key] = relevant_count_dict.get(key, 0) + 1
        if processed_keys or relevant_count_dict:
            logging.info(
                f"[Worker {worker_id}] [Resume] {len(processed_keys)} rows already done "
                f"(identity={identity_col}), {len(relevant_count_dict)} ({identity_col}, {INPUT_TASK_COLUMN}) "
                f"keys with Relevant count — from output CSV"
            )
    except Exception as e:
        logging.warning(
            f"[Worker {worker_id}] Could not read partial output {partial_output}: {e}. Starting fresh."
        )
    return processed_keys, relevant_count_dict


def cleanup_checkpoint():
    """
    Remove checkpoint file after successful completion.
    """
    if CHECKPOINT_CLEANUP_ON_SUCCESS and os.path.exists(CHECKPOINT_FILE):
        try:
            os.remove(CHECKPOINT_FILE)
            logger.info("[Checkpoint] ✓ Checkpoint file cleaned up after successful completion")
        except Exception as e:
            logger.warning(f"[Checkpoint] Could not cleanup checkpoint file: {e}")

# ===== END OF CHECKPOINT FUNCTIONS =====

# ===== API USAGE TRACKING FUNCTIONS =====

def initialize_api_usage_log():
    """
    Initialize API usage log file with headers if it doesn't exist.
    """
    if not os.path.exists(API_USAGE_LOG_FILE):
        with open(API_USAGE_LOG_FILE, 'w', newline='', encoding='utf-8') as f:
            writer = csv.writer(f)
            writer.writerow([
                'Timestamp',
                'Worker_ID',
                'Input_File',
                'Row_Number',
                'School_ID',
                'Task',
                'Evidence_URL',
                'Model_Name',
                'API_Call_Type',
                'Input_Tokens',
                'Output_Tokens',
                'Total_Tokens',
                'Input_Cost_USD',
                'Output_Cost_USD',
                'Total_Cost_USD',
                'Pricing_Status',
                'Status',
                'Error_Message'
            ])
        logging.info(f"[API Usage] Created new API usage log: {API_USAGE_LOG_FILE}")

def log_api_usage(worker_id, input_file, row_number, school_id, task, model_name, 
                  api_call_type, response=None, status='success', error_message='', evidence_url=''):
    """
    Log API usage with token counts and costs to CSV file (thread-safe).
    
    Args:
        worker_id: Worker/thread identifier
        input_file: Input CSV file being processed
        row_number: Row number in the input file
        school_id: School ID from the row
        task: Task name
        model_name: Gemini model name used
        api_call_type: Type of API call (e.g., 'image_analysis', 'pdf_analysis', 'enrollment_analysis')
        response: Gemini API response object (contains usage_metadata)
        status: 'success' or 'failure'
        error_message: Error message if status is 'failure'
        evidence_url: URL of the evidence being processed
    """
    try:
        # Extract token counts from response
        input_tokens = 0
        output_tokens = 0
        total_tokens = 0
        
        if response and hasattr(response, 'usage_metadata'):
            input_tokens = getattr(response.usage_metadata, 'prompt_token_count', 0)
            output_tokens = getattr(response.usage_metadata, 'candidates_token_count', 0)
            total_tokens = getattr(response.usage_metadata, 'total_token_count', 0)
        
        # Calculate costs based on model pricing. If pricing couldn't be resolved
        # (e.g. OpenRouter /models lookup failed for this model), record the
        # cost as unavailable rather than substituting another model's rates.
        pricing = _resolve_model_pricing(model_name)
        if pricing is None:
            input_cost = output_cost = total_cost = 0.0
            pricing_status = 'unavailable'
        else:
            input_cost = (input_tokens / 1_000_000) * pricing["input_price_per_million"]
            output_cost = (output_tokens / 1_000_000) * pricing["output_price_per_million"]
            total_cost = input_cost + output_cost
            pricing_status = 'ok'

        # Thread-safe write to CSV
        with api_usage_lock:
            with open(API_USAGE_LOG_FILE, 'a', newline='', encoding='utf-8') as f:
                writer = csv.writer(f)
                writer.writerow([
                    time.strftime('%Y-%m-%d %H:%M:%S'),
                    worker_id,
                    os.path.basename(input_file),
                    row_number,
                    school_id,
                    task[:50] if task else '',  # Truncate task name to 50 chars
                    evidence_url[:200] if evidence_url else '',  # Truncate URL to 200 chars
                    model_name,
                    api_call_type,
                    input_tokens,
                    output_tokens,
                    total_tokens,
                    f"{input_cost:.6f}",
                    f"{output_cost:.6f}",
                    f"{total_cost:.6f}",
                    pricing_status,
                    status,
                    error_message[:100] if error_message else ''  # Truncate error to 100 chars
                ])
    except Exception as e:
        logging.warning(f"[API Usage] Failed to log API usage: {e}")

def generate_api_usage_summary():
    """
    Generate summary statistics from API usage log.
    Returns dict with summary stats.
    """
    try:
        if not os.path.exists(API_USAGE_LOG_FILE):
            return None
        
        df = pd.read_csv(API_USAGE_LOG_FILE)
        
        if df.empty:
            return None
        
        summary = {
            'total_api_calls': len(df),
            'successful_calls': len(df[df['Status'] == 'success']),
            'failed_calls': len(df[df['Status'] == 'failure']),
            'total_input_tokens': df['Input_Tokens'].sum(),
            'total_output_tokens': df['Output_Tokens'].sum(),
            'total_tokens': df['Total_Tokens'].sum(),
            'total_cost_usd': df['Total_Cost_USD'].astype(float).sum(),
            'avg_input_tokens_per_call': df['Input_Tokens'].mean(),
            'avg_output_tokens_per_call': df['Output_Tokens'].mean(),
            'avg_cost_per_call': df['Total_Cost_USD'].astype(float).mean(),
        }
        
        # Per-model breakdown
        model_breakdown = df.groupby('Model_Name').agg({
            'Input_Tokens': 'sum',
            'Output_Tokens': 'sum',
            'Total_Tokens': 'sum',
            'Total_Cost_USD': lambda x: x.astype(float).sum()
        }).to_dict('index')
        
        summary['model_breakdown'] = model_breakdown
        
        # Per-worker breakdown
        worker_breakdown = df.groupby('Worker_ID').agg({
            'Input_Tokens': 'sum',
            'Output_Tokens': 'sum',
            'Total_Tokens': 'sum',
            'Total_Cost_USD': lambda x: x.astype(float).sum()
        }).to_dict('index')
        
        summary['worker_breakdown'] = worker_breakdown
        
        return summary
        
    except Exception as e:
        logging.error(f"[API Usage] Failed to generate summary: {e}")
        return None

# ===== END OF API USAGE TRACKING FUNCTIONS =====

def _normalize_task_name(name):
    """
    Normalize a task name ONLY for question-lookup purposes (never written to CSV).

    Problem: the same task arrives with many stray-quote / spacing variants:
      "4. 'Conduct Quiz Activity'"  (quote right after number prefix + trailing)
      "4. Conduct Quiz Activity'"   (trailing quote only)
      "4.Conduct Quiz Activity"     (no space after dot)
      " 4. Conduct Quiz Activity  " (extra spaces)

    All of the above must resolve to the SAME lookup key.

    Rules (number prefix is KEPT, only stray quotes / spaces removed):
      1. Strip surrounding whitespace
      2. Strip leading/trailing single & double quotes
      3. Remove stray quote(s) directly after 'N. ' — e.g. "4. '" → "4. "
      4. Strip trailing quotes / periods / whitespace
      5. Collapse consecutive internal spaces to one
      6. Lowercase
    """
    if not isinstance(name, str) or not name:
        return ""
    s = name.strip()
    # Strip quotes from both ends (handles "'Conduct Quiz'" → "Conduct Quiz")
    s = s.strip("'\"")
    # Remove stray quote(s) immediately after a numeric prefix like '4. ' or '4.'
    # e.g. "4. 'Conduct Quiz Activity" → "4. Conduct Quiz Activity"
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
    s = unicodedata.normalize("NFC", s)
    return s.lower()


def _normalize_column_name(name):
    return " ".join(str(name).strip().lower().split())


def _is_valid_column_name(name):
    return bool(re.fullmatch(r"[A-Za-z0-9 _().+\-]+", str(name or "").strip()))


def _resolve_column(
    columns,
    configured_column,
    default_column,
    *,
    label,
    compatibility_fallbacks=None,
):
    normalized_to_actual = {
        _normalize_column_name(col): str(col)
        for col in columns
    }

    configured = (configured_column or "").strip()
    if configured:
        if not _is_valid_column_name(configured):
            logging.warning(
                "[Columns] Invalid configured %s column '%s'. Falling back to default '%s'.",
                label,
                configured,
                default_column,
            )
        else:
            configured_resolved = normalized_to_actual.get(_normalize_column_name(configured))
            if configured_resolved:
                return configured_resolved
            logging.warning(
                "[Columns] Configured %s column '%s' not found. Falling back to default '%s'.",
                label,
                configured,
                default_column,
            )
    else:
        logging.warning(
            "[Columns] Missing configured %s column. Falling back to default '%s'.",
            label,
            default_column,
        )

    default_resolved = normalized_to_actual.get(_normalize_column_name(default_column))
    if default_resolved:
        return default_resolved

    for fallback in compatibility_fallbacks or []:
        fallback_resolved = normalized_to_actual.get(_normalize_column_name(fallback))
        if fallback_resolved:
            logging.warning(
                "[Columns] Default %s column '%s' not found. Using compatibility fallback '%s'.",
                label,
                default_column,
                fallback_resolved,
            )
            return fallback_resolved

    raise KeyError(
        f"Could not resolve {label} column. Configured='{configured or '(missing)'}', "
        f"default='{default_column}', available={list(columns)}"
    )


def load_questions_mapping(questions_file):
    """Load questions mapping and per-task extra-field extraction config from the criteria CSV.

    Returns (questions_map, extra_fields_by_task):
      - questions_map: dict keyed by *normalized* task name (raw form also included as a
        secondary key for exact-match speed) so all stray-quote / number-prefix / whitespace
        variants resolve to the same question.
      - extra_fields_by_task: dict keyed by normalized task name -> list of
        {"field", "description", "type"} dicts, parsed from the optional
        field_name / field_description / value_type columns. A row
        contributes an entry here regardless of whether its question column is empty,
        so a criteria CSV can carry extraction-only rows (no question) for a task.
    """
    questions_map = {}      # normalized key -> question
    questions_map_raw = {}  # raw cleaned key -> question (for exact-match fast path)
    extra_fields_by_task = {}  # normalized task key -> list of field configs
    try:
        df_questions = pd.read_csv(questions_file)
        # Strip stray whitespace from header names (e.g. " field_name") so the raw
        # column-name checks below (has_extraction_columns, row.get("field_name")/
        # ("field_description")/("value_type")) match a padded header the same way
        # the upload-validation side already does — otherwise a padded header disables
        # extraction for the whole run without any error.
        df_questions.columns = [str(col).strip() for col in df_questions.columns]
        task_column = _resolve_column(
            df_questions.columns,
            QUESTION_TASK_COLUMN,
            DEFAULT_QUESTION_TASK_COLUMN,
            label="question-task",
            compatibility_fallbacks=["Tasks"],
        )
        question_column = _resolve_column(
            df_questions.columns,
            QUESTION_TEXT_COLUMN,
            DEFAULT_QUESTION_TEXT_COLUMN,
            label="question-text",
            compatibility_fallbacks=[
                "Refined questions using tool and webpage",
                "QUESTIONS FOR METRICS",
                "Evidence Criteria",
            ],
        )
        has_extraction_columns = {"field_name", "field_description", "value_type"} <= set(df_questions.columns)

        for _, row in df_questions.iterrows():
            task_name_raw = str(row.get(task_column, "")).strip()
            norm_key = _normalize_task_name(task_name_raw)

            if has_extraction_columns and norm_key:
                extraction_field = str(row.get("field_name", "")).strip()
                if extraction_field and extraction_field.lower() not in ("nan", "none"):
                    field_config = {
                        "field": extraction_field,
                        "description": str(row.get("field_description", "")).strip(),
                        "type": (str(row.get("value_type", "")).strip().lower() or "string"),
                    }
                    task_fields = extra_fields_by_task.setdefault(norm_key, [])
                    if any(f["field"] == extraction_field for f in task_fields):
                        logging.warning(
                            "[ExtraFields] Duplicate field_name '%s' for task '%s' — keeping first occurrence",
                            extraction_field, norm_key,
                        )
                    else:
                        task_fields.append(field_config)

            question = str(row.get(question_column, "")).strip()
            if not question or question.lower() in ("nan", "none"):
                logging.debug(f"[Questions] Skipping task with empty question: '{task_name_raw}'")
                continue

            # Exact / lightly-cleaned key
            questions_map_raw[task_name_raw] = question

            # Fully-normalized key (handles all stray-quote / prefix variants)
            if norm_key:
                questions_map[norm_key] = question

        logging.info(f"[Questions] Loaded {len(df_questions)} standard tasks from questions.csv")
        logging.info(
            "[Questions] Using columns: task='%s', question='%s'",
            task_column,
            question_column,
        )
        logging.info(f"[Questions] Unique normalized keys: {len(questions_map)} | raw keys: {len(questions_map_raw)}")
        if extra_fields_by_task:
            logging.info(
                "[ExtraFields] Loaded extraction fields for %d task(s): %s",
                len(extra_fields_by_task),
                {k: [f["field"] for f in v] for k, v in extra_fields_by_task.items()},
            )
    except Exception as e:
        logging.warning(f"Could not load questions file {questions_file}: {e}")

    # Return a single flat dict: raw keys + normalized keys.
    # Normalized keys overwrite raw keys on conflict (more permissive wins).
    combined = {**questions_map_raw, **questions_map}
    return combined, extra_fields_by_task

def _load_tokens_from_env(prefixes: list, label: str) -> list:
    tokens = []
    seen = set()
    for key in sorted(os.environ.keys()):
        if any(key.startswith(p) for p in prefixes):
            val = (os.getenv(key, "") or "").strip()
            if val and val not in seen and not _looks_like_placeholder(val):
                tokens.append(val)
                seen.add(val)
    if not tokens:
        logging.error("[%s] No valid tokens found. (empty/placeholder values were ignored)", label)
    else:
        logging.info("[%s] Loaded %s token(s) from environment", label, len(tokens))
    return tokens


def get_gemini_tokens_from_env():
    return _load_tokens_from_env(["GEMINI_TOKEN", "GEMINI_API_KEY"], "Gemini")


def get_openrouter_tokens_from_env():
    return _load_tokens_from_env(["OPENROUTER_API_KEY_"], "OpenRouter")


if _LLM_PROVIDER_NAME == PROVIDER_OPENROUTER:
    _LLM_TOKENS = get_openrouter_tokens_from_env()
elif _LLM_PROVIDER_NAME == PROVIDER_GEMINI:
    _LLM_TOKENS = get_gemini_tokens_from_env()
else:
    raise ValueError(f"Unsupported LLM_PROVIDER: {_LLM_PROVIDER_NAME!r}")

def _build_generation_config():
    return {
        "temperature": 0.0,
    }


def _parse_model_json_response(response_text):
    """
    Parse model output into JSON object with graceful handling of markdown
    wrappers and extra prose around the JSON payload.
    """
    text = str(response_text or "").strip()
    if not text:
        raise ValueError("Model returned empty response text")

    # Common pattern: fenced markdown JSON block.
    if text.startswith("```"):
        text = re.sub(r"^```(?:json)?\s*", "", text, flags=re.IGNORECASE).strip()
        text = re.sub(r"\s*```$", "", text).strip()

    candidates = [text]
    match = re.search(r"\{.*\}", text, flags=re.DOTALL)
    if match:
        candidates.append(match.group(0))

    list_match = re.search(r"\[.*\]", text, flags=re.DOTALL)
    if list_match:
        candidates.append(list_match.group(0))

    last_error = None
    for candidate in candidates:
        try:
            normalized_candidate = re.sub(r"(?m)(?<!https:)(?<!http:)//.*$", "", candidate)
            normalized_candidate = re.sub(r",\s*([}\]])", r"\1", normalized_candidate)
            parsed = json.loads(normalized_candidate)
            if isinstance(parsed, dict):
                return parsed
            if isinstance(parsed, list):
                return {"answers": parsed, "reasonings": []}
        except Exception as exc:  # noqa: BLE001
            last_error = exc

        try:
            python_candidate = normalized_candidate
            python_candidate = re.sub(r"\bnull\b", "None", python_candidate)
            python_candidate = re.sub(r"\btrue\b", "True", python_candidate, flags=re.IGNORECASE)
            python_candidate = re.sub(r"\bfalse\b", "False", python_candidate, flags=re.IGNORECASE)
            parsed = ast.literal_eval(python_candidate)
            if isinstance(parsed, dict):
                return parsed
            if isinstance(parsed, list):
                return {"answers": parsed, "reasonings": []}
        except Exception as exc:  # noqa: BLE001
            last_error = exc

    # Last-resort extraction from loose text outputs.
    inferred_answers = []
    inferred_reasonings = []
    lower_text = text.lower()
    if "no" in lower_text and ("evidence" in lower_text or "not" in lower_text):
        inferred_answers.append("NO")
    elif text:
        inferred_answers.append(text)
    if text:
        inferred_reasonings.append(text)
    if inferred_answers or inferred_reasonings:
        return {"answers": inferred_answers, "reasonings": inferred_reasonings}

    raise ValueError(f"Failed to parse model JSON response: {last_error}")


def _estimate_question_count(task_evidence_question):
    text = str(task_evidence_question or "").strip()
    if not text:
        return 1

    lines = [line.strip() for line in text.splitlines() if line.strip()]
    if not lines:
        return 1

    numbered = [line for line in lines if re.match(r"^(?:q\s*\d+|\d+[\).:-]|[-*•])\s*", line, flags=re.IGNORECASE)]
    question_like = [line for line in lines if "?" in line]
    count = max(len(numbered), len(question_like), 1)
    return min(count, 25)


def _coerce_to_text_list(value):
    if value is None:
        return []

    if isinstance(value, list):
        return [str(item).strip() for item in value if str(item).strip()]

    text = str(value).strip()
    if not text:
        return []

    if text.startswith("[") and text.endswith("]"):
        for parser in (json.loads, ast.literal_eval):
            try:
                parsed = parser(text)
                if isinstance(parsed, list):
                    return [str(item).strip() for item in parsed if str(item).strip()]
            except Exception:  # noqa: BLE001
                continue

    split_pattern = r"\n+(?=(?:q\s*\d+|\d+[\).:-]|[-*•])\s*)|;\s+"
    parts = [part.strip() for part in re.split(split_pattern, text, flags=re.IGNORECASE) if part.strip()]
    return parts if parts else [text]


def _ensure_required_qa_fields(response_json, expected_questions=1):
    """Normalize and validate required keys expected by downstream logic."""
    if not isinstance(response_json, dict):
        raise ValueError("Model response must be a JSON object")

    answers = response_json.get("answers")
    if answers is None:
        answers = response_json.get("answer")

    reasonings = response_json.get("reasonings")
    if reasonings is None:
        reasonings = response_json.get("reasoning") or response_json.get("reasons")

    answers_list = _coerce_to_text_list(answers)
    reasonings_list = _coerce_to_text_list(reasonings)

    if not answers_list and not reasonings_list:
        raise ValueError("Model response must include answers/reasonings content")

    target_len = max(
        1,
        int(expected_questions or 1),
        len(answers_list),
        len(reasonings_list),
    )
    if len(answers_list) < target_len:
        answers_list.extend([answers_list[-1] if answers_list else ""] * (target_len - len(answers_list)))
    if len(reasonings_list) < target_len:
        reasonings_list.extend([reasonings_list[-1] if reasonings_list else ""] * (target_len - len(reasonings_list)))

    response_json["answers"] = answers_list[:target_len]
    response_json["reasonings"] = reasonings_list[:target_len]
    return response_json


# Token -> timestamp it was marked dead. A dict (not a set) so a dead token can be
# re-admitted to rotation after DEAD_TOKEN_TTL_SECONDS — an unauthorized/401 response can
# be a transient auth-service blip, not necessarily a permanently revoked key, so banning
# it for the rest of the process would otherwise silently shrink the token pool forever.
_dead_tokens: dict[str, float] = {}
_dead_tokens_lock = threading.Lock()
DEAD_TOKEN_TTL_SECONDS = max(1, int(os.getenv("DEAD_TOKEN_TTL_SECONDS", "300")))

def _mark_token_dead(token: str):
    with _dead_tokens_lock:
        _dead_tokens[token] = time.time()
    logging.error("[LLM] token=***** marked_dead  removing from rotation for %ds", DEAD_TOKEN_TTL_SECONDS)

def _dead_tokens_in_cooldown() -> set[str]:
    """Tokens still inside their dead-cooldown window. Caller must hold _dead_tokens_lock."""
    now = time.time()
    return {t for t, marked_at in _dead_tokens.items() if now - marked_at < DEAD_TOKEN_TTL_SECONDS}

# Token -> timestamp it hit a rate-limit (429/quota) response. Separate from _dead_tokens
# (auth failures) and much shorter-lived: a rate limit is expected to clear on its own, so
# the token goes on a brief hold — letting OTHER workers currently pinned to it reroute to
# a different token — rather than being excluded as broken.
_rate_limited_tokens: dict[str, float] = {}
_rate_limited_tokens_lock = threading.Lock()
RATE_LIMIT_COOLDOWN_SECONDS = max(1, int(os.getenv("RATE_LIMIT_COOLDOWN_SECONDS", "60")))

def _mark_token_rate_limited(token: str):
    with _rate_limited_tokens_lock:
        _rate_limited_tokens[token] = time.time()
    logging.warning("[LLM] token=*****  rate_limited  cooling_down for %ds", RATE_LIMIT_COOLDOWN_SECONDS)

def _rate_limited_tokens_in_cooldown() -> set[str]:
    """Tokens still inside their rate-limit cooldown window."""
    now = time.time()
    with _rate_limited_tokens_lock:
        return {t for t, marked_at in _rate_limited_tokens.items() if now - marked_at < RATE_LIMIT_COOLDOWN_SECONDS}

def get_worker_token(worker_id: int | None) -> str:
    with _dead_tokens_lock:
        dead_now = _dead_tokens_in_cooldown()
    cooling_down = _rate_limited_tokens_in_cooldown()
    active = [t for t in _LLM_TOKENS if t not in dead_now and t not in cooling_down]
    if not active:
        logging.error("[LLM] all_tokens_dead_or_cooling_down  falling back to last configured token")
        return _LLM_TOKENS[-1]
    # worker_id defaults to None on process_image/process_pdf/process_excel (callable
    # directly without going through main()'s worker pool) — fall back to worker 1
    # instead of crashing on `None - 1`.
    resolved_worker_id = worker_id if worker_id is not None else 1
    return active[(resolved_worker_id - 1) % len(active)]


# === Gemini Model Setup ===
class AnalysisResponse(typing.TypedDict):
    answers: list[str]
    reasonings: list[str]

if not _LLM_TOKENS:
    raise ValueError(f"[{_LLM_PROVIDER_NAME}] No valid tokens found!")


def _llm_generate(parts, token):
    return generate_content(
        parts,
        api_key=token,
        model_name=LLM_MODEL_NAME,
        generation_config=_build_generation_config(),
    )

# === Extra fields extraction: casts whatever Gemini returned for each criteria-CSV-defined
# field to its declared type. Field definitions (name/description/type) come from
# extra_fields_by_task, built by load_questions_mapping() from the criteria CSV.
def _extract_and_cast_extra_fields(response_json, extra_fields):
    """Pull each configured extra field out of the parsed Gemini response and cast it to
    its declared type. No range/cross-field validation — the extraction_description in the
    criteria CSV is the only guidance given to the model; a missing/unparseable value is
    just recorded as None rather than guessed at."""
    result = {}
    for field_config in extra_fields:
        field_name = field_config["field"]
        raw = response_json.get(field_name)
        if raw is None:
            result[field_name] = None
            continue
        try:
            field_type = field_config["type"]
            if field_type == "int":
                result[field_name] = int(float(raw))
            elif field_type == "float":
                result[field_name] = float(raw)
            else:
                result[field_name] = str(raw)
        except (ValueError, TypeError):
            logging.warning("extra_field_cast_failed  field=%s  raw=%r  type=%s", field_name, raw, field_config.get("type"))
            result[field_name] = None
    return result

# === Utility functions ===
def calculate_relevance_tag(answers, mode=None, question_text=None, reasonings=None):
    """
    Calculate relevance tag based on answers with configurable scoring modes.
    
    Args:
        answers: List of answer strings from Gemini API
        mode: Scoring mode - "strict" (Bihar), "mixed" (Haryana), "descriptive" (only descriptive)
              If None, uses global RELEVANCE_MODE setting
    
    Modes:
        - "strict": Only YES/NO answers matter, descriptive content ignored (for Bihar)
        - "mixed": Both YES/NO and descriptive answers contribute (for Haryana)
        - "descriptive": Only descriptive answers matter, YES/NO ignored
    
    Returns:
        str: 'Relevant', 'Partially Relevant', or 'Irrelevant'
    """
    if not answers or not isinstance(answers, list):
        return RELEVANCE_TAG_IRRELEVANT

    total_answers = len(answers)
    if total_answers == 0:
        return RELEVANCE_TAG_IRRELEVANT

    # Use global mode if not specified
    if mode is None:
        mode = RELEVANCE_MODE

    yes_no_answers = []
    descriptive_answers = []
    reasonings = reasonings if isinstance(reasonings, list) else []

    # Categorize answers
    for answer in answers:
        if answer is None or str(answer).strip() == '':
            continue
        answer_str = str(answer).strip().upper()
        if answer_str in ['YES', 'NO']:
            yes_no_answers.append(answer_str)
        else:
            # Consider it descriptive if it's not just YES/NO
            descriptive_answers.append(str(answer).strip())

    # Calculate scores for each type
    yes_no_score = 0
    descriptive_score = 0

    # Score YES/NO answers
    if yes_no_answers:
        yes_count = sum(1 for answer in yes_no_answers if answer == 'YES')
        yes_no_score = (yes_count / len(yes_no_answers)) if yes_no_answers else 0

    # Score descriptive answers
    if descriptive_answers:
        total_desc_score = 0
        for desc_answer in descriptive_answers:
            # Score based on length and content richness
            length_score = min(len(desc_answer) / 50, 1)  # Max score for 50+ chars

            # Bonus for containing specific educational terms
            education_terms = ['student', 'teacher', 'school', 'class', 'learning',
                             'activity', 'meeting', 'enrollment', 'enrolment',
                             'छात्र', 'शिक्षक', 'विद्यालय', 'कक्षा']  # Added Hindi terms
            term_count = sum(1 for term in education_terms if term.lower() in desc_answer.lower())
            term_score = min(term_count / 3, 1)  # Max score for 3+ terms

            # Avoid very short or generic answers
            if len(desc_answer) < 10:
                total_desc_score += 0.2  # Low score for very short answers
            else:
                total_desc_score += (length_score * 0.6 + term_score * 0.4)

        descriptive_score = total_desc_score / len(descriptive_answers)

    # ============================================================
    # MODE-SPECIFIC SCORING LOGIC
    # ============================================================
    
    if mode == "strict":
        # BIHAR MODE: Only YES/NO answers count
        # Descriptive content is completely ignored
        if yes_no_answers:
            combined_score = yes_no_score
            logging.debug(f"[Relevance-Strict] YES/NO only: {yes_no_score:.2f} (YES: {sum(1 for a in yes_no_answers if a == 'YES')}/{len(yes_no_answers)})")
        else:
            # No YES/NO answers in strict mode = Irrelevant
            combined_score = 0
            logging.debug(f"[Relevance-Strict] No YES/NO answers found, marking as Irrelevant")
    
    elif mode == "descriptive":
        # DESCRIPTIVE MODE: Only descriptive answers count
        # YES/NO answers are ignored
        if descriptive_answers:
            combined_score = descriptive_score
            logging.debug(f"[Relevance-Descriptive] Descriptive only: {descriptive_score:.2f}")
        else:
            # No descriptive answers = Irrelevant
            combined_score = 0
            logging.debug(f"[Relevance-Descriptive] No descriptive answers found, marking as Irrelevant")
    
    else:  # mode == "mixed" (default for Haryana)
        # MIXED MODE: Both YES/NO and descriptive answers contribute
        if yes_no_answers and descriptive_answers:
            # Case 1: Mixed answers - weighted average based on count
            yes_no_weight = len(yes_no_answers) / total_answers
            descriptive_weight = len(descriptive_answers) / total_answers
            combined_score = (yes_no_score * yes_no_weight) + (descriptive_score * descriptive_weight)
            logging.debug(f"[Relevance-Mixed] YES/NO: {yes_no_score:.2f} (weight: {yes_no_weight:.2f}), Descriptive: {descriptive_score:.2f} (weight: {descriptive_weight:.2f}), Combined: {combined_score:.2f}")
        elif yes_no_answers:
            # Case 2: Only YES/NO answers
            combined_score = yes_no_score
            logging.debug(f"[Relevance-Mixed] YES/NO only: {combined_score:.2f}")
        elif descriptive_answers:
            # Case 3: Only descriptive answers
            combined_score = descriptive_score
            logging.debug(f"[Relevance-Mixed] Descriptive only: {combined_score:.2f}")
        else:
            # No valid answers
            combined_score = 0
            logging.debug(f"[Relevance-Mixed] No valid answers found")

    combined_text = " ".join(
        [
            " ".join(descriptive_answers),
            " ".join(str(item).strip() for item in reasonings if str(item).strip()),
        ]
    ).lower()

    negative_markers = [
        "no evidence",
        "not visible",
        "cannot determine",
        "not available",
        "does not show",
        "no table",
        "no graph",
        "no numerical data",
        "no enrollment",
        "no enrolment",
        "not related",
        "unrelated",
        "not relevant",
    ]
    has_negative_signal = any(marker in combined_text for marker in negative_markers)
    if has_negative_signal:
        # Strongly penalize long but explicitly negative narratives.
        combined_score = min(combined_score, 0.2)

    question_lower = str(question_text or "").lower()
    if question_lower:
        question_tokens = set(re.findall(r"[a-zA-Z]{4,}", question_lower))
        answer_tokens = set(re.findall(r"[a-zA-Z]{4,}", combined_text))
        overlap_ratio = (
            (len(question_tokens & answer_tokens) / len(question_tokens))
            if question_tokens else 0.0
        )
        # If descriptive content has very weak lexical overlap with question intent,
        # avoid classifying it as Relevant by length alone.
        if not yes_no_answers and overlap_ratio < 0.15:
            combined_score = min(combined_score, 0.35)

        enrollment_keywords = {"enrollment", "enrolment", "नामांकन"}
        if any(keyword in question_lower for keyword in enrollment_keywords) and has_negative_signal:
            combined_score = min(combined_score, 0.1)

    # Determine relevance tag based on combined score and configurable thresholds
    if combined_score >= RELEVANT_THRESHOLD:
        tag = RELEVANCE_TAG_RELEVANT
    elif combined_score >= PARTIALLY_RELEVANT_THRESHOLD:
        tag = RELEVANCE_TAG_PARTIAL
    else:
        tag = RELEVANCE_TAG_IRRELEVANT
    
    logging.debug(f"[Relevance-{mode.upper()}] Final score: {combined_score:.2f} → Tag: {tag}")
    return tag

# Default matches the old single global bucket (MAX_REQUESTS_PER_MINUTE=2000, shared by
# all tokens) as a conservative per-token starting point — NOT a validated provider quota.
# The real ceiling is account/model-dependent (OpenRouter varies by plan and model,
# Gemini by project tier) and must be tuned per deployment via this env var.
MAX_RPM_PER_TOKEN = max(1, int(os.getenv("MAX_RPM_PER_TOKEN", "2000")))

_token_buckets: dict[str, deque] = {}
_token_locks: dict[str, threading.Lock] = {}
_buckets_init_lock = threading.Lock()

def _ensure_bucket(token: str):
    if token not in _token_buckets:
        with _buckets_init_lock:
            if token not in _token_buckets:          # double-checked under lock
                _token_locks[token]   = threading.Lock()  # lock first — outer guard checks _token_buckets
                _token_buckets[token] = deque()

def rate_limiter(token: str):
    """Block until this specific token is under MAX_RPM_PER_TOKEN. Lock released during sleep."""
    _ensure_bucket(token)
    lock = _token_locks[token]
    dq   = _token_buckets[token]
    while True:
        with lock:
            now = time.time()
            while dq and now - dq[0] > 60:
                dq.popleft()
            if len(dq) < MAX_RPM_PER_TOKEN:
                dq.append(now)
                return
            sleep_time = 61.0 - (now - dq[0])
        if sleep_time > 0:
            jitter = random.uniform(0, 3)
            logging.info("[RateLimiter] token=***** throttled %.1fs (+%.1fs jitter)", sleep_time, jitter)
            time.sleep(sleep_time + jitter)


def process_image(task_evidence_link, task_evidence_question, task_name=None, max_retries=3,
                  worker_id=None, input_file=None, row_number=None, school_id=None, extra_fields=None):
    extra_fields = extra_fields or []
    retries = 0
    worker_token = get_worker_token(worker_id)
    expected_questions = _estimate_question_count(task_evidence_question)
    while retries < max_retries:
        try:
            rate_limiter(worker_token)
            image = httpx.get(task_evidence_link)

            # Flexible prompt that allows both YES/NO and descriptive answers
            prompt = f"""You are an educational evidence validator. Analyze the given image and answer these questions:

{task_evidence_question}

IMPORTANT RESPONSE FORMAT:
- For each question, provide EXACTLY ONE answer in the "answers" array
- Put your reasoning/explanation in the "reasonings" array (NOT in answers)
- The answer can be either:
  1. A clear YES or NO
  2. A detailed descriptive answer (e.g., "The school has organized activities...")
- Return ONLY valid JSON (no markdown, no code fences, no comments)

Example for 1 question:
{{
  "answers": ["YES"],
  "reasonings": ["The image clearly shows relevant evidence"]
}}

DO NOT put both YES/NO and explanation in the answers array!

Focus on:
- Visual evidence in the image
- Relevance to the question
- Quality and clarity of the evidence
- Educational context and completeness"""

            if extra_fields:
                prompt += _build_extra_fields_prompt_suffix(extra_fields)

            response = _llm_generate([
                {"mime_type": "image/jpeg", "data": base64.b64encode(image.content).decode("utf-8")},
                prompt,
            ], token=worker_token)
            response_json = _ensure_required_qa_fields(
                _parse_model_json_response(getattr(response, "text", "")),
                expected_questions=expected_questions,
            )

            # Log API usage
            api_call_type = "extra_fields_analysis" if extra_fields else "image_analysis"
            log_api_usage(
                worker_id=worker_id or "unknown",
                input_file=input_file or "unknown",
                row_number=row_number or 0,
                school_id=school_id or "unknown",
                task=task_name or "unknown",
                model_name=LLM_MODEL_NAME,
                api_call_type=api_call_type,
                response=response,
                status='success',
                evidence_url=task_evidence_link or ""
            )
            
            return response_json
        except Exception as e:
            error_str = str(e).lower()
            if any(k in error_str for k in ["rate limit", "quota", "429", "resource_exhausted"]):
                logging.warning("[LLM] rate_limit_hit  worker=%s  token=*****  error=%s",
                                worker_id, str(e)[:120])
                _mark_token_rate_limited(worker_token)
                previous_token = worker_token
                worker_token = get_worker_token(worker_id)
                if worker_token != previous_token:
                    # Rerouted to a token that isn't currently dead/rate-limited. Still
                    # counts against max_retries — a token pool that keeps reshuffling
                    # (tokens cycling in/out of cooldown) would otherwise loop forever.
                    retries += 1
                    continue
                # No alternative token available (every other token is dead or also
                # cooling down) — fall back to backoff+retry on this same token.
                wait = min(60 * (2 ** retries), 300)
                logging.warning("[LLM] rate_limit_backoff  worker=%s  attempt=%d  backoff=%ds  no_alternate_token=true",
                                worker_id, retries + 1, wait)
                time.sleep(wait)
                retries += 1
            elif any(k in error_str for k in ["401", "unauthorized", "user not found"]):
                logging.error("[LLM] unauthorized  worker=%s  token=*****  marking_dead  error=%s",
                              worker_id, str(e)[:120])
                _mark_token_dead(worker_token)
                with _dead_tokens_lock:
                    all_dead = len(_dead_tokens_in_cooldown()) >= len(_LLM_TOKENS)
                if all_dead:
                    logging.error("[LLM] all_tokens_dead  worker=%s  aborting", worker_id)
                    break
                worker_token = get_worker_token(worker_id)
                retries += 1
            else:
                logging.error(f"[Gemini] Error: {e}")
                retries += 1
    logging.error("[Gemini] Max retries reached.")
    return {"error": "Max retries reached"}


# === Helper function to determine evidence type ===
def get_evidence_type(url):
    """Determine the evidence type from URL by matching its extension against the
    configured EVIDENCE_TYPE_EXTENSIONS map. Checks the URL path first (handles the
    common case of extra query params after the file, e.g. '?w=100'), then falls back
    to the full raw URL string — needed for download-proxy URLs that embed the actual
    filename inside a query value (e.g. '.../download?file=.../photo.jpg'), where the
    path itself ('/download') has no extension at all. Mirrors the pre-processor's
    resolver exactly so a row the pre-processor lets through never resolves to None
    here — a None would leave the per-row accumulator lists misaligned and crash
    _flush_to_csv on length mismatch.
    Returns the type key (e.g. 'image', 'pdf', 'excel', or any tenant-configured key), or
    None if no extension matched."""
    url = str(url).strip()
    if not url or url.lower() == "null":
        return None
    try:
        parsed = urlparse(url)
        path = parsed.path.lower()
        full = url.lower()
        for type_key, extensions in EVIDENCE_TYPE_EXTENSIONS.items():
            if any(path.endswith(ext) or full.endswith(ext) for ext in extensions):
                return type_key
    except Exception:
        pass
    return None


# === Extra-fields prompt suffix (shared between image/pdf/excel processors) ===
# Fields are entirely criteria-CSV-driven (extraction_field/extraction_description/
# extraction_type columns, loaded per-task by load_questions_mapping()) — this function
# just renders whatever fields the caller passes in, it has no knowledge of what any
# specific field means.
def _build_extra_fields_prompt_suffix(extra_fields):
    if not extra_fields:
        return ""
    lines = "\n".join(
        f"- {f['field']} ({f['type']}): {f['description']}" for f in extra_fields
    )
    return f"""

====================================================================================
ADDITIONAL STRUCTURED FIELDS TO EXTRACT
====================================================================================
In addition to answering the questions above, extract the following fields from the
evidence and include them as extra keys in your JSON response:

{lines}

Rules:
- If a value cannot be confidently determined, set it to null. Do NOT guess.
- Treat each field independently — do not reuse the same value across multiple fields
  unless a field's own instructions say to.
====================================================================================
"""


def process_pdf(task_evidence_link, task_evidence_question, task_name=None, max_retries=3,
                worker_id=None, input_file=None, row_number=None, school_id=None, extra_fields=None):
    """Process PDF evidence using Gemini API with usage tracking"""
    extra_fields = extra_fields or []
    retries = 0
    worker_token = get_worker_token(worker_id)
    expected_questions = _estimate_question_count(task_evidence_question)
    while retries < max_retries:
        try:
            rate_limiter(worker_token)
            # Download PDF
            pdf_response = httpx.get(task_evidence_link)
            pdf_data = pdf_response.content

            prompt = f"""You are an educational evidence validator. Analyze the given PDF document and answer these questions:

{task_evidence_question}

IMPORTANT RESPONSE FORMAT:
- For each question, provide EXACTLY ONE answer in the "answers" array
- Put your reasoning/explanation in the "reasonings" array (NOT in answers)
- The answer can be either:
  1. A clear YES or NO
  2. A detailed descriptive answer (e.g., "The school has organized activities...")
- Return ONLY valid JSON (no markdown, no code fences, no comments)

Example for 1 question:
{{
  "answers": ["YES"],
  "reasonings": ["The document clearly shows relevant evidence"]
}}

DO NOT put both YES/NO and explanation in the answers array!

Focus on:
- Content evidence in the document
- Relevance to the question
- Quality and clarity of the evidence
- Educational context and completeness"""

            if extra_fields:
                prompt += _build_extra_fields_prompt_suffix(extra_fields)

            response = _llm_generate([
                {"mime_type": "application/pdf", "data": base64.b64encode(pdf_data).decode("utf-8")},
                prompt,
            ], token=worker_token)
            response_json = _ensure_required_qa_fields(
                _parse_model_json_response(getattr(response, "text", "")),
                expected_questions=expected_questions,
            )

            # Log API usage
            api_call_type = "extra_fields_analysis" if extra_fields else "pdf_analysis"
            log_api_usage(
                worker_id=worker_id or "unknown",
                input_file=input_file or "unknown",
                row_number=row_number or 0,
                school_id=school_id or "unknown",
                task=task_name or "unknown",
                model_name=LLM_MODEL_NAME,
                api_call_type=api_call_type,
                response=response,
                status='success',
                evidence_url=task_evidence_link or ""
            )
            
            return response_json
        except Exception as e:
            error_str = str(e).lower()
            if any(k in error_str for k in ["rate limit", "quota", "429", "resource_exhausted"]):
                logging.warning("[LLM] rate_limit_hit  worker=%s  token=*****  error=%s",
                                worker_id, str(e)[:120])
                _mark_token_rate_limited(worker_token)
                previous_token = worker_token
                worker_token = get_worker_token(worker_id)
                if worker_token != previous_token:
                    # Rerouted to a token that isn't currently dead/rate-limited. Still
                    # counts against max_retries — a token pool that keeps reshuffling
                    # (tokens cycling in/out of cooldown) would otherwise loop forever.
                    retries += 1
                    continue
                # No alternative token available (every other token is dead or also
                # cooling down) — fall back to backoff+retry on this same token.
                wait = min(60 * (2 ** retries), 300)
                logging.warning("[LLM] rate_limit_backoff  worker=%s  attempt=%d  backoff=%ds  no_alternate_token=true",
                                worker_id, retries + 1, wait)
                time.sleep(wait)
                retries += 1
            elif any(k in error_str for k in ["401", "unauthorized", "user not found"]):
                logging.error("[LLM] unauthorized  worker=%s  token=*****  marking_dead  error=%s",
                              worker_id, str(e)[:120])
                _mark_token_dead(worker_token)
                with _dead_tokens_lock:
                    all_dead = len(_dead_tokens_in_cooldown()) >= len(_LLM_TOKENS)
                if all_dead:
                    logging.error("[LLM] all_tokens_dead  worker=%s  aborting", worker_id)
                    break
                worker_token = get_worker_token(worker_id)
                retries += 1
            else:
                logging.error(f"[Gemini] PDF processing error: {e}")
                retries += 1
    logging.error("[Gemini] Max retries reached for PDF processing.")
    return {"error": "Max retries reached"}


def process_excel(task_evidence_link, task_evidence_question, task_name=None, max_retries=3,
                  worker_id=None, input_file=None, row_number=None, school_id=None, extra_fields=None):
    """Process Excel evidence - download and convert to text for Gemini with usage tracking"""
    extra_fields = extra_fields or []
    retries = 0
    worker_token = get_worker_token(worker_id)
    expected_questions = _estimate_question_count(task_evidence_question)
    while retries < max_retries:
        try:
            rate_limiter(worker_token)
            # Download Excel file
            excel_response = httpx.get(task_evidence_link)

            # Read Excel into DataFrame
            df_excel = pd.read_excel(io.BytesIO(excel_response.content))
            excel_text = df_excel.to_string()

            prompt = f"""You are an educational evidence validator. Analyze the following Excel spreadsheet data and answer these questions:

{task_evidence_question}

EXCEL DATA:
{excel_text[:10000]}

IMPORTANT RESPONSE FORMAT:
- For each question, provide EXACTLY ONE answer in the "answers" array
- Put your reasoning/explanation in the "reasonings" array (NOT in answers)
- The answer can be either:
  1. A clear YES or NO
  2. A detailed descriptive answer (e.g., "The evidence clearly meets the criteria")
- Return ONLY valid JSON (no markdown, no code fences, no comments)

Example for 1 question:
{{
  "answers": ["YES"],
  "reasonings": ["The spreadsheet clearly shows relevant evidence"]
}}

DO NOT put both YES/NO and explanation in the answers array!

Focus on:
- Data evidence in the spreadsheet
- Relevance to the question
- Quality and completeness of the data
- Educational context"""

            if extra_fields:
                prompt += _build_extra_fields_prompt_suffix(extra_fields)

            response = _llm_generate([prompt], token=worker_token)
            response_json = _ensure_required_qa_fields(
                _parse_model_json_response(getattr(response, "text", "")),
                expected_questions=expected_questions,
            )

            # Log API usage
            api_call_type = "extra_fields_analysis" if extra_fields else "excel_analysis"
            log_api_usage(
                worker_id=worker_id or "unknown",
                input_file=input_file or "unknown",
                row_number=row_number or 0,
                school_id=school_id or "unknown",
                task=task_name or "unknown",
                model_name=LLM_MODEL_NAME,
                api_call_type=api_call_type,
                response=response,
                status='success',
                evidence_url=task_evidence_link or ""
            )
            
            return response_json
        except Exception as e:
            error_str = str(e).lower()
            if any(k in error_str for k in ["rate limit", "quota", "429", "resource_exhausted"]):
                logging.warning("[LLM] rate_limit_hit  worker=%s  token=*****  error=%s",
                                worker_id, str(e)[:120])
                _mark_token_rate_limited(worker_token)
                previous_token = worker_token
                worker_token = get_worker_token(worker_id)
                if worker_token != previous_token:
                    # Rerouted to a token that isn't currently dead/rate-limited. Still
                    # counts against max_retries — a token pool that keeps reshuffling
                    # (tokens cycling in/out of cooldown) would otherwise loop forever.
                    retries += 1
                    continue
                # No alternative token available (every other token is dead or also
                # cooling down) — fall back to backoff+retry on this same token.
                wait = min(60 * (2 ** retries), 300)
                logging.warning("[LLM] rate_limit_backoff  worker=%s  attempt=%d  backoff=%ds  no_alternate_token=true",
                                worker_id, retries + 1, wait)
                time.sleep(wait)
                retries += 1
            elif any(k in error_str for k in ["401", "unauthorized", "user not found"]):
                logging.error("[LLM] unauthorized  worker=%s  token=*****  marking_dead  error=%s",
                              worker_id, str(e)[:120])
                _mark_token_dead(worker_token)
                with _dead_tokens_lock:
                    all_dead = len(_dead_tokens_in_cooldown()) >= len(_LLM_TOKENS)
                if all_dead:
                    logging.error("[LLM] all_tokens_dead  worker=%s  aborting", worker_id)
                    break
                worker_token = get_worker_token(worker_id)
                retries += 1
            else:
                logging.error(f"[Gemini] Excel processing error: {e}")
                retries += 1
    logging.error("[Gemini] Max retries reached for Excel processing.")
    return {"error": "Max retries reached"}


# === Incremental CSV flush helper ===
def _flush_to_csv(df_slice, qa, reason, tags, types, extra_data, output_file, write_header, worker_id):
    """
    Append a completed batch of rows to the output CSV.

    Strategy (fault-tolerant):
      1. Write the batch fully into a per-worker temp file (.batch_tmp).
      2. Read the temp file and append its lines to the real output file.
      3. Delete the temp file.

    This means the real output file only ever receives *complete* batches.
    Even if the process is killed mid-step-2 the worst case is a single
    incomplete line at the very end, which is detected and trimmed on resume.
    The checkpoint is saved right AFTER this function returns so that on
    resume those rows are skipped and not duplicated.
    """
    if df_slice.empty:
        return

    df_out = df_slice.copy()
    df_out["Evidence Q and A"] = list(qa)
    df_out["Evidence Q and A Reason"] = list(reason)
    df_out["Relevance Tag"] = list(tags)
    df_out["Task Type"] = list(types)
    for key_name, vals in extra_data.items():
        df_out[key_name] = list(vals)
    df_out["Image Preview"] = df_out[EVIDENCE_COLUMN].apply(
        lambda x: str(x) if str(x).lower().endswith(tuple(IMAGE_FORMATS)) else ""
    )

    tmp_path = output_file + ".batch_tmp"
    try:
        # Write complete batch to temp file
        df_out.to_csv(tmp_path, index=False)

        with open(tmp_path, 'r', encoding='utf-8', errors='replace') as f_tmp:
            lines = f_tmp.readlines()

        if not lines:
            return

        # First write → include header; subsequent writes → skip header line
        open_mode = 'w' if write_header else 'a'
        with open(output_file, open_mode, encoding='utf-8') as f_out:
            if write_header:
                f_out.writelines(lines)          # header + data rows
            else:
                f_out.writelines(lines[1:])      # data rows only

        logging.info(
            f"[Worker {worker_id}] [Flush] Wrote {len(df_out)} rows → "
            f"{os.path.basename(output_file)} ({'header' if write_header else 'append'})"
        )
    except Exception as exc:
        logging.error(f"[Worker {worker_id}] [Flush] Failed to write batch: {exc}")
    finally:
        if os.path.exists(tmp_path):
            try:
                os.remove(tmp_path)
            except Exception:
                pass


def _trim_incomplete_last_line(output_file, worker_id):
    """
    If the output CSV ends with an incomplete (partial) line caused by a
    mid-write crash, remove that line so the file stays valid on resume.
    """
    if not os.path.exists(output_file):
        return
    try:
        with open(output_file, 'rb') as f:
            content = f.read()
        if not content:
            return
        # A complete CSV line ends with '\n'. If the file doesn't end with '\n'
        # the last line is incomplete — truncate back to the previous newline.
        if not content.endswith(b'\n'):
            last_newline = content.rfind(b'\n')
            if last_newline != -1:
                with open(output_file, 'wb') as f:
                    f.write(content[:last_newline + 1])
                logging.warning(
                    f"[Worker {worker_id}] [Resume] Trimmed incomplete last line from "
                    f"{os.path.basename(output_file)}"
                )
    except Exception as exc:
        logging.error(f"[Worker {worker_id}] [Resume] Could not trim incomplete line: {exc}")


# === Main processing ===
def main(input_file, worker_id=None, checkpoint_data=None):
    try:
        logging.info(f"[Worker {worker_id}] Starting processing for {input_file}")

        if not os.path.exists(input_file):
            logging.error(f"[Worker {worker_id}] File not found: {input_file}")
            return None
        
        # Get base filename for checkpoint tracking
        input_filename = os.path.basename(input_file)
        
        # Initialize checkpoint for this file if not exists
        if checkpoint_data is None:
            checkpoint_data = {}
        
        if input_filename not in checkpoint_data:
            checkpoint_data[input_filename] = {
                'processed_ids': {},
                'started_at': time.strftime('%Y-%m-%d %H:%M:%S'),
                'total_processed': 0
            }
        
        # Track checkpoint stats
        rows_skipped_from_checkpoint = 0
        rows_processed_new = 0

        # Load questions mapping + per-task extra-field extraction config (both from the
        # same criteria CSV — see load_questions_mapping()).
        questions_file = QUESTIONS_FILE
        questions_map, extra_fields_by_task = load_questions_mapping(questions_file)
        all_extra_field_names = sorted({
            f["field"] for fields in extra_fields_by_task.values() for f in fields
        })

        df = pd.read_excel(input_file) if input_file.endswith(".xlsx") else pd.read_csv(input_file)

        required_input_columns = [EVIDENCE_COLUMN, INPUT_TASK_COLUMN, INPUT_TASK_QUESTION_COLUMN]
        missing_input_columns = [column for column in required_input_columns if column not in df.columns]
        if missing_input_columns:
            raise KeyError(
                f"Missing required input columns: {missing_input_columns}. "
                f"Detected columns: {list(df.columns)}"
            )

        # Filter: Keep rows with evidence, but allow null mapped-question column for user-owned tasks.
        df_filtered = df[
            ~df[EVIDENCE_COLUMN].isin([None, "Null"])
        ].dropna(subset=[EVIDENCE_COLUMN])

        # Don't filter out rows with null mapped-question value - they might be user-owned tasks.
        logging.info(f"[Worker {worker_id}] Total rows after filtering: {len(df_filtered)}")

        # ===== RESUME STATE: read partial output CSV once, use for both row-skip and cap rebuild =====
        # Using the output CSV (not the checkpoint JSON) means state survives service-level reruns:
        # the partial output is flushed to disk progressively; the checkpoint JSON is ephemeral.
        # Identity column for row-skip matching: IDENTITY_COLUMN (per-tenant, from
        # CsvSourceType.column_mappings.identifier) when the input has it, else fall back to
        # SCHOOL_ID_COLUMN (what the old hash-based checkpoint keyed on) so datasets without
        # an identity column still get basic resume protection instead of silently
        # reprocessing everything.
        resume_identity_col = IDENTITY_COLUMN if IDENTITY_COLUMN in df_filtered.columns else SCHOOL_ID_COLUMN
        resume_processed_keys, resume_relevant_counts = _read_resume_state(
            OUTPUT_DIR, input_filename, worker_id, identity_col=resume_identity_col
        )

        # ===== CHECKPOINT: Filter out already-processed rows BEFORE processing =====
        if resume_processed_keys:
            def _already_done(row):
                return (
                    str(row.get(resume_identity_col, "")).strip(),
                    str(row.get(INPUT_TASK_COLUMN, "")).strip(),
                    str(row.get(EVIDENCE_COLUMN, "")).strip(),
                ) in resume_processed_keys
            mask = df_filtered.apply(_already_done, axis=1)
            rows_skipped_from_checkpoint = int(mask.sum())
            df_filtered = df_filtered[~mask].copy()
            df_filtered.reset_index(drop=True, inplace=True)
            if rows_skipped_from_checkpoint > 0:
                logging.info(f"[Worker {worker_id}] [Resume] Skipping {rows_skipped_from_checkpoint} already-processed rows (output-CSV state)")

        # ===== RELEVANT CAP: per-(IDENTITY_COLUMN, task) counters for this worker's file =====
        # Per-worker scope is correct because group-aware splitting keeps each
        # (IDENTITY_COLUMN, task) pair inside a single file. Disabled gracefully when the
        # input lacks the identity column.
        is_relevant_limit_enabled = MAX_RELEVANT_PER_USER_TASK is not None
        if is_relevant_limit_enabled and IDENTITY_COLUMN not in df_filtered.columns:
            logging.warning(f"[Worker {worker_id}] relevant_cap_disabled reason=missing_identity_column column={IDENTITY_COLUMN} file={input_filename}")
            is_relevant_limit_enabled = False
        relevant_count_per_key = dict(resume_relevant_counts) if is_relevant_limit_enabled else {}
        not_validated_count = 0
        # AI success = the AI returned a usable response (Relevant/Partial/Irrelevant all
        # count — a real verdict, not a failure). AI failure = no usable response at all
        # (see the "Invalid response" branch below). Counted directly at the point each
        # outcome is known, same as the other per-row counters here.
        ai_success_count = 0
        ai_failure_count = 0
        success_list = []
        failed_list = []
        if is_relevant_limit_enabled and relevant_count_per_key:
            logging.info(f"[Worker {worker_id}] [Resume] Restored Relevant counts for {len(relevant_count_per_key)} ({IDENTITY_COLUMN}, task) groups from output CSV")
        if is_relevant_limit_enabled:
            logging.info(f"[Worker {worker_id}] Relevant cap ENABLED: max {MAX_RELEVANT_PER_USER_TASK} Relevant per ({IDENTITY_COLUMN}, task)")

        # Add extra field columns (criteria-CSV-driven) if any task defines them
        for key_name in all_extra_field_names:
            if key_name not in df_filtered.columns:
                df_filtered[key_name] = ""
                logging.info("[Worker %s] Added extra field column: %s", worker_id, key_name)

        processed_count = 0
        task_evidence_qa = []
        task_evidence_qa_reason = []
        relevance_tags = []
        task_types = []  # Track if task is standard or user-owned

        # Initialize extra field accumulator columns
        extra_keys_data = {key: [] for key in all_extra_field_names}

        # ===== INCREMENTAL CSV WRITE SETUP =====
        output_filename = os.path.join(
            OUTPUT_DIR,
            f"processed_{os.path.basename(input_file).split('.')[0]}.csv"
        )
        # On resume an existing partial file is already present; new rows must be
        # appended (no header).  On a fresh run we write the header first.
        # Based on the output file's own existence/content, not on whether this run's
        # resume-key matching happened to find rows to skip — those can diverge (e.g. a
        # rerun whose current row set doesn't overlap the previous partial output), and
        # treating "0 rows skipped" as "no prior output" would make the first flush open
        # the file in write mode and truncate real prior results.
        csv_header_written = os.path.exists(output_filename) and os.path.getsize(output_filename) > 0
        if csv_header_written:
            # Trim any incomplete trailing line left by a previous crash
            _trim_incomplete_last_line(output_filename, worker_id)
            logging.info(
                f"[Worker {worker_id}] [Resume] Appending to existing partial output: "
                f"{os.path.basename(output_filename)}"
            )
        last_flushed = 0      # index into the accumulator lists of the last flushed row
        FLUSH_EVERY = 10      # write a batch after this many newly processed rows

        for idx, row in df_filtered.iterrows():
            # ===== CHECKPOINT: Generate row hash for marking as processed =====
            row_hash = generate_row_hash(row)
            
            # ===== PROCESS ROW (all rows here need processing) =====
            task_evidence = str(row[EVIDENCE_COLUMN]).strip()
            task_question_raw = row.get(INPUT_TASK_QUESTION_COLUMN, "")
            task_question = str(task_question_raw).strip() if pd.notna(task_question_raw) and task_question_raw != "Null" else ""
            task_name_raw = str(row.get(INPUT_TASK_COLUMN, "")).strip()
            school_id = str(row.get(SCHOOL_ID_COLUMN, "unknown")).strip()

            # Normalize task name for matching (handles all stray-quote / prefix variants)
            task_name = _normalize_task_name(task_name_raw)

            # Check if task is user-owned (not in questions mapping)
            # Try raw key first, then fully-normalized key
            is_user_owned = (task_name_raw not in questions_map and task_name not in questions_map)

            # If no question found in the question sheet, skip evaluation entirely.
            if not task_question:
                logging.info(f"[Worker {worker_id}] SKIPPING row {idx+1} — no question found for task: '{task_name_raw}'")
                task_types.append("User-Owned")
                task_evidence_qa.append(None)
                task_evidence_qa_reason.append(None)
                relevance_tags.append(None)
                for key in all_extra_field_names:
                    extra_keys_data[key].append(None)
                processed_count += 1  # must count skipped rows so list lengths stay aligned

                # Flush batch to CSV if threshold reached (skip path)
                if (processed_count - last_flushed) >= FLUSH_EVERY:
                    _flush_to_csv(
                        df_filtered.iloc[last_flushed:processed_count],
                        task_evidence_qa[last_flushed:processed_count],
                        task_evidence_qa_reason[last_flushed:processed_count],
                        relevance_tags[last_flushed:processed_count],
                        task_types[last_flushed:processed_count],
                        {k: v[last_flushed:processed_count] for k, v in extra_keys_data.items()},
                        output_filename, not csv_header_written, worker_id
                    )
                    csv_header_written = True
                    last_flushed = processed_count
                    save_checkpoint(checkpoint_data)

                continue

            # ===== RELEVANT CAP: once an (IDENTITY_COLUMN, task) group hit the cap, mark and skip =====
            # Computed once per row and reused by the success-path increment below. Mirrors
            # the "no question" skip path above to keep the parallel output lists aligned
            # (one append per list + processed_count += 1), but makes no API call.
            # A blank/NaN identity value must not become a shared relevant_evidence_cap_key —
            # str(nan) == "nan", which is truthy, so rows with a missing identity value would
            # otherwise all be grouped under the same ("nan", task) key and capped together
            # even though they belong to different users/submissions.
            _row_identity = str(row.get(IDENTITY_COLUMN, "")).strip()
            relevant_evidence_cap_key = (_row_identity, task_name_raw) if is_relevant_limit_enabled and _row_identity.lower() not in ("nan", "null", "none", "") else None
            if relevant_evidence_cap_key and relevant_count_per_key.get(relevant_evidence_cap_key, 0) >= MAX_RELEVANT_PER_USER_TASK:
                logging.info(f"[Worker {worker_id}] Row {idx+1} — Relevant cap reached for ({IDENTITY_COLUMN}, task); marking notValidated")
                task_types.append("Capped")
                task_evidence_qa.append(None)
                task_evidence_qa_reason.append(None)
                relevance_tags.append(RELEVANCE_TAG_NOT_VALIDATED)
                for key in all_extra_field_names:
                    extra_keys_data[key].append(None)
                not_validated_count += 1
                mark_row_processed(input_filename, row_hash, checkpoint_data, RELEVANCE_TAG_NOT_VALIDATED)
                processed_count += 1

                # Flush batch to CSV if threshold reached (capped path)
                if (processed_count - last_flushed) >= FLUSH_EVERY:
                    _flush_to_csv(
                        df_filtered.iloc[last_flushed:processed_count],
                        task_evidence_qa[last_flushed:processed_count],
                        task_evidence_qa_reason[last_flushed:processed_count],
                        relevance_tags[last_flushed:processed_count],
                        task_types[last_flushed:processed_count],
                        {k: v[last_flushed:processed_count] for k, v in extra_keys_data.items()},
                        output_filename, not csv_header_written, worker_id
                    )
                    csv_header_written = True
                    last_flushed = processed_count
                    save_checkpoint(checkpoint_data)

                continue

            if idx == 0 or idx % 10 == 0:  # Log every 10th row for debugging
                logging.debug(f"[Worker {worker_id}] Task name: '{task_name_raw}' -> normalized: '{task_name}' -> {'USER-OWNED' if is_user_owned else 'STANDARD'}")

            task_types.append("User-Owned" if is_user_owned else "Standard")

            # Determine evidence type and route to appropriate processor.
            # Evidence-type filtering is enforced upstream by the pre-processor — rows with
            # a disallowed evidence_type are dropped there and never reach this script.
            evidence_type = get_evidence_type(task_evidence)
            if evidence_type:
                logging.info(f"[Worker {worker_id}] Processing {evidence_type} {'user-owned' if is_user_owned else 'standard'} task row {idx+1}/{len(df_filtered)}")

                row_extra_fields = extra_fields_by_task.get(task_name, [])

                # Route to appropriate processor based on evidence type
                if evidence_type == "image":
                    response = process_image(
                        task_evidence, task_question, task_name_raw,
                        worker_id=worker_id, input_file=input_file,
                        row_number=idx+1, school_id=school_id, extra_fields=row_extra_fields
                    )
                elif evidence_type == "pdf":
                    response = process_pdf(
                        task_evidence, task_question, task_name_raw,
                        worker_id=worker_id, input_file=input_file,
                        row_number=idx+1, school_id=school_id, extra_fields=row_extra_fields
                    )
                elif evidence_type == "excel":
                    response = process_excel(
                        task_evidence, task_question, task_name_raw,
                        worker_id=worker_id, input_file=input_file,
                        row_number=idx+1, school_id=school_id, extra_fields=row_extra_fields
                    )
                else:
                    response = None
                if isinstance(response, dict) and "answers" in response and "reasonings" in response:
                    answers = response["answers"]
                    reasonings = response["reasonings"]
                    task_evidence_qa.append(answers)
                    task_evidence_qa_reason.append(reasonings)
                    relevance_tag = calculate_relevance_tag(
                        answers,
                        question_text=task_question,
                        reasonings=reasonings,
                    )
                    relevance_tags.append(relevance_tag)
                    ai_success_count += 1
                    success_list.append(task_evidence)

                    # Count this Relevant hit toward the per-(UUID, task) cap so later rows
                    # of the same pair are capped once the limit is reached.
                    if relevant_evidence_cap_key and relevance_tag == RELEVANCE_TAG_RELEVANT:
                        relevant_count_per_key[relevant_evidence_cap_key] = relevant_count_per_key.get(relevant_evidence_cap_key, 0) + 1

                    # ===== CHECKPOINT: Mark row as processed =====
                    rows_processed_new += 1
                    mark_row_processed(input_filename, row_hash, checkpoint_data, relevance_tag)

                    # Extract this task's configured extra fields (if any) from the JSON response
                    extracted = _extract_and_cast_extra_fields(response, row_extra_fields)
                    for key in all_extra_field_names:
                        extra_keys_data[key].append(extracted.get(key))
                else:
                    logging.warning(f"[Worker {worker_id}] Invalid response at row {idx+1}")
                    task_evidence_qa.append(None)
                    task_evidence_qa_reason.append(None)
                    relevance_tags.append(RELEVANCE_TAG_IRRELEVANT)
                    task_types[-1] = "Failed"  # Update the last task type
                    ai_failure_count += 1
                    failed_list.append(task_evidence)
                    for key in all_extra_field_names:
                        extra_keys_data[key].append(None)
            # No final else: the pre-processor's Rule 3 already drops any row whose evidence
            # type can't be resolved at all, so evidence_type is never falsy here for rows
            # reaching this point through the normal pre-processor -> processor pipeline.

            processed_count += 1

            # ===== INCREMENTAL CSV FLUSH (every FLUSH_EVERY rows) =====
            if (processed_count - last_flushed) >= FLUSH_EVERY:
                _flush_to_csv(
                    df_filtered.iloc[last_flushed:processed_count],
                    task_evidence_qa[last_flushed:processed_count],
                    task_evidence_qa_reason[last_flushed:processed_count],
                    relevance_tags[last_flushed:processed_count],
                    task_types[last_flushed:processed_count],
                    {k: v[last_flushed:processed_count] for k, v in extra_keys_data.items()},
                    output_filename, not csv_header_written, worker_id
                )
                csv_header_written = True
                last_flushed = processed_count
                # Save checkpoint right after writing so resume skips these rows
                save_checkpoint(checkpoint_data)

            if MAX_PROCESSED_ROWS > 0 and processed_count >= MAX_PROCESSED_ROWS:
                logging.info(f"[Worker {worker_id}] Reached max processed rows ({MAX_PROCESSED_ROWS})")
                break


        # ===== FLUSH REMAINING ROWS (final partial batch) =====
        if last_flushed < processed_count:
            _flush_to_csv(
                df_filtered.iloc[last_flushed:processed_count],
                task_evidence_qa[last_flushed:processed_count],
                task_evidence_qa_reason[last_flushed:processed_count],
                relevance_tags[last_flushed:processed_count],
                task_types[last_flushed:processed_count],
                {k: v[last_flushed:processed_count] for k, v in extra_keys_data.items()},
                output_filename, not csv_header_written, worker_id
            )
            csv_header_written = True
            last_flushed = processed_count

        # ===== CHECKPOINT: Final save for this file =====
        save_checkpoint(checkpoint_data)
        logging.info(
            f"[Worker {worker_id}] [Checkpoint] Final save — "
            f"Total: {rows_skipped_from_checkpoint + rows_processed_new} rows "
            f"({rows_skipped_from_checkpoint} from checkpoint, {rows_processed_new} newly processed)"
        )

        logging.info(f"[Worker {worker_id}] Finished processing {input_file}. Output: {output_filename}")

        # Build in-memory df for stats and user-owned reporting only
        # (CSV is already fully written; no second to_csv call needed)
        df_filtered = df_filtered.head(processed_count).copy()
        df_filtered["Evidence Q and A"] = task_evidence_qa
        df_filtered["Evidence Q and A Reason"] = task_evidence_qa_reason
        df_filtered["Relevance Tag"] = relevance_tags
        df_filtered["Task Type"] = task_types
        for key_name, values in extra_keys_data.items():
            df_filtered[key_name] = values
        df_filtered["Image Preview"] = df_filtered[EVIDENCE_COLUMN].apply(
            lambda x: str(x) if str(x).lower().endswith(tuple(IMAGE_FORMATS)) else ""
        )
        df_to_save = df_filtered  # already written to CSV; used only for stats below

        # ===== SAFETY NET: ensure the output file exists even when nothing was ever flushed =====
        # _flush_to_csv is only called from inside the per-row loop above, so if every row was
        # excluded before reaching it (e.g. an evidence-type filter leaves 0 rows for this split),
        # output_filename is never created and the Main merge step's pd.read_csv(f) crashes with
        # FileNotFoundError. df_to_save always has the right columns even with 0 rows, so writing
        # it here (header-only in that case) keeps the merge step working unconditionally.
        if not os.path.exists(output_filename):
            df_to_save.to_csv(output_filename, index=False)
            logging.info(f"[Worker {worker_id}] No rows reached the flush step — wrote header-only output: {output_filename}")

        # Separate user-owned tasks for reporting
        user_owned_df = df_to_save[df_to_save["Task Type"] == "User-Owned"]
        if not user_owned_df.empty:
            user_owned_filename = os.path.join(OUTPUT_DIR, f"user_owned_tasks_{os.path.basename(input_file).split('.')[0]}.csv")
            user_owned_df.to_csv(user_owned_filename, index=False)
            logging.info(f"[Worker {worker_id}] User-owned tasks saved to: {user_owned_filename}")
            return {
                "output_file": output_filename,
                "user_owned_file": user_owned_filename,
                "rows_attempted": processed_count,
                "api_calls": rows_processed_new,  # Only count new API calls
                # notValidated rows (relevant-cap reached) made no API call — exclude them
                # from success/failure so these stay scoped to rows actually sent to the AI.
                "api_successes": ai_success_count,
                "api_failures": ai_failure_count,
                "success_list": success_list,
                "failed_list": failed_list,
                "user_owned_count": len(user_owned_df),
                "standard_count": len(df_to_save) - len(user_owned_df),
                "checkpoint_skipped": rows_skipped_from_checkpoint,
                "checkpoint_new": rows_processed_new,
                "not_validated_count": not_validated_count
            }
        else:
            return {
                "output_file": output_filename,
                "rows_attempted": processed_count,
                "api_calls": rows_processed_new,  # Only count new API calls
                # notValidated rows (relevant-cap reached) made no API call — exclude them
                # from success/failure so these stay scoped to rows actually sent to the AI.
                "api_successes": ai_success_count,
                "api_failures": ai_failure_count,
                "success_list": success_list,
                "failed_list": failed_list,
                "user_owned_count": 0,
                "standard_count": len(df_to_save),
                "checkpoint_skipped": rows_skipped_from_checkpoint,
                "checkpoint_new": rows_processed_new,
                "not_validated_count": not_validated_count
            }


    except Exception as e:
        logging.exception(f"[Worker {worker_id}] Failed to process {input_file}: {e}")
        # ===== CHECKPOINT: Save on error too =====
        if checkpoint_data:
            save_checkpoint(checkpoint_data)
            logging.info(f"[Worker {worker_id}] [Checkpoint] Saved progress before error exit")
        return None


def process_file_parallel(file_path, worker_id, checkpoint_data=None):
    os.makedirs(OUTPUT_DIR, exist_ok=True)
    result = main(file_path, worker_id, checkpoint_data)  # Get stats dictionary
    if result and isinstance(result, dict):
        logging.info(f"[Worker {worker_id}] Output saved as {result['output_file']}")
        if 'user_owned_file' in result:
            logging.info(f"[Worker {worker_id}] User-owned tasks saved as {result['user_owned_file']}")
    else:
        logging.warning(f"[Worker {worker_id}] Processing failed for {file_path}")
    return result  # Return the entire stats dictionary (or None)

# === Entry point ===
if __name__ == "__main__":
    if not os.path.isdir(INPUT_DIR):
        logging.error(f"[Main] Input directory does not exist: {INPUT_DIR}")
        raise SystemExit(1)

    # Read input files
    input_files = [
        os.path.join(INPUT_DIR, file)
        for file in os.listdir(INPUT_DIR)
        if file.endswith((".xlsx", ".csv"))
    ]
    
    # Sort input files for consistent processing order
    # For split files (split_001.csv, split_002.csv), this ensures proper ordering
    input_files.sort()

    logging.info(f"[Main] Found {len(input_files)} input files to process.")
    if not input_files:
        logging.error(f"[Main] No input CSV/XLSX files found in: {INPUT_DIR}")
        raise SystemExit(1)
    
    # Check for split manifest and validate
    manifest_file = os.path.join(INPUT_DIR, "..", "preprocessor_output", "split_manifest.json")
    if os.path.exists(manifest_file):
        try:
            with open(manifest_file, "r", encoding="utf-8") as mf:
                manifest = json.load(mf)
            
            expected_splits = manifest.get("total_splits", 1)
            split_enabled = manifest.get("split_enabled", False)
            total_rows = manifest.get("total_rows", 0)
            
            if split_enabled:
                logging.info(f"[Main] Split manifest detected:")
                logging.info(f"[Main]   - Expected splits: {expected_splits}")
                logging.info(f"[Main]   - Total rows: {total_rows:,}")
                logging.info(f"[Main]   - Actual files found: {len(input_files)}")
                
                if len(input_files) != expected_splits:
                    logging.warning(
                        f"[Main] ⚠️  File count mismatch! Expected {expected_splits} splits, "
                        f"found {len(input_files)} files"
                    )
            else:
                logging.info(f"[Main] Single file mode (total rows: {total_rows:,})")
        except Exception as exc:
            logging.warning(f"[Main] Could not read split manifest: {exc}")
    
    # Cap thread pool to prevent resource exhaustion
    # Get MAX_SPLIT_FILES from environment or use default of 100
    MAX_SPLIT_FILES = int(os.getenv("MAX_SPLIT_FILES", "100"))
    max_workers = min(len(input_files), MAX_SPLIT_FILES)
    
    if len(input_files) > MAX_SPLIT_FILES:
        logging.warning(
            f"[Main] File count ({len(input_files)}) exceeds MAX_SPLIT_FILES ({MAX_SPLIT_FILES}). "
            f"Capping thread pool to {max_workers} workers."
        )
    else:
        logging.info(f"[Main] Spawning {max_workers} workers for {len(input_files)} split files")

    # ===== RESUME VISIBILITY: report split-level progress up front =====
    # Each worker figures out its own row-level resume state by comparing its split file
    # against its own partial output CSV (see _read_resume_state). That answers "what rows
    # are left" per split, but nothing previously reported the same signal in aggregate, so a
    # resumed run gave no visibility into how many of the N splits were already done until
    # every worker finished one by one. This block reads that same signal up front, purely
    # for logging — it does not change which rows get processed.
    def _count_csv_data_rows(path):
        try:
            with open(path, "r", encoding="utf-8", errors="ignore") as fh:
                return max(sum(1 for _ in fh) - 1, 0)  # minus header row
        except OSError:
            return 0

    _not_started = _in_progress = _complete = 0
    for _split_file in input_files:
        _stem = os.path.basename(_split_file).split(".")[0]
        _partial_output = os.path.join(OUTPUT_DIR, f"processed_{_stem}.csv")
        if not os.path.isfile(_partial_output):
            _not_started += 1
            continue
        _split_rows = _count_csv_data_rows(_split_file)
        _partial_rows = _count_csv_data_rows(_partial_output)
        if _split_rows and _partial_rows >= _split_rows:
            _complete += 1
        else:
            _in_progress += 1

    logging.info(
        f"[Main] Resume status (from existing partial output): "
        f"{_complete}/{len(input_files)} splits already complete, "
        f"{_in_progress} in progress, {_not_started} not started"
    )

    # ===== CHECKPOINT: Load existing checkpoint =====
    global_checkpoint = load_checkpoint()
    
    # ===== API USAGE: Initialize tracking log =====
    initialize_api_usage_log()
    logging.info(f"[Main] API usage log: {API_USAGE_LOG_FILE}")
    
    # Log checkpoint configuration
    logging.info(f"[Main] ===== CHECKPOINT CONFIGURATION =====")
    logging.info(f"[Main] Checkpoint save frequency: Every {CHECKPOINT_SAVE_FREQUENCY} rows")
    logging.info(f"[Main] Checkpoint cleanup on success: {CHECKPOINT_CLEANUP_ON_SUCCESS}")
    if global_checkpoint:
        total_existing = sum(len(v.get('processed_ids', {})) for k, v in global_checkpoint.items() if not k.startswith('_'))
        logging.info(f"[Main] Found existing checkpoint with {total_existing} processed rows")
    logging.info(f"[Main] ===============================================")
    
    # Log relevance scoring configuration
    logging.info(f"[Main] ===== RELEVANCE SCORING CONFIGURATION =====")
    logging.info(f"[Main] State: {STATE_NAME}")
    logging.info(f"[Main] Relevance Mode: {RELEVANCE_MODE}")
    logging.info(f"[Main]   - 'strict': Only YES/NO answers (Bihar)")
    logging.info(f"[Main]   - 'mixed': Both YES/NO and descriptive (Haryana)")
    logging.info(f"[Main]   - 'descriptive': Only descriptive answers")
    logging.info(f"[Main] Relevant Threshold: >= {RELEVANT_THRESHOLD}")
    logging.info(f"[Main] Partially Relevant Threshold: >= {PARTIALLY_RELEVANT_THRESHOLD}")
    logging.info(f"[Main] ===============================================")

    logging.info("[Main] ===== COLUMN CONFIGURATION =====")
    logging.info(
        "[Main] Questions CSV task column (config/default): %s",
        QUESTION_TASK_COLUMN or DEFAULT_QUESTION_TASK_COLUMN,
    )
    logging.info(
        "[Main] Questions CSV question column (config/default): %s",
        QUESTION_TEXT_COLUMN or DEFAULT_QUESTION_TEXT_COLUMN,
    )
    logging.info("[Main] Input CSV task column: %s", INPUT_TASK_COLUMN)
    logging.info("[Main] Input CSV mapped-question column: %s", INPUT_TASK_QUESTION_COLUMN)
    logging.info("[Main] ===============================================")
    
    # Log extra-field extraction configuration (criteria-CSV-driven — see load_questions_mapping())
    _, _main_extra_fields_by_task = load_questions_mapping(QUESTIONS_FILE)
    all_extra_field_names = sorted({
        f["field"] for fields in _main_extra_fields_by_task.values() for f in fields
    })
    if all_extra_field_names:
        logging.info("[Main] Extra field extraction ENABLED. Fields to extract:")
        for task_key, fields in _main_extra_fields_by_task.items():
            for f in fields:
                logging.info("   - task=%s  field=%s  type=%s", task_key, f["field"], f["type"])
    else:
        logging.info("[Main] Extra field extraction DISABLED (no extraction_field rows in criteria CSV).")

    # ✅ --- Global Stats Aggregators ---
    total_rows_processed_all = 0
    total_api_calls_all = 0
    total_api_success_all = 0
    total_api_failure_all = 0
    total_checkpoint_skipped_all = 0
    total_checkpoint_new_all = 0
    all_success_lists = []
    all_failed_lists = []
    user_owned_files = [] # List of user-owned task files
    failed_files = [] # List of input files that failed to process
    total_user_owned_count = 0
    total_standard_count = 0
    total_not_validated_all = 0

    processed_files = []
    with concurrent.futures.ThreadPoolExecutor(max_workers=max_workers) as executor:
        futures = {
            executor.submit(process_file_parallel, f, idx + 1, global_checkpoint): f
            for idx, f in enumerate(input_files)
        }
        for future in concurrent.futures.as_completed(futures):
            original_file = futures[future]
            result_stats = future.result()
            
            if result_stats and isinstance(result_stats, dict):  # ✅ Check if processing was successful
                processed_files.append(result_stats["output_file"])
                total_rows_processed_all += result_stats["rows_attempted"]
                total_api_calls_all += result_stats["api_calls"]
                total_api_success_all += result_stats["api_successes"]
                total_api_failure_all += result_stats["api_failures"]
                total_checkpoint_skipped_all += result_stats.get("checkpoint_skipped", 0)
                total_checkpoint_new_all += result_stats.get("checkpoint_new", 0)
                all_success_lists.extend(result_stats["success_list"])
                all_failed_lists.extend(result_stats["failed_list"])
                total_user_owned_count += result_stats.get("user_owned_count", 0)
                total_standard_count += result_stats.get("standard_count", 0)
                total_not_validated_all += result_stats.get("not_validated_count", 0)
                
                if "user_owned_file" in result_stats:
                    user_owned_files.append(result_stats["user_owned_file"])
                    
                logging.info(f"[Main] Worker finished processing: {original_file}")
            else:
                logging.warning(f"[Main] File {original_file} failed to process.")
                failed_files.append(original_file)

    if not processed_files:
        logging.error("[Main] No files processed successfully. Exiting.")
        # ✅ Still log the summary even if exiting
    else:
        try:
            logging.info(f"[Main] Merging {len(processed_files)} files into {FINAL_OUTPUT_FILE}")
            merged_df = pd.concat([pd.read_csv(f) for f in processed_files], ignore_index=True)
            merged_df.to_csv(FINAL_OUTPUT_FILE, index=False)
            logging.info(f"✅ All files processed and merged into: {FINAL_OUTPUT_FILE}")
            
            # Clean up checkpoint after successful completion
            if CHECKPOINT_CLEANUP_ON_SUCCESS:
                cleanup_checkpoint()
                logging.info(f"[Main] Checkpoint cleaned up successfully")
            
            # Create separate user-owned tasks summary if any exist
            if user_owned_files:
                user_owned_summary_file = os.path.join(OUTPUT_DIR, "user_owned_tasks_summary.csv")
                user_owned_df = pd.concat([pd.read_csv(f) for f in user_owned_files], ignore_index=True)
                user_owned_df.to_csv(user_owned_summary_file, index=False)
                logging.info(f"✅ User-owned tasks merged into: {user_owned_summary_file}")
            
            # Log final extra-field extraction statistics
            if all_extra_field_names:
                logging.info("[Main] Final extra-field statistics:")
                for key_name in all_extra_field_names:
                    if key_name in merged_df.columns:
                        non_null = merged_df[key_name].notna().sum()
                        total = len(merged_df)
                        logging.info(
                            "   - %s: %d/%d (%.1f%%)", key_name, non_null, total, non_null / total * 100
                        )
        except Exception as e:
            logging.exception(f"[Main] Error during merging: {e}")
            exit(1)

    # ✅ --- Log the Final Summary ---
    try:
        logging.info("="*80)
        logging.info("===== 🚀 PROCESSING RUN SUMMARY =====")
        logging.info("="*80)
        
        logging.info(f"Total Rows Processed (sum of attempts): {total_rows_processed_all}")
        logging.info(f"Total Image Rows Processed: {total_api_calls_all}")
        logging.info(f"  - ✅ AI responded (Relevant/Partial/Irrelevant): {total_api_success_all}")
        logging.info(f"  - ⬜ Failed (no usable AI response): {total_api_failure_all}")
        if total_not_validated_all > 0:
            logging.info(f"  - 🚫 notValidated (relevant cap reached, no API call): {total_not_validated_all}")
        
        # Checkpoint statistics
        logging.info("")
        logging.info("===== CHECKPOINT STATISTICS =====")
        logging.info(f"Rows skipped (from checkpoint): {total_checkpoint_skipped_all}")
        logging.info(f"New API calls made: {total_checkpoint_new_all}")
        logging.info(f"API calls saved: {total_checkpoint_skipped_all}")
        if total_checkpoint_skipped_all > 0:
            # Rough estimate: $0.01 per API call (adjust based on your API pricing)
            estimated_savings = total_checkpoint_skipped_all * 0.01
            logging.info(f"💰 Estimated cost saved: ${estimated_savings:.2f}")
        
        # API Usage Statistics
        logging.info("")
        logging.info(f"===== API USAGE & COST STATISTICS =====")
        api_summary = generate_api_usage_summary()
        if api_summary:
            logging.info(f"Total API Calls Logged: {api_summary['total_api_calls']}")
            logging.info(f"  - ✅ Successful: {api_summary['successful_calls']}")
            logging.info(f"  - ❌ Failed: {api_summary['failed_calls']}")
            logging.info(f"")
            logging.info(f"Token Usage:")
            logging.info(f"  - Input Tokens: {api_summary['total_input_tokens']:,}")
            logging.info(f"  - Output Tokens: {api_summary['total_output_tokens']:,}")
            logging.info(f"  - Total Tokens: {api_summary['total_tokens']:,}")
            logging.info(f"")
            logging.info(f"Cost Breakdown:")
            logging.info(f"  - Total Cost: ${api_summary['total_cost_usd']:.4f} USD")
            logging.info(f"  - Avg Cost per Call: ${api_summary['avg_cost_per_call']:.6f} USD")
            logging.info(f"  - Avg Input Tokens per Call: {api_summary['avg_input_tokens_per_call']:.0f}")
            logging.info(f"  - Avg Output Tokens per Call: {api_summary['avg_output_tokens_per_call']:.0f}")
            
            if api_summary.get('model_breakdown'):
                logging.info(f"")
                logging.info(f"Per-Model Breakdown:")
                for model, stats in api_summary['model_breakdown'].items():
                    logging.info(f"  {model}:")
                    logging.info(f"    - Tokens: {stats['Total_Tokens']:,}")
                    logging.info(f"    - Cost: ${stats['Total_Cost_USD']:.4f} USD")
            
            if api_summary.get('worker_breakdown'):
                logging.info(f"")
                logging.info(f"Per-Worker Breakdown:")
                for worker, stats in api_summary['worker_breakdown'].items():
                    logging.info(f"  Worker-{worker}:")
                    logging.info(f"    - Tokens: {stats['Total_Tokens']:,}")
                    logging.info(f"    - Cost: ${stats['Total_Cost_USD']:.4f} USD")
            
            logging.info(f"")
            logging.info(f"📊 Detailed API usage log saved to: {API_USAGE_LOG_FILE}")
        else:
            logging.info(f"No API usage data recorded")
        
        logging.info("")
        logging.info(f"Total Input Files Processed Successfully: {len(processed_files)}")
        logging.info(f"Total Input Files Failed to Process: {len(failed_files)}")
        if failed_files:
            logging.warning("Failed Input Files:")
            for f in failed_files:
                logging.warning(f"  - {f}")

        logging.info("")
        logging.info(f"Task Type Breakdown:")
        logging.info(f"  - Standard Tasks: {total_standard_count}")
        logging.info(f"  - User-Owned Tasks: {total_user_owned_count}")
        logging.info(f"  - Total Tasks: {total_standard_count + total_user_owned_count}")

        if all_failed_lists:
            logging.warning(f"List of Evidence URLs with No Usable AI Response ({len(all_failed_lists)}):")
            for item in all_failed_lists:
                logging.warning(f"  - {item}")
        else:
            logging.info("✅ No failed AI responses recorded.")

        if all_success_lists:
            logging.info(f"List of Evidence URLs the AI Responded To ({len(all_success_lists)}):")
            for item in all_success_lists:
                logging.info(f"  - {item}")
        else:
            logging.info("No AI responses recorded.")
            
        logging.info("="*80)
        logging.info("===== 🏁 END OF SUMMARY =====")
        logging.info("="*80 + "\n")
    except Exception as e:
        logging.exception(f"[Main] Failed to write summary to log: {e}")
