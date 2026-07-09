"""
Execution processor
Runs end-to-end execution pipeline in an isolated workspace.
"""
from __future__ import annotations

import asyncio
import csv
import json
import logging
import os
import shutil
import socket
import subprocess
import sys
import traceback
from copy import deepcopy
from decimal import Decimal
from dataclasses import dataclass
from datetime import datetime
from pathlib import Path
from typing import Any
from uuid import UUID

from sqlalchemy.orm import Session

from core.config import SERVICE_ROOT, settings
from core.constants import EVIDENCE_TYPE_EXTENSIONS, PROCESSING_CONFIG_KEY_EVIDENCE_TYPES
from db.database import SessionLocal
from models.csv_source_type import CsvSourceType
from models.execution import Execution
from services.email_service import EmailService
from services.storage_service import StorageService
from utils.llm_provider import build_llm_env_overrides

logger = logging.getLogger(__name__)


class ExecutionSkipError(Exception):
    """Raised when execution cannot be claimed or should not be processed."""


class ExecutionProcessingError(Exception):
    """Raised when execution processing fails and should be retried/failed."""

    def __init__(self, message: str, error_logs: str = ""):
        super().__init__(message)
        self.message = message
        self.error_logs = error_logs


@dataclass
class ExecutionWorkspace:
    root_dir: Path
    input_dir: Path
    preprocessor_output_dir: Path
    processor_input_dir: Path
    processor_output_dir: Path
    input_csv: Path
    questions_csv: Path
    preprocessed_csv: Path
    final_output_csv: Path
    checkpoint_file: Path
    api_usage_log_file: Path
    main_batches_dir: Path

    def batch_paths(self, batch_index: int) -> "BatchWorkspace":
        """Per-batch paths for main-file split processing (batch_index is 0-based).

        Every path is batch-scoped — nothing is shared with other batches, mirroring the
        sister repo's fully-independent per-main-split directories (separate INPUT_DIR/
        OUTPUT_DIR per split, no shared checkpoint/log of any kind). This avoids any
        collision risk by construction rather than relying on filenames staying unique.
        Only used when MAIN_FILE_SPLIT is on — the off path never calls this.
        """
        suffix = f"batch_{batch_index:03d}"
        preprocessor_output_dir = self.preprocessor_output_dir / suffix
        processor_input_dir = self.processor_input_dir / suffix
        processor_output_dir = self.processor_output_dir / suffix
        preprocessor_output_dir.mkdir(parents=True, exist_ok=True)
        processor_input_dir.mkdir(parents=True, exist_ok=True)
        processor_output_dir.mkdir(parents=True, exist_ok=True)
        return BatchWorkspace(
            batch_index=batch_index,
            filtered_batch_csv=self.main_batches_dir / f"filtered_{suffix}.csv",
            preprocessor_output_dir=preprocessor_output_dir,
            processor_input_dir=processor_input_dir,
            processor_output_dir=processor_output_dir,
            merged_output_csv=processor_output_dir / "merged_output.csv",
            checkpoint_file=processor_output_dir / ".processing_checkpoint.json",
            api_usage_log_file=processor_output_dir / "api_usage_log.csv",
        )


@dataclass
class BatchWorkspace:
    batch_index: int
    filtered_batch_csv: Path
    preprocessor_output_dir: Path
    processor_input_dir: Path
    processor_output_dir: Path
    merged_output_csv: Path
    checkpoint_file: Path
    api_usage_log_file: Path


def _run_async(coro):
    return asyncio.run(coro)


def _resolve_script_path(raw_path: str) -> Path:
    candidate = Path(raw_path)
    if not candidate.is_absolute():
        candidate = (SERVICE_ROOT / candidate).resolve()
    if not candidate.exists():
        raise ExecutionProcessingError(f"Script not found: {candidate}")
    return candidate



def _count_csv_rows(file_path: Path, sample_size: int = 10000) -> tuple[int, bool]:
    """
    Fast row counting with estimation for large files.
    
    Args:
        file_path: Path to CSV file
        sample_size: Number of rows to read before estimating (default 10000)
    
    Returns:
        Tuple of (row_count, is_estimate)
        - row_count: Estimated or exact number of data rows (excluding header)
        - is_estimate: True if row count is estimated, False if exact
    """
    try:
        file_size = file_path.stat().st_size
        
        # Files under 5MB: exact count (fast enough)
        if file_size < 5 * 1024 * 1024:
            with file_path.open("r", encoding="utf-8", newline="") as csv_file:
                reader = csv.reader(csv_file)
                next(reader, None)  # skip header
                count = sum(1 for row in reader if any((cell or "").strip() for cell in row))
                logger.info(f"Exact row count: {count:,} rows ({file_size:,} bytes)")
                return count, False
        
        # Large files: estimate from sample
        # Use readline() throughout — next() on a csv.reader disables tell()
        with file_path.open("r", encoding="utf-8", newline="") as csv_file:
            header_line = csv_file.readline()
            if not header_line:
                return 0, False

            header_pos = csv_file.tell()

            # Read sample rows using readline() to keep tell() functional
            sample_count = 0
            for i in range(sample_size):
                line = csv_file.readline()
                if not line:
                    break
                if line.strip():
                    sample_count += 1

            sample_end_pos = csv_file.tell()
            
            if sample_count == 0:
                return 0, False
            
            # If we read all rows, return exact count
            if sample_count < sample_size:
                logger.info(f"Exact row count: {sample_count:,} rows ({file_size:,} bytes)")
                return sample_count, False
            
            # Estimate total rows from sample
            data_size = file_size - header_pos
            sample_size_bytes = sample_end_pos - header_pos
            
            if sample_size_bytes > 0:
                estimated_rows = int((data_size / sample_size_bytes) * sample_count)
                logger.info(
                    f"Estimated row count: ~{estimated_rows:,} rows "
                    f"(sampled {sample_count:,} rows, {file_size:,} bytes)"
                )
                return estimated_rows, True
            
            return sample_count, True
            
    except Exception as exc:
        logger.warning(f"Error counting CSV rows: {exc}. Returning estimate of 1000.")
        return 1000, True


def _calculate_optimal_split_count(
    row_count: int,
) -> tuple[bool, int, int, str]:
    """
    Calculate optimal file splitting strategy based on row count.
    
    Strategy:
    - Very small files (< MIN_ROWS_FOR_SPLITTING): No splitting (overhead too high)
    - Small files (200-2000 rows): Split to ~100-200 rows per file for parallelization
    - Medium files (2000-10000 rows): Split to ~200-300 rows per file
    - Large files (>= 10000 rows): Split to ~200-500 rows per file, capped at MAX_SPLIT_FILES
    
    Args:
        row_count: Number of data rows in input file
    
    Returns:
        Tuple of (enable_split, num_splits, rows_per_file, strategy_name)
        - enable_split: True to split, False to use single file
        - num_splits: Number of split files to create (1 if not splitting)
        - rows_per_file: Target rows per split file
        - strategy_name: Human-readable strategy name
    """
    import math
    
    # Check if manual configuration is set
    manual_split_files = (settings.SPLIT_FILES or "").strip().lower()
    manual_rows_per_file = settings.ROWS_PER_FILE
    
    # Manual mode: respect explicit configuration
    if manual_split_files in ("yes", "true", "1", "enable", "enabled"):
        if manual_rows_per_file > 0:
            num_splits = max(1, math.ceil(row_count / manual_rows_per_file))
            # Cap at MAX_SPLIT_FILES
            if num_splits > settings.MAX_SPLIT_FILES:
                num_splits = settings.MAX_SPLIT_FILES
                manual_rows_per_file = math.ceil(row_count / num_splits)
            
            logger.info(
                f"Manual splitting enabled: {num_splits} splits of ~{manual_rows_per_file} rows "
                f"(total: {row_count:,} rows)"
            )
            return True, num_splits, manual_rows_per_file, "manual_split"
        else:
            logger.info(f"Manual splitting enabled but ROWS_PER_FILE not set, using dynamic logic")
    
    elif manual_split_files in ("no", "false", "0", "disable", "disabled"):
        logger.info(f"Splitting manually disabled: single file (total: {row_count:,} rows)")
        return False, 1, row_count, "no_split_manual"
    
    # Dynamic mode: intelligent calculation
    if not settings.ENABLE_DYNAMIC_SPLITTING:
        logger.info(f"Dynamic splitting disabled: single file (total: {row_count:,} rows)")
        return False, 1, row_count, "no_split_disabled"
    
    # Very small files: no splitting (overhead too high)
    if row_count < settings.MIN_ROWS_FOR_SPLITTING:
        logger.info(
            f"Very small file detected: {row_count:,} rows < {settings.MIN_ROWS_FOR_SPLITTING:,} threshold → "
            "single file (strategy: no_split)"
        )
        return False, 1, row_count, "no_split"
    
    # Small files (200-2000 rows): optimize for parallelization with ~100-200 rows per split
    if row_count < 2000:
        # Target rows between MIN and optimal, aim for 5-10 splits
        target_rows = max(settings.TARGET_ROWS_PER_SPLIT_MIN, row_count // 10)
        target_rows = min(target_rows, settings.OPTIMAL_ROWS_PER_SPLIT)
        num_splits = max(2, math.ceil(row_count / target_rows))
        rows_per_file = math.ceil(row_count / num_splits)
        
        logger.info(
            f"Small file detected: {row_count:,} rows → "
            f"{num_splits} splits of ~{rows_per_file} rows each (strategy: small_split)"
        )
        return True, num_splits, rows_per_file, "small_split"
    
    # Medium files (2000-10000 rows): balanced splitting with ~200-300 rows per split
    if row_count < 10000:
        target_rows = settings.OPTIMAL_ROWS_PER_SPLIT
        num_splits = math.ceil(row_count / target_rows)
        # Cap at 50 splits for medium files to avoid too many workers
        num_splits = min(50, num_splits)
        rows_per_file = math.ceil(row_count / num_splits)
        
        logger.info(
            f"Medium file detected: {row_count:,} rows → "
            f"{num_splits} splits of ~{rows_per_file} rows each (strategy: medium_split)"
        )
        return True, num_splits, rows_per_file, "medium_split"
    
    # Large files (>=10000 rows): aggressive splitting with target ~200-500 rows per split
    # Use optimal target but allow up to max range for very large files
    target_rows = settings.OPTIMAL_ROWS_PER_SPLIT
    num_splits = math.ceil(row_count / target_rows)
    
    # Cap at MAX_SPLIT_FILES
    if num_splits > settings.MAX_SPLIT_FILES:
        num_splits = settings.MAX_SPLIT_FILES
        rows_per_file = math.ceil(row_count / num_splits)
        logger.info(
            f"Large file detected: {row_count:,} rows → "
            f"{num_splits} splits of ~{rows_per_file} rows each "
            f"(capped at {settings.MAX_SPLIT_FILES}, strategy: large_split_capped)"
        )
        return True, num_splits, rows_per_file, "large_split_capped"
    
    rows_per_file = math.ceil(row_count / num_splits)
    logger.info(
        f"Large file detected: {row_count:,} rows → "
        f"{num_splits} splits of ~{rows_per_file} rows each (strategy: large_split)"
    )
    return True, num_splits, rows_per_file, "large_split"


def _calculate_optimal_batch_count(row_count: int) -> tuple[bool, int, int, str]:
    """
    Calculate whether to cut a large upload into sequential main batches, and how many.

    Mirrors _calculate_optimal_split_count()'s manual/dynamic split, one level up: main-batch
    processing exists to bound how much of a very large upload is in flight at once, so in
    dynamic mode it only kicks in well above the row counts the fine-grained split above
    already handles well on its own — no manual toggle needed for the common case.

    Returns:
        Tuple of (enable_batching, num_batches, rows_per_batch, strategy_name)
    """
    import math

    manual_main_split = (settings.MAIN_FILE_SPLIT or "").strip().lower()

    if manual_main_split in ("yes", "true", "1", "enable", "enabled"):
        num_batches = max(1, math.ceil(row_count / settings.MAIN_BATCH_ROWS_PER_BATCH))
        num_batches = min(num_batches, settings.MAX_MAIN_BATCHES)
        rows_per_batch = math.ceil(row_count / num_batches)
        logger.info(
            "Manual main-batching enabled: %d batches of ~%d rows (total: %s rows)",
            num_batches, rows_per_batch, f"{row_count:,}",
        )
        return True, num_batches, rows_per_batch, "manual_main_batch"

    if manual_main_split in ("no", "false", "0", "disable", "disabled"):
        logger.info("Main-batching manually disabled: single main file (total: %s rows)", f"{row_count:,}")
        return False, 1, row_count, "no_main_batch_manual"

    # Dynamic mode: only batch when the upload is large enough that fine-grained splitting
    # alone would put too much of it in flight simultaneously.
    if row_count < settings.MIN_ROWS_FOR_MAIN_BATCHING:
        logger.info(
            "Row count %s below main-batching threshold (%s) → single main file",
            f"{row_count:,}", f"{settings.MIN_ROWS_FOR_MAIN_BATCHING:,}",
        )
        return False, 1, row_count, "no_main_batch"

    # Dynamic-mode batch count: one rule, no separate tiers. MIN_ROWS_FOR_MAIN_BATCHING is
    # both the no-split floor (checked above) and the target rows per batch, so every batch
    # stays at ~that size no matter how large the upload is — no upper cap on batch count,
    # so batch size never grows past the target for huge files.
    threshold = settings.MIN_ROWS_FOR_MAIN_BATCHING
    num_batches = math.ceil(row_count / threshold)

    rows_per_batch = math.ceil(row_count / num_batches)
    logger.info(
        "Large upload detected: %s rows → %d main batches of ~%d rows each "
        "(strategy: dynamic_main_batch, threshold=%s)",
        f"{row_count:,}", num_batches, rows_per_batch, f"{threshold:,}",
    )
    return True, num_batches, rows_per_batch, "dynamic_main_batch"


def _build_workspace(execution_id: UUID) -> ExecutionWorkspace:
    root_dir = Path(settings.EXECUTION_WORKSPACE_ROOT).resolve() / str(execution_id)
    input_dir = root_dir / "input"
    preprocessor_output_dir = root_dir / "preprocessor_output"
    processor_input_dir = root_dir / "processor_input"
    processor_output_dir = root_dir / "processor_output"
    main_batches_dir = root_dir / "main_batches"

    input_dir.mkdir(parents=True, exist_ok=True)
    preprocessor_output_dir.mkdir(parents=True, exist_ok=True)
    processor_input_dir.mkdir(parents=True, exist_ok=True)
    processor_output_dir.mkdir(parents=True, exist_ok=True)
    main_batches_dir.mkdir(parents=True, exist_ok=True)

    return ExecutionWorkspace(
        root_dir=root_dir,
        input_dir=input_dir,
        preprocessor_output_dir=preprocessor_output_dir,
        processor_input_dir=processor_input_dir,
        processor_output_dir=processor_output_dir,
        input_csv=input_dir / "input.csv",
        questions_csv=input_dir / "question.csv",
        preprocessed_csv=preprocessor_output_dir / "preprocessed_data.csv",
        final_output_csv=processor_output_dir / "merged_output.csv",
        checkpoint_file=processor_output_dir / ".processing_checkpoint.json",
        api_usage_log_file=processor_output_dir / "api_usage_log.csv",
        main_batches_dir=main_batches_dir,
    )


def _clean_preprocessing_dirs(workspace: ExecutionWorkspace) -> None:
    """Clear pre-processor output and processor input before every run, including resumes.

    These directories are fully regenerated each run (split files or a single preprocessed
    CSV, then copied into processor_input_dir). If a prior attempt used a different splitting
    strategy (e.g. single-file vs. split, or a different split count), stale files left behind
    get picked up alongside the new ones and reprocessed as duplicates. main_batches_dir gets
    the same treatment: the batch-cut step re-runs unconditionally on every attempt, so a
    leftover filtered_batch_*.csv from a run that produced a different batch count would
    otherwise be picked up by the glob alongside the freshly-cut files and double-counted in
    the final merge. processor_output_dir (and each batch's own subdirectory under it) is
    intentionally left untouched — it holds the partial/completed merged output
    _read_resume_state() and the per-batch skip-if-completed check rely on to resume correctly.
    """
    for stale_dir in (workspace.preprocessor_output_dir, workspace.processor_input_dir, workspace.main_batches_dir):
        if stale_dir.exists():
            shutil.rmtree(stale_dir)
        stale_dir.mkdir(parents=True, exist_ok=True)


def _resolve_processor_columns_from_config(db: Session, execution: Execution) -> dict[str, str]:
    columns: dict[str, str] = {}
    csv_type_id = (execution.csv_type_id or "").strip()
    if not csv_type_id:
        return columns

    source_type = (
        db.query(CsvSourceType)
        .filter(
            CsvSourceType.tenant_code == execution.tenant_code,
            CsvSourceType.organization_code == execution.organization_code,
            CsvSourceType.type_key == csv_type_id,
            CsvSourceType.is_active.is_(True),
        )
        .first()
    )
    if not source_type:
        return columns

    evidence_context_config = (
        source_type.evidence_context_config if isinstance(source_type.evidence_context_config, dict) else {}
    )
    title_column = str(evidence_context_config.get("title_column", "")).strip()
    if title_column:
        columns["task_column"] = title_column

    question_config = source_type.question_config if isinstance(source_type.question_config, dict) else {}
    question_text_column = str(question_config.get("question_column", "")).strip()
    if not question_text_column:
        mandatory_columns = question_config.get("mandatory_columns", [])
        if isinstance(mandatory_columns, list):
            for column in mandatory_columns:
                column_str = str(column or "").strip()
                if not column_str:
                    continue
                if column_str == "evidence_context_config.title_column":
                    continue
                question_text_column = column_str
                break

    if question_text_column:
        columns["question_text_column"] = question_text_column

    return columns


def _resolve_evidence_type_extensions_from_config(db: Session, execution: Execution) -> dict[str, list[str]]:
    """Per-tenant evidence-type -> file-extension map, sourced from
    CsvSourceType.evidence_types_config so a new type/extension is addable without a deploy.
    Falls back to core.constants.EVIDENCE_TYPE_EXTENSIONS when no active config row exists.
    """
    csv_type_id = (execution.csv_type_id or "").strip()
    source_type = (
        db.query(CsvSourceType)
        .filter(
            CsvSourceType.tenant_code == execution.tenant_code,
            CsvSourceType.organization_code == execution.organization_code,
            CsvSourceType.type_key == csv_type_id,
            CsvSourceType.is_active.is_(True),
        )
        .first()
        if csv_type_id
        else None
    )
    evidence_types_config = source_type.evidence_types_config if source_type else None
    if not isinstance(evidence_types_config, list) or not evidence_types_config:
        return EVIDENCE_TYPE_EXTENSIONS

    return {
        str(item["key"]): [str(ext) for ext in item.get("extensions", [])]
        for item in evidence_types_config
        if item.get("key")
    }


def _run_command(command: list[str], env: dict[str, str], label: str) -> None:
    result = subprocess.run(
        command,
        cwd=str(SERVICE_ROOT),
        capture_output=True,
        text=True,
        env=env,
    )
    if result.returncode == 0:
        return

    stdout = (result.stdout or "").strip()
    stderr = (result.stderr or "").strip()
    combined_logs = f"{label} failed.\nSTDOUT:\n{stdout}\n\nSTDERR:\n{stderr}".strip()

    # Scripts that abort on a known, actionable condition print "FATAL: <reason>" to stderr
    # before exiting non-zero. Surface that reason as the failure message shown to the user
    # (ExecutionResponse.failure_reason / failure email); otherwise fall back to the generic
    # exit-code message. Full stdout/stderr is always kept in error_logs for debugging.
    fatal_reason = next(
        (line[len("FATAL: "):].strip() for line in reversed(stderr.splitlines()) if line.startswith("FATAL: ")),
        None,
    )
    message = f"{label} failed: {fatal_reason}" if fatal_reason else f"{label} failed with exit code {result.returncode}"

    raise ExecutionProcessingError(
        message=message,
        error_logs=combined_logs,
    )


def _inject_llm_env(base_env: dict[str, str]) -> dict[str, str]:
    """
    Ensure the processor subprocess receives LLM auth/model env even when
    the parent process loaded values only via pydantic settings.
    Propagates LLM_PROVIDER and the corresponding provider credentials.
    """
    env = dict(base_env)
    env.update(build_llm_env_overrides(base_env))
    return env


def _claim_execution(
    db: Session,
    *,
    execution_id: UUID,
    worker_id: str,
) -> Execution:
    exists = db.query(Execution.id).filter(Execution.id == execution_id).first()
    if not exists:
        raise ExecutionSkipError(f"Execution not found: {execution_id}")

    execution = (
        db.query(Execution)
        .filter(Execution.id == execution_id)
        .with_for_update(skip_locked=True)
        .first()
    )
    if execution is None:
        raise ExecutionSkipError(f"Execution is locked by another worker: {execution_id}")

    if (execution.status or "").strip().lower() != "queued":
        raise ExecutionSkipError(
            f"Execution is not queued (current status: {execution.status})"
        )

    execution.status = "in_progress"
    execution.worker_id = worker_id
    execution.processing_started_at = datetime.utcnow()
    execution.processing_completed_at = None
    execution.completed_at = None
    execution.failure_reason = None
    execution.error_logs = None
    execution.notification_sent = False
    execution.notification_sent_at = None
    db.commit()
    db.refresh(execution)
    return execution


def _read_actual_cost_from_log(api_usage_log_path: Path) -> Decimal | None:
    """Read the total cost from the api_usage_log.csv written by the processor script."""
    try:
        if not api_usage_log_path.exists():
            return None
        total = Decimal("0")
        with open(api_usage_log_path, newline="", encoding="utf-8") as f:
            reader = csv.DictReader(f)
            for row in reader:
                val = row.get("Total_Cost_USD", "").strip()
                if val:
                    total += Decimal(val)
        return total.quantize(Decimal("0.0001"))
    except Exception as exc:
        logger.warning("Could not read actual cost from api_usage_log: %s", exc)
        return None


def _mark_execution_completed(
    execution_id: UUID,
    *,
    output_file_url: str,
    output_file_size: int,
    processed_rows: int,
    elapsed_seconds: float,
    actual_cost: Decimal | None = None,
    unfiltered_output: dict[str, Any] | None = None,
    processing_log: dict[str, Any] | None = None,
) -> None:
    db = SessionLocal()
    try:
        execution = (
            db.query(Execution)
            .filter(Execution.id == execution_id)
            .with_for_update()
            .first()
        )
        if not execution:
            return

        completed_at = datetime.utcnow()
        execution.status = "completed"
        execution.output_file_url = output_file_url
        execution.output_file_size = output_file_size
        execution.processed_rows = processed_rows
        execution.total_rows = processed_rows
        execution.processing_completed_at = completed_at
        execution.completed_at = completed_at
        execution.failure_reason = None
        execution.error_logs = None
        if actual_cost is not None:
            execution.actual_cost = actual_cost
        if unfiltered_output is not None:
            # Reuses the existing checkpoint_data["files"] shape (already tracks input/
            # questions file state) rather than adding a new column for this audit trail.
            checkpoint = deepcopy(execution.checkpoint_data) if isinstance(execution.checkpoint_data, dict) else {}
            files = checkpoint.get("files")
            if not isinstance(files, dict):
                files = {}
            files["unfiltered_output"] = unfiltered_output
            checkpoint["files"] = files
            execution.checkpoint_data = checkpoint
        if processing_log is not None:
            checkpoint = deepcopy(execution.checkpoint_data) if isinstance(execution.checkpoint_data, dict) else {}
            files = checkpoint.get("files")
            if not isinstance(files, dict):
                files = {}
            files["processing_log"] = processing_log
            checkpoint["files"] = files
            execution.checkpoint_data = checkpoint
        if processed_rows > 0:
            execution.average_processing_time = elapsed_seconds / processed_rows
        db.commit()
        db.refresh(execution)
        EmailService.notify_execution_status(db, execution)
    finally:
        db.close()


def mark_execution_for_retry(
    execution_id: str,
    *,
    retry_count: int,
    reason: str,
    error_logs: str,
) -> None:
    execution_uuid = UUID(str(execution_id))
    db = SessionLocal()
    try:
        execution = (
            db.query(Execution)
            .filter(Execution.id == execution_uuid)
            .with_for_update(skip_locked=True)
            .first()
        )
        if not execution:
            return

        execution.status = "queued"
        execution.retry_count = retry_count
        execution.failure_reason = reason
        execution.error_logs = error_logs
        execution.notification_sent = False
        execution.notification_sent_at = None
        db.commit()
    finally:
        db.close()


def mark_execution_failed(
    execution_id: str,
    *,
    retry_count: int,
    reason: str,
    error_logs: str,
) -> None:
    execution_uuid = UUID(str(execution_id))
    db = SessionLocal()
    try:
        execution = (
            db.query(Execution)
            .filter(Execution.id == execution_uuid)
            .with_for_update(skip_locked=True)
            .first()
        )
        if not execution:
            return

        failed_at = datetime.utcnow()
        execution.status = "failed"
        execution.retry_count = retry_count
        execution.failure_reason = reason
        execution.error_logs = error_logs
        execution.processing_completed_at = failed_at
        execution.completed_at = failed_at
        db.commit()
        db.refresh(execution)
        EmailService.notify_execution_status(db, execution)
    finally:
        db.close()


def process_execution(execution_id: str) -> dict[str, Any]:
    execution_uuid = UUID(str(execution_id))
    worker_id = f"celery@{socket.gethostname()}:{os.getpid()}"
    workspace: ExecutionWorkspace | None = None
    start_ts = datetime.utcnow()

    db = SessionLocal()
    try:
        execution = _claim_execution(db, execution_id=execution_uuid, worker_id=worker_id)
        storage_service = StorageService()

        if not execution.input_file_url or not execution.criterias_file_url:
            raise ExecutionProcessingError(
                "Execution is missing input/questions file URLs."
            )

        # Never wipe the workspace before a run: a prior attempt's checkpoint and partial
        # output CSV (if any) are exactly what lets this run resume instead of reprocessing
        # rows from scratch. Workspace is absent on a first run — _build_workspace creates it
        # fresh below. On success the workspace is removed afterward (EXECUTION_CLEANUP_ON_SUCCESS),
        # so anything left on disk here only exists because a previous attempt failed mid-run.
        _resume_root = Path(settings.EXECUTION_WORKSPACE_ROOT).resolve() / str(execution.id)
        if _resume_root.exists():
            logger.info("resume_skip_cleanup  execution=%s  workspace=%s", execution.id, _resume_root)
        workspace = _build_workspace(execution.id)
        _clean_preprocessing_dirs(workspace)

        input_bytes = _run_async(storage_service.download_file(execution.input_file_url))
        questions_bytes = _run_async(storage_service.download_file(execution.criterias_file_url))
        if not input_bytes:
            raise ExecutionProcessingError("Input file could not be downloaded from storage.")
        if not questions_bytes:
            raise ExecutionProcessingError("Questions file could not be downloaded from storage.")

        workspace.input_csv.write_bytes(input_bytes)
        workspace.questions_csv.write_bytes(questions_bytes)

        preprocessor_script = _resolve_script_path(settings.PREPROCESS_SCRIPT_PATH)
        processor_script = _resolve_script_path(settings.PROCESSOR_SCRIPT_PATH)

        base_env = _inject_llm_env(os.environ.copy())

        # Per-(user, task) relevant-evidence cap config for this execution. Resolved once
        # here because BOTH subprocesses need it: the pre-processor must keep each
        # (UUID, task) group inside one split file (group-aware splitting), and the
        # processor enforces the cap. Driven solely by this execution's threshold_config —
        # only present when the user supplied a number at execution create/update; absent
        # (or not a positive number) means no cap and all rows are processed.
        # threshold_config shape: {"max_relevant_per_user_task": 5}, or None when no cap was requested.
        cap_config = execution.threshold_config if isinstance(execution.threshold_config, dict) else {}
        max_relevant = cap_config.get("max_relevant_per_user_task")
        # bool is an int subclass in Python — isinstance(True, int) is True — so a malformed
        # evidence_threshold: true from the client would otherwise silently enable a cap of 1.
        is_relevant_limit_enabled = (
            isinstance(max_relevant, int) and not isinstance(max_relevant, bool) and max_relevant > 0
        )

        preprocessor_env = {
            **base_env,
            "PYTHONUNBUFFERED": "1",
        }
        configured_columns = _resolve_processor_columns_from_config(db, execution)
        question_task_column = configured_columns.get("task_column", "")
        question_text_column = configured_columns.get("question_text_column", "")
        if question_task_column:
            preprocessor_env["PREPROCESS_QUESTION_TASK_COLUMN"] = question_task_column

        # processor_env is built here (not inside either branch below) because both the
        # off-path and every per-batch run in the on-path need an identical copy of it.
        processing_config = execution.processing_config if isinstance(execution.processing_config, dict) else {}
        evidence_types = processing_config.get(PROCESSING_CONFIG_KEY_EVIDENCE_TYPES)
        if not isinstance(evidence_types, list) or not evidence_types:
            raise ExecutionProcessingError(
                f"Execution {execution.id} is missing required evidence_types in processing_config"
            )
        # Per-tenant type->extension map (DB-driven; see CsvSourceType.evidence_types_config)
        # passed to both scripts so a new type/extension is addable without a code change.
        evidence_types_to_validate_json = json.dumps(_resolve_evidence_type_extensions_from_config(db, execution))

        processor_env = {
            **base_env,
            "PYTHONUNBUFFERED": "1",
            "CHECKPOINT_CLEANUP_ON_SUCCESS": "True",
        }
        if question_task_column:
            processor_env["PROCESSOR_QUESTION_TASK_COLUMN"] = question_task_column
            processor_env["PROCESSOR_INPUT_TASK_COLUMN"] = question_task_column
        if question_text_column:
            processor_env["PROCESSOR_QUESTION_TEXT_COLUMN"] = question_text_column

        # Per-(user, task) relevant-evidence cap (config resolved above), not a secret —
        # passed as a CLI arg like every other per-execution setting (task columns,
        # --max-processed-rows), consistent with how the pre-processor already receives
        # this same value. Applied at each processor invocation below (single-file path
        # and per-batch path, since MAIN_FILE_SPLIT builds its own command per batch).
        # Omitted entirely means no cap, all rows processed.
        if is_relevant_limit_enabled:
            logger.info(
                "relevant_cap_enabled  execution=%s  max_relevant_per_user_task=%s",
                execution.id,
                max_relevant,
            )

        batch_api_usage_logs: list[Path] = []

        # Decided once, dynamically, from the actual uploaded file — not a fixed toggle — so
        # small/medium uploads never pay the main-batch overhead and only genuinely large ones
        # get cut into sequential batches. See _calculate_optimal_batch_count for thresholds.
        input_row_count, input_row_count_is_estimate = _count_csv_rows(workspace.input_csv)
        main_batch_enabled, num_main_batches, rows_per_main_batch, main_batch_strategy = (
            _calculate_optimal_batch_count(input_row_count)
        )
        logger.info(
            "main_batch_strategy  execution=%s  strategy=%s  rows=%d%s  num_batches=%d  rows_per_batch=~%d",
            execution.id,
            main_batch_strategy,
            input_row_count,
            " (estimated)" if input_row_count_is_estimate else "",
            num_main_batches if main_batch_enabled else 1,
            rows_per_main_batch,
        )

        if main_batch_enabled:
            # === Main-batch sequential processing ===
            # Cut the whole input into main batches once, then run each one fully
            # (fine-split + parallel processing, both unchanged) before starting the next.
            batch_cut_cmd = [
                sys.executable,
                str(preprocessor_script),
                "--input-csv",
                str(workspace.input_csv),
                "--question-csv",
                str(workspace.questions_csv),
                "--output-dir",
                str(workspace.main_batches_dir),
                "--main-batch-rows",
                str(rows_per_main_batch),
                "--max-main-batches",
                str(settings.MAX_MAIN_BATCHES),
                "--evidence-types",
                ",".join(evidence_types),
                "--evidence-types-to-validate",
                evidence_types_to_validate_json,
            ]
            # Forced off here, same as the single-file and per-batch preprocessor calls below —
            # this branch doesn't wire real school-filter support into the batch-cut step yet.
            # Without this, the arg is omitted entirely and the script falls back to whatever
            # USE_SCHOOL_FILTER happens to be set to in the environment, inconsistent with the
            # other two call sites which explicitly force it false.
            batch_cut_cmd.extend(["--use-school-filter", "false"])
            if is_relevant_limit_enabled:
                # Keep (UUID, task) groups within a single main batch so per-batch
                # cap counts stay correct (mirrors group-aware fine-splitting below).
                batch_cut_cmd.extend(["--max-relevant-per-user-task", str(max_relevant)])
            batch_cut_env = {**preprocessor_env, "MAIN_FILE_SPLIT": "true"}
            _run_command(batch_cut_cmd, batch_cut_env, "Pre-processor batch-cut")

            batch_files = sorted(
                workspace.main_batches_dir.glob("filtered_batch_*.csv"),
                key=lambda p: p.name,
            )
            if not batch_files:
                raise ExecutionProcessingError(
                    f"MAIN_FILE_SPLIT is on but no main batch files were produced in "
                    f"{workspace.main_batches_dir}"
                )

            logger.info(
                "main_batch_split  execution=%s  total_batches=%d",
                execution.id,
                len(batch_files),
            )

            merged_batch_outputs: list[Path] = []
            for batch_index, batch_csv in enumerate(batch_files):
                bw = workspace.batch_paths(batch_index)
                label = f"batch {batch_index + 1}/{len(batch_files)}"
                batch_api_usage_logs.append(bw.api_usage_log_file)

                if bw.merged_output_csv.exists():
                    logger.info("main_batch_already_completed  execution=%s  %s", execution.id, label)
                    merged_batch_outputs.append(bw.merged_output_csv)
                    continue

                logger.info("main_batch_started  execution=%s  %s", execution.id, label)

                batch_row_count, batch_is_estimate = _count_csv_rows(batch_csv)
                batch_enable_split, _num_splits, batch_rows_per_file, batch_strategy = (
                    _calculate_optimal_split_count(batch_row_count)
                )
                logger.info(
                    "batch_split_strategy  execution=%s  %s  strategy=%s  rows=%d%s  rows_per_split=~%d",
                    execution.id,
                    label,
                    batch_strategy,
                    batch_row_count,
                    " (estimated)" if batch_is_estimate else "",
                    batch_rows_per_file,
                )

                batch_preprocessor_cmd = [
                    sys.executable,
                    str(preprocessor_script),
                    "--input-csv",
                    str(batch_csv),
                    "--question-csv",
                    str(workspace.questions_csv),
                    "--output-dir",
                    str(bw.preprocessor_output_dir),
                    "--split-files",
                    "yes" if batch_enable_split else "no",
                ]
                if batch_enable_split:
                    batch_preprocessor_cmd.extend(["--rows-per-file", str(batch_rows_per_file)])
                batch_preprocessor_cmd.extend(["--use-school-filter", "false", "--skip-batch-cut"])
                batch_preprocessor_cmd.extend(["--evidence-types", ",".join(evidence_types)])
                batch_preprocessor_cmd.extend(["--evidence-types-to-validate", evidence_types_to_validate_json])
                if is_relevant_limit_enabled:
                    batch_preprocessor_cmd.extend(["--max-relevant-per-user-task", str(max_relevant)])
                _run_command(batch_preprocessor_cmd, preprocessor_env, f"Pre-processor script ({label})")

                if batch_enable_split:
                    batch_split_files = sorted(
                        bw.preprocessor_output_dir.glob("split_*.csv"),
                        key=lambda p: p.name,
                    )
                    if not batch_split_files:
                        raise ExecutionProcessingError(
                            f"No split files found in {bw.preprocessor_output_dir} after splitting "
                            f"enabled ({label})"
                        )
                    for split_file in batch_split_files:
                        shutil.copy2(split_file, bw.processor_input_dir / split_file.name)
                else:
                    batch_preprocessed_csv = bw.preprocessor_output_dir / "preprocessed_data.csv"
                    if not batch_preprocessed_csv.exists():
                        raise ExecutionProcessingError(
                            f"Pre-processed file not found: {batch_preprocessed_csv} ({label})"
                        )
                    shutil.copy2(batch_preprocessed_csv, bw.processor_input_dir / "input.csv")

                batch_processor_cmd = [
                    sys.executable,
                    str(processor_script),
                    "--input-dir",
                    str(bw.processor_input_dir),
                    "--output-dir",
                    str(bw.processor_output_dir),
                    "--final-output-file",
                    str(bw.merged_output_csv),
                    "--checkpoint-file",
                    str(bw.checkpoint_file),
                    "--api-usage-log-file",
                    str(bw.api_usage_log_file),
                    "--questions-file",
                    str(workspace.questions_csv),
                    "--max-processed-rows",
                    str(settings.PROCESSOR_MAX_ROWS),
                    "--evidence-types-to-validate",
                    evidence_types_to_validate_json,
                ]
                if is_relevant_limit_enabled:
                    batch_processor_cmd.extend(["--max-relevant-per-user-task", str(max_relevant)])
                _run_command(batch_processor_cmd, processor_env, f"Processor script ({label})")

                if not bw.merged_output_csv.exists():
                    raise ExecutionProcessingError(
                        f"Merged output file not found: {bw.merged_output_csv} ({label})"
                    )

                logger.info("main_batch_completed  execution=%s  %s", execution.id, label)
                merged_batch_outputs.append(bw.merged_output_csv)

            # Merging batch outputs is CSV-processing logic, so it lives in its own script
            # (subprocess), same as every other transformation step in this pipeline —
            # not hand-rolled here in the service layer.
            merge_script = _resolve_script_path(settings.MERGE_SCRIPT_PATH)
            merge_cmd = [sys.executable, str(merge_script)]
            for batch_output in merged_batch_outputs:
                merge_cmd.extend(["--input-csv", str(batch_output)])
            merge_cmd.extend(["--output-csv", str(workspace.final_output_csv)])
            _run_command(merge_cmd, base_env, "Merge batch outputs script")
        else:
            # === Single main file — today's exact existing behavior, unchanged ===
            row_count, is_estimate = input_row_count, input_row_count_is_estimate

            enable_split, num_splits, rows_per_file, strategy_name = _calculate_optimal_split_count(row_count)

            logger.info(
                f"Splitting strategy selected: {strategy_name} | "
                f"Input rows: {row_count:,}{' (estimated)' if is_estimate else ''} | "
                f"Splits: {num_splits if enable_split else 1} | "
                f"Rows per split: ~{rows_per_file}"
            )

            preprocessor_cmd = [
                sys.executable,
                str(preprocessor_script),
                "--input-csv",
                str(workspace.input_csv),
                "--question-csv",
                str(workspace.questions_csv),
                "--output-dir",
                str(workspace.preprocessor_output_dir),
                "--split-files",
                "yes" if enable_split else "no",
            ]

            # Add rows-per-file parameter if splitting is enabled
            if enable_split:
                preprocessor_cmd.extend(["--rows-per-file", str(rows_per_file)])

            preprocessor_cmd.extend([
                "--use-school-filter",
                "false",
            ])
            preprocessor_cmd.extend(["--evidence-types", ",".join(evidence_types)])
            preprocessor_cmd.extend(["--evidence-types-to-validate", evidence_types_to_validate_json])
            if is_relevant_limit_enabled:
                preprocessor_cmd.extend(["--max-relevant-per-user-task", str(max_relevant)])

            _run_command(preprocessor_cmd, preprocessor_env, "Pre-processor script")

            # Handle split files or single file based on strategy
            if enable_split:
                # Multiple split files expected
                split_files = sorted(
                    workspace.preprocessor_output_dir.glob("split_*.csv"),
                    key=lambda p: p.name
                )

                if not split_files:
                    raise ExecutionProcessingError(
                        f"No split files found in {workspace.preprocessor_output_dir} after splitting enabled"
                    )

                logger.info(f"Found {len(split_files)} split files, copying to processor input directory")

                # Copy all split files to processor input directory
                for split_file in split_files:
                    shutil.copy2(
                        split_file,
                        workspace.processor_input_dir / split_file.name,
                    )
            else:
                # Single file expected
                if not workspace.preprocessed_csv.exists():
                    raise ExecutionProcessingError(
                        f"Pre-processed file not found: {workspace.preprocessed_csv}"
                    )

                shutil.copy2(
                    workspace.preprocessed_csv,
                    workspace.processor_input_dir / "input.csv",
                )

            processor_cmd = [
                sys.executable,
                str(processor_script),
                "--input-dir",
                str(workspace.processor_input_dir),
                "--output-dir",
                str(workspace.processor_output_dir),
                "--final-output-file",
                str(workspace.final_output_csv),
                "--checkpoint-file",
                str(workspace.checkpoint_file),
                "--api-usage-log-file",
                str(workspace.api_usage_log_file),
                "--questions-file",
                str(workspace.questions_csv),
                "--max-processed-rows",
                str(settings.PROCESSOR_MAX_ROWS),
                "--evidence-types-to-validate",
                evidence_types_to_validate_json,
            ]
            if is_relevant_limit_enabled:
                processor_cmd.extend(["--max-relevant-per-user-task", str(max_relevant)])
            _run_command(processor_cmd, processor_env, "Processor script")

            batch_api_usage_logs.append(workspace.api_usage_log_file)

        if not workspace.final_output_csv.exists():
            raise ExecutionProcessingError(
                f"Merged output file not found: {workspace.final_output_csv}"
            )

        # processed_rows reflects actual processing volume, counted from the unfiltered
        # file — unaffected by whether REMOVE_INVALID_ROWS_FROM_OUTPUT later trims the
        # deliverable. Read before any cleanup so this metric never changes meaning.
        unfiltered_bytes = workspace.final_output_csv.read_bytes()
        processed_rows, processed_rows_is_estimate = _count_csv_rows(workspace.final_output_csv)
        if processed_rows_is_estimate:
            # _count_csv_rows() estimates from a sample on large files. processed_rows,
            # total_rows, and average_processing_time below are derived from this value,
            # so flag it — those persisted metrics are an approximation, not exact.
            logger.warning(
                "completion_metrics_estimated  execution=%s  processed_rows=%d",
                execution.id,
                processed_rows,
            )

        # Always upload the unfiltered merged output first, before any row-removal step,
        # so the full evidence trail survives in cloud storage even when the cleanup below
        # strips rows from the deliverable. This is what makes automatic removal safe to
        # default on — a bug in the cap/processor logic can no longer silently destroy the
        # only record of what actually happened (see commit feeecef, which reverted an
        # earlier automatic-strip attempt for exactly that risk).
        unfiltered_file_name = f"output_{execution.id}_unfiltered.csv"
        unfiltered_file_path = storage_service.build_execution_file_path(
            user_id=execution.created_by or "system",
            execution_id=str(execution.id),
            file_name=unfiltered_file_name,
        )
        uploaded_unfiltered_path = _run_async(
            storage_service.upload_file(
                file_content=unfiltered_bytes,
                file_path=unfiltered_file_path,
                content_type="text/csv",
            )
        )
        unfiltered_output_meta = {
            "url": uploaded_unfiltered_path,
            "size": len(unfiltered_bytes),
            "uploaded_at": datetime.utcnow().isoformat(),
        }

        if settings.REMOVE_INVALID_ROWS_FROM_OUTPUT:
            cleanup_script = _resolve_script_path(settings.CLEANUP_SCRIPT_PATH)
            cleaned_path = workspace.final_output_csv.with_suffix(".cleaned.csv")
            cleanup_cmd = [
                sys.executable,
                str(cleanup_script),
                "--input-csv", str(workspace.final_output_csv),
                "--output-csv", str(cleaned_path),
            ]
            _run_command(cleanup_cmd, base_env, "Cleanup script")
            cleaned_path.replace(workspace.final_output_csv)

        output_bytes = workspace.final_output_csv.read_bytes()
        output_file_name = f"output_{execution.id}.csv"
        output_file_path = storage_service.build_execution_file_path(
            user_id=execution.created_by or "system",
            execution_id=str(execution.id),
            file_name=output_file_name,
        )
        uploaded_output_path = _run_async(
            storage_service.upload_file(
                file_content=output_bytes,
                file_path=output_file_path,
                content_type="text/csv",
            )
        )

        elapsed_seconds = (datetime.utcnow() - start_ts).total_seconds()
        # batch_api_usage_logs has exactly one entry (workspace.api_usage_log_file) when
        # MAIN_FILE_SPLIT is off, or one entry per completed batch when it's on — each
        # batch logs its own cost independently (no shared log across batches), so the
        # execution's total cost is the sum across whichever logs actually exist.
        batch_costs = [
            cost
            for log_path in batch_api_usage_logs
            if (cost := _read_actual_cost_from_log(log_path)) is not None
        ]
        actual_cost = sum(batch_costs) if batch_costs else None

        # Each processor invocation writes its own processing.log next to its
        # api_usage_log_file (one per batch when MAIN_FILE_SPLIT is on, one overall
        # otherwise). Merge them into a single per-execution log and upload it — previously
        # this log only ever existed inside the temp workspace and was destroyed by
        # EXECUTION_CLEANUP_ON_SUCCESS on every successful run, so it was never actually
        # retrievable after the fact.
        processing_log_parts: list[str] = []
        for batch_number, log_path in enumerate(batch_api_usage_logs, start=1):
            candidate = log_path.parent / "processing.log"
            if not candidate.exists():
                continue
            if len(batch_api_usage_logs) > 1:
                processing_log_parts.append(f"===== Batch {batch_number}/{len(batch_api_usage_logs)} =====")
            processing_log_parts.append(candidate.read_text(encoding="utf-8", errors="replace"))

        processing_log_meta: dict[str, Any] | None = None
        if processing_log_parts:
            processing_log_bytes = "\n".join(processing_log_parts).encode("utf-8")
            processing_log_file_name = f"processing_log_{execution.id}.txt"
            processing_log_file_path = storage_service.build_execution_file_path(
                user_id=execution.created_by or "system",
                execution_id=str(execution.id),
                file_name=processing_log_file_name,
            )
            uploaded_processing_log_path = _run_async(
                storage_service.upload_file(
                    file_content=processing_log_bytes,
                    file_path=processing_log_file_path,
                    content_type="text/plain",
                )
            )
            processing_log_meta = {
                "url": uploaded_processing_log_path,
                "size": len(processing_log_bytes),
                "uploaded_at": datetime.utcnow().isoformat(),
            }

        _mark_execution_completed(
            execution.id,
            output_file_url=uploaded_output_path,
            output_file_size=len(output_bytes),
            processed_rows=processed_rows,
            elapsed_seconds=elapsed_seconds,
            actual_cost=actual_cost,
            unfiltered_output=unfiltered_output_meta,
            processing_log=processing_log_meta,
        )

        if settings.EXECUTION_CLEANUP_ON_SUCCESS and workspace.root_dir.exists():
            shutil.rmtree(workspace.root_dir, ignore_errors=True)

        return {
            "status": "completed",
            "execution_id": str(execution.id),
            "output_file_url": uploaded_output_path,
            "processed_rows": processed_rows,
            "worker_id": worker_id,
        }
    except ExecutionSkipError:
        raise
    except ExecutionProcessingError:
        raise
    except Exception as exc:  # noqa: BLE001
        raise ExecutionProcessingError(
            message=str(exc),
            error_logs=traceback.format_exc(),
        ) from exc
    finally:
        db.close()
