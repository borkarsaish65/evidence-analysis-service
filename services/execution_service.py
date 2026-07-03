"""
Execution Service
Handles execution business logic, file management, and background processing
"""
import asyncio
import csv
import io
import json
import logging
import re
import unicodedata
from copy import deepcopy
from datetime import datetime
from decimal import Decimal
from pathlib import Path
from typing import Any, Optional
from uuid import UUID

from fastapi import HTTPException, status
from pydantic import ValidationError
from sqlalchemy import func, or_
from sqlalchemy.exc import SQLAlchemyError
from sqlalchemy.orm import Session

from core.config import settings
from core.constants import PROCESSING_CONFIG_KEY_EVIDENCE_TYPES
from models.csv_source_type import CsvSourceType
from models.execution import Execution
from models.schemas import (
    CloudDownloadableUrlRequest,
    CloudDownloadableUrlResponse,
    CloudSignedUrlRequest,
    CloudSignedUrlResponse,
    ExecutionCreate,
    ExecutionCreateRequest,
    ExecutionDetail,
    ExecutionFileCheckpointState,
    ExecutionFileCompleteResponse,
    ExecutionFilePreviewResponse,
    ExecutionFileUploadUrlResponse,
    ExecutionList,
    ExecutionNotificationResponse,
    ExecutionResponse,
    ExecutionValidationResponse,
    ExecutionUploadInitRequest,
    ExecutionUploadInitResponse,
    ExecutionUpdate,
    FileUploadUrlRequest,
    FileValidationResult,
    SignedUrlPayload,
    StatusResponse,
    UserResponse,
)
from services.background_worker import BackgroundWorker
from services.email_service import EmailService
from utils.llm_provider import get_llm_model_name
from services.storage_service import StorageService

logger = logging.getLogger(__name__)

_ALLOWED_CSV_CONTENT_TYPES = {
    "text/csv",
    "application/csv",
    "application/vnd.ms-excel",
    "text/plain",
    "application/octet-stream",
}

_FILE_TYPE_HINTS = {
    "questions": ("question", "questions", "criteria", "checklist"),
    "input": ("input", "evidence", "data"),
}
_DOWNLOADABLE_PATH_SEGMENT_PATTERN = re.compile(r"^[A-Za-z0-9._-]+$")
_DEFAULT_ESTIMATED_COST_PER_ROW = Decimal("0.0010")
_DEFAULT_ESTIMATED_TIME_PER_ROW_SECONDS = 0.5


class ExecutionService:
    """Service for managing executions"""

    def __init__(self, db: Session, worker: BackgroundWorker):
        self.db = db
        self.storage_service = StorageService()
        self.worker = worker

    @staticmethod
    def _to_float(value: Optional[Decimal]) -> Optional[float]:
        if value is None:
            return None
        return float(value)

    @staticmethod
    def _to_execution_response(execution: Execution, checkpoint_override: Optional[dict] = None) -> ExecutionResponse:
        """Build API response with safe fallbacks for nullable legacy fields."""
        # For in-progress executions, try to get real-time progress from checkpoint file
        processed_rows = execution.processed_rows or 0
        if (execution.status or "").strip().lower() == "in_progress":
            # Use pre-fetched checkpoint data when available (avoids per-row file I/O)
            if checkpoint_override is not None:
                checkpoint_progress = checkpoint_override
            else:
                checkpoint_progress = ExecutionService._read_processor_checkpoint_file(execution.id)
            checkpoint_processed = checkpoint_progress.get('total_processed', 0)
            # Use checkpoint value if it's more recent (higher) than DB value
            if checkpoint_processed > processed_rows:
                processed_rows = checkpoint_processed
        
        return ExecutionResponse(
            id=execution.id,
            name=execution.name or "Untitled execution",
            csv_type_id=execution.csv_type_id,
            status=execution.status or "queued",
            states=execution.states or [],
            created_by=execution.created_by,
            created_at=execution.created_at or datetime.utcnow(),
            updated_at=execution.updated_at,
            completed_at=execution.completed_at,
            total_rows=execution.total_rows,
            processed_rows=processed_rows,
            input_file_url=execution.input_file_url,
            criterias_file_url=execution.criterias_file_url,
            output_file_url=execution.output_file_url,
            estimated_cost=ExecutionService._to_float(execution.estimated_cost),
            actual_cost=ExecutionService._to_float(execution.actual_cost),
            estimated_time_seconds=execution.estimated_time_seconds,
            failure_reason=execution.failure_reason,
            average_processing_time=ExecutionService._to_float(execution.average_processing_time),
            notification_sent=bool(execution.notification_sent) if execution.notification_sent is not None else False,
        )

    def _validate_file_descriptor(self, file_name: str, size_bytes: int, content_type: Optional[str], label: str) -> tuple[str, str]:
        safe_file_name = self.storage_service.sanitize_filename(file_name)
        extension = Path(safe_file_name).suffix.lower()
        allowed_extensions = {ext.lower() for ext in settings.ALLOWED_EXTENSIONS}

        if extension not in allowed_extensions:
            raise HTTPException(
                status_code=status.HTTP_400_BAD_REQUEST,
                detail=f"{label} must be one of: {', '.join(sorted(allowed_extensions))}",
            )

        if size_bytes <= 0:
            raise HTTPException(
                status_code=status.HTTP_400_BAD_REQUEST,
                detail=f"{label} size must be greater than 0 bytes",
            )

        if size_bytes > settings.MAX_UPLOAD_SIZE:
            raise HTTPException(
                status_code=status.HTTP_400_BAD_REQUEST,
                detail=(
                    f"{label} exceeds max size of {settings.MAX_UPLOAD_SIZE} bytes"
                ),
            )

        normalized_content_type = (content_type or "text/csv").strip().lower()
        if normalized_content_type not in _ALLOWED_CSV_CONTENT_TYPES:
            raise HTTPException(
                status_code=status.HTTP_400_BAD_REQUEST,
                detail=(
                    f"{label} content type '{normalized_content_type}' is not allowed"
                ),
            )

        return safe_file_name, normalized_content_type

    def _validate_file_name_for_signed_url(self, file_name: str, label: str) -> str:
        """Validate file name when size/content-type are not provided."""
        safe_file_name = self.storage_service.sanitize_filename(file_name)
        extension = Path(safe_file_name).suffix.lower()
        allowed_extensions = {ext.lower() for ext in settings.ALLOWED_EXTENSIONS}

        if extension not in allowed_extensions:
            raise HTTPException(
                status_code=status.HTTP_400_BAD_REQUEST,
                detail=f"{label} must be one of: {', '.join(sorted(allowed_extensions))}",
            )

        return safe_file_name

    @staticmethod
    def _infer_file_type(file_name: str, used_types: set[str]) -> str:
        lower_name = (file_name or "").strip().lower()

        if "questions" not in used_types:
            if any(token in lower_name for token in _FILE_TYPE_HINTS["questions"]):
                return "questions"
        if "input" not in used_types:
            if any(token in lower_name for token in _FILE_TYPE_HINTS["input"]):
                return "input"

        if "input" not in used_types:
            return "input"
        if "questions" not in used_types:
            return "questions"

        return "questions"

    def _validate_uploaded_metadata(self, metadata: dict, label: str) -> None:
        size_bytes = int(metadata.get("size_bytes", 0))
        if size_bytes <= 0:
            raise HTTPException(
                status_code=status.HTTP_400_BAD_REQUEST,
                detail=f"{label} was not uploaded or is empty",
            )

        if size_bytes > settings.MAX_UPLOAD_SIZE:
            raise HTTPException(
                status_code=status.HTTP_400_BAD_REQUEST,
                detail=f"{label} exceeds max size of {settings.MAX_UPLOAD_SIZE} bytes",
            )

        content_type = metadata.get("content_type")
        if content_type:
            normalized_content_type = content_type.strip().lower()
            if normalized_content_type not in _ALLOWED_CSV_CONTENT_TYPES:
                raise HTTPException(
                    status_code=status.HTTP_400_BAD_REQUEST,
                    detail=(
                        f"{label} content type '{normalized_content_type}' is not allowed"
                    ),
                )

    @staticmethod
    def _resolve_scope(current_user: UserResponse) -> tuple[str, str]:
        tenant_code = (current_user.tenant_code or settings.DEFAULT_TENANT_CODE or "").strip()
        organization_code = (current_user.organization_code or settings.DEFAULT_ORGANIZATION_CODE or "").strip()
        if not tenant_code or not organization_code:
            raise HTTPException(
                status_code=status.HTTP_500_INTERNAL_SERVER_ERROR,
                detail="Execution defaults are not configured",
            )
        return tenant_code, organization_code

    @staticmethod
    def _cloud_storage_label() -> str:
        raw_value = (
            (settings.CLOUD_STORAGE_PROVIDER or "").strip()
            or (settings.CLOUD_STORAGE or "").strip()
            or "gcp"
        ).lower()
        labels = {
            "aws": "AWS",
            "gcp": "GCP",
            "azure": "AZURE",
            "local": "LOCAL",
        }
        return labels.get(raw_value, raw_value.upper())

    @classmethod
    def _validate_downloadable_file_path(cls, file_path: str) -> tuple[str, str, str, str]:
        normalized = (file_path or "").strip().lstrip("/")
        if not normalized:
            raise HTTPException(
                status_code=status.HTTP_400_BAD_REQUEST,
                detail="filePath cannot be empty.",
            )

        segments = normalized.split("/")
        if len(segments) != 5 or segments[3] != "executions":
            raise HTTPException(
                status_code=status.HTTP_400_BAD_REQUEST,
                detail=(
                    "Invalid filePath pattern. Expected: "
                    "tenant_code/org_code/userId/executions/fileName.ext"
                ),
            )

        tenant_code, organization_code, user_id, _, file_name = segments
        for segment_label, segment_value in (
            ("tenant_code", tenant_code),
            ("org_code", organization_code),
            ("userId", user_id),
            ("fileName", file_name),
        ):
            if not _DOWNLOADABLE_PATH_SEGMENT_PATTERN.fullmatch(segment_value):
                raise HTTPException(
                    status_code=status.HTTP_400_BAD_REQUEST,
                    detail=f"Invalid {segment_label} in filePath: {normalized}",
                )

        if "." not in file_name:
            raise HTTPException(
                status_code=status.HTTP_400_BAD_REQUEST,
                detail=f"filePath must include a file extension: {normalized}",
            )

        return normalized, tenant_code, organization_code, user_id

    @classmethod
    def _enforce_download_path_scope(
        cls,
        *,
        tenant_code: str,
        organization_code: str,
        current_user_id: str,
        path_tenant: str,
        path_organization: str,
        path_user_id: str,
        file_path: str,
    ) -> None:
        if path_tenant != tenant_code or path_organization != organization_code:
            raise HTTPException(
                status_code=status.HTTP_403_FORBIDDEN,
                detail=f"Not allowed to access filePath outside your tenant/org scope: {file_path}",
            )

        if path_user_id != (current_user_id or "").strip():
            raise HTTPException(
                status_code=status.HTTP_403_FORBIDDEN,
                detail=f"Not allowed to access another user's filePath: {file_path}",
            )

    def _get_csv_source_type(self, tenant_code: str, organization_code: str, type_key: str) -> Optional[CsvSourceType]:
        normalized_type_key = (type_key or "").strip()
        if not normalized_type_key:
            return None

        return (
            self.db.query(CsvSourceType)
            .filter(
                CsvSourceType.tenant_code == tenant_code,
                CsvSourceType.organization_code == organization_code,
                CsvSourceType.type_key == normalized_type_key,
                CsvSourceType.is_active.is_(True),
            )
            .first()
        )

    @staticmethod
    def _resolve_criterias_mode(source_type: CsvSourceType) -> Optional[str]:
        question_config = source_type.question_config if isinstance(source_type.question_config, dict) else {}

        for field_name in ("criterias_mode", "mode"):
            value = str(question_config.get(field_name, "")).strip()
            if value:
                return value

        entry_options = question_config.get("entry_options")
        if isinstance(entry_options, list):
            for option in entry_options:
                if not isinstance(option, dict):
                    continue
                value = str(option.get("key", "")).strip() or str(option.get("value", "")).strip()
                if value:
                    return value
        return None

    @staticmethod
    def _resolve_processing_config(request_data: ExecutionCreate) -> Optional[dict[str, Any]]:
        """Build the processing_config JSONB payload. None (not {}) when nothing was configured,
        so a proper subset of evidence types is distinguishable from "no restriction"."""
        if request_data.evidence_types:
            return {PROCESSING_CONFIG_KEY_EVIDENCE_TYPES: request_data.evidence_types}
        return None

    @staticmethod
    def _build_estimates(row_count: int) -> tuple[Optional[Decimal], Optional[int]]:
        if row_count <= 0:
            return None, None

        cost_per_row = Decimal(
            str(getattr(settings, "ESTIMATED_COST_PER_INPUT_ROW", _DEFAULT_ESTIMATED_COST_PER_ROW))
        )
        time_per_row_seconds = float(
            getattr(settings, "ESTIMATED_TIME_SECONDS_PER_INPUT_ROW", _DEFAULT_ESTIMATED_TIME_PER_ROW_SECONDS)
        )
        if time_per_row_seconds <= 0:
            time_per_row_seconds = float(_DEFAULT_ESTIMATED_TIME_PER_ROW_SECONDS)

        estimated_cost = (Decimal(row_count) * cost_per_row).quantize(Decimal("0.0001"))
        estimated_time_seconds = max(1, int(round(row_count * time_per_row_seconds)))
        return estimated_cost, estimated_time_seconds

    @staticmethod
    def _flatten_column_mapping_headers(value: Any) -> list[str]:
        headers: list[str] = []
        if isinstance(value, str):
            normalized = value.strip()
            if normalized:
                headers.append(normalized)
            return headers

        if isinstance(value, dict):
            for nested_value in value.values():
                headers.extend(ExecutionService._flatten_column_mapping_headers(nested_value))
            return headers

        if isinstance(value, list):
            for nested_value in value:
                headers.extend(ExecutionService._flatten_column_mapping_headers(nested_value))
            return headers

        return headers

    @staticmethod
    def _decode_csv_bytes(file_bytes: bytes) -> str:
        for encoding in ("utf-8-sig", "utf-8", "latin-1"):
            try:
                return file_bytes.decode(encoding)
            except UnicodeDecodeError:
                continue
        raise HTTPException(
            status_code=status.HTTP_400_BAD_REQUEST,
            detail="Unable to decode CSV file. Please upload a UTF-8 compatible CSV.",
        )

    @classmethod
    def _extract_headers_and_row_count(cls, file_bytes: bytes, label: str) -> tuple[list[str], int]:
        decoded_content = cls._decode_csv_bytes(file_bytes)
        reader = csv.reader(io.StringIO(decoded_content))

        try:
            raw_headers = next(reader)
        except StopIteration as exc:
            raise HTTPException(
                status_code=status.HTTP_400_BAD_REQUEST,
                detail=f"{label} is empty.",
            ) from exc

        headers = [header.strip() for header in raw_headers if header and header.strip()]
        if not headers:
            raise HTTPException(
                status_code=status.HTTP_400_BAD_REQUEST,
                detail=f"{label} is missing a valid header row.",
            )

        row_count = 0
        for row in reader:
            if any((cell or "").strip() for cell in row):
                row_count += 1

        return headers, row_count

    @classmethod
    def _parse_csv_preview(
        cls,
        file_bytes: bytes,
        label: str,
        *,
        preview_limit: int = 10,
        tracked_column: Optional[str] = None,
    ) -> tuple[list[str], int, list[dict[str, str]], set[str]]:
        decoded_content = cls._decode_csv_bytes(file_bytes)
        reader = csv.reader(io.StringIO(decoded_content))

        try:
            raw_headers = next(reader)
        except StopIteration as exc:
            raise HTTPException(
                status_code=status.HTTP_400_BAD_REQUEST,
                detail=f"{label} is empty.",
            ) from exc

        header_cells = [(cell or "").strip() for cell in raw_headers]
        headers: list[str] = []
        header_to_index: dict[str, int] = {}
        for index, header in enumerate(header_cells):
            if not header or header in header_to_index:
                continue
            headers.append(header)
            header_to_index[header] = index

        if not headers:
            raise HTTPException(
                status_code=status.HTTP_400_BAD_REQUEST,
                detail=f"{label} is missing a valid header row.",
            )

        row_count = 0
        preview_rows: list[dict[str, str]] = []
        tracked_values: set[str] = set()
        tracked_key = (tracked_column or "").strip()

        for row in reader:
            if not any((cell or "").strip() for cell in row):
                continue

            row_count += 1
            row_payload: dict[str, str] = {}
            for header in headers:
                index = header_to_index.get(header, -1)
                value = (row[index] or "").strip() if index >= 0 and index < len(row) else ""
                row_payload[header] = value

            if len(preview_rows) < preview_limit:
                preview_rows.append(row_payload)

            if tracked_key:
                tracked_value = row_payload.get(tracked_key, "").strip()
                if tracked_value:
                    tracked_values.add(tracked_value)

        return headers, row_count, preview_rows, tracked_values

    def _collect_required_input_headers(self, source_type: CsvSourceType) -> list[str]:
        required_headers = self._flatten_column_mapping_headers(source_type.column_mappings or {})

        evidence_columns = source_type.evidence_columns or []
        if isinstance(evidence_columns, list):
            for evidence_config in evidence_columns:
                if not isinstance(evidence_config, dict):
                    continue
                column_name = str(evidence_config.get("column", "")).strip()
                if column_name:
                    required_headers.append(column_name)

        # Preserve order while removing duplicates.
        unique_headers: list[str] = []
        seen: set[str] = set()
        for header in required_headers:
            normalized = header.strip()
            if not normalized:
                continue
            if normalized in seen:
                continue
            seen.add(normalized)
            unique_headers.append(normalized)

        return unique_headers

    def _validate_input_csv_metadata(
        self,
        headers: list[str],
        row_count: int,
        source_type: CsvSourceType,
    ) -> None:
        header_set = set(headers)

        max_rows = source_type.max_rows_per_upload
        if isinstance(max_rows, int) and max_rows > 0 and row_count > max_rows:
            raise HTTPException(
                status_code=status.HTTP_400_BAD_REQUEST,
                detail=f"Input file exceeds max rows limit ({max_rows}).",
            )

        required_headers = self._collect_required_input_headers(source_type)
        missing_headers = [header for header in required_headers if header not in header_set]
        if missing_headers:
            raise HTTPException(
                status_code=status.HTTP_400_BAD_REQUEST,
                detail=f"Input file is missing required columns: {', '.join(missing_headers)}",
            )

    def _validate_input_csv_against_source(
        self,
        input_file_bytes: bytes,
        source_type: CsvSourceType,
    ) -> None:
        headers, row_count = self._extract_headers_and_row_count(input_file_bytes, "Input file")
        self._validate_input_csv_metadata(headers, row_count, source_type)

    @staticmethod
    def _resolve_required_question_columns(source_type: CsvSourceType) -> list[str]:
        question_config = source_type.question_config or {}
        if not isinstance(question_config, dict):
            question_config = {}

        mandatory_columns = question_config.get("mandatory_columns", [])
        if not isinstance(mandatory_columns, list):
            mandatory_columns = []

        evidence_context_config = source_type.evidence_context_config or {}
        title_column = str(evidence_context_config.get("title_column", "")).strip()

        required_columns: list[str] = []
        for column in mandatory_columns:
            column_str = str(column).strip()
            if not column_str:
                continue
            if column_str == "evidence_context_config.title_column" and title_column:
                required_columns.append(title_column)
                continue
            required_columns.append(column_str)

        return required_columns

    def _validate_questions_csv_metadata(
        self,
        headers: list[str],
        source_type: CsvSourceType,
    ) -> None:
        header_set = set(headers)
        required_columns = self._resolve_required_question_columns(source_type)
        missing_columns = [column for column in required_columns if column not in header_set]
        if missing_columns:
            raise HTTPException(
                status_code=status.HTTP_400_BAD_REQUEST,
                detail=f"Questions file is missing mandatory columns: {', '.join(missing_columns)}",
            )

    def _validate_questions_csv_against_source(
        self,
        questions_file_bytes: bytes,
        source_type: CsvSourceType,
    ) -> None:
        headers, _ = self._extract_headers_and_row_count(questions_file_bytes, "Questions file")
        self._validate_questions_csv_metadata(headers, source_type)

    @staticmethod
    def _normalize_task_string(value: str) -> str:
        """Normalize a task string for reliable cross-file matching.

        Handles: leading/trailing single/double quotes, extra internal whitespace,
        Unicode NFC normalization (critical for Hindi/Devanagari text where the
        same glyph can be encoded as precomposed NFC or decomposed NFD).
        """
        # NFC normalization first so subsequent operations work on stable codepoints
        value = unicodedata.normalize("NFC", value)
        # Strip surrounding whitespace, then surrounding quote characters
        value = value.strip().strip("\"'").strip()
        # Collapse runs of internal whitespace (including NBSP U+00A0, ZWSP, etc.)
        value = re.sub(r"[\s ​‌‍﻿]+", " ", value).strip()
        return value

    @staticmethod
    def _validate_tasks_cross_reference_from_values(input_tasks: set[str], questions_tasks: set[str]) -> None:
        normalized_input = {ExecutionService._normalize_task_string(t) for t in input_tasks}
        normalized_questions = {ExecutionService._normalize_task_string(t) for t in questions_tasks}
        # Build map from normalized → original so error messages show readable originals
        questions_norm_to_orig = {
            ExecutionService._normalize_task_string(t): t for t in questions_tasks
        }
        missing_normalized = normalized_questions - normalized_input
        if not missing_normalized:
            return
        missing_tasks = {questions_norm_to_orig.get(n, n) for n in missing_normalized}

        missing_list = sorted(missing_tasks)[:5]
        missing_display = ", ".join(missing_list)
        if len(missing_tasks) > 5:
            missing_display += f" (and {len(missing_tasks) - 5} more)"
        raise HTTPException(
            status_code=status.HTTP_400_BAD_REQUEST,
            detail=f"Questions file references tasks not found in input file: {missing_display}",
        )

    def _validate_tasks_cross_reference(
        self,
        input_file_bytes: bytes,
        questions_file_bytes: bytes,
        source_type: CsvSourceType,
    ) -> None:
        """Validate that tasks mentioned in questions CSV exist in input CSV."""
        evidence_context_config = source_type.evidence_context_config or {}
        title_column = evidence_context_config.get("title_column", "")
        
        if not title_column:
            return  # No title column configured, skip cross-validation

        _, _, _, input_tasks = self._parse_csv_preview(
            input_file_bytes,
            "Input file",
            tracked_column=title_column,
        )
        _, _, _, questions_tasks = self._parse_csv_preview(
            questions_file_bytes,
            "Questions file",
            tracked_column=title_column,
        )
        self._validate_tasks_cross_reference_from_values(input_tasks, questions_tasks)

    @staticmethod
    def _extract_missing_columns_from_detail(detail: str) -> list[str]:
        if ":" not in detail:
            return []
        missing = detail.split(":", 1)[1].strip()
        return [item.strip() for item in missing.split(",") if item.strip()]

    @staticmethod
    def _to_user_friendly_validation_message(detail: str, file_label: str) -> str:
        normalized = (detail or "").strip()
        if not normalized:
            return f"We found an issue in the {file_label}. Please review and re-upload the file."

        normalized_lower = normalized.lower()
        file_label_lower = file_label.lower()

        if normalized.endswith("is missing.") and normalized_lower.startswith(file_label_lower):
            return f"{file_label} is missing. Please upload it and try again."

        if "missing required columns:" in normalized_lower or "missing mandatory columns:" in normalized_lower:
            missing_columns = ExecutionService._extract_missing_columns_from_detail(normalized)
            if len(missing_columns) == 1:
                return f"Missing required column: {missing_columns[0]}. Please fix the CSV and re-upload."
            if missing_columns:
                return f"Missing required columns: {', '.join(missing_columns)}. Please fix the CSV and re-upload."
            return "Required columns are missing. Please fix the CSV and re-upload."

        if "references tasks not found in input file:" in normalized_lower:
            missing_tasks = normalized.split(":", 1)[1].strip() if ":" in normalized else ""
            if missing_tasks:
                return (
                    "Some tasks in the Criteria file were not found in the Input file: "
                    f"{missing_tasks}. Please fix and re-upload."
                )
            return "Some tasks in the Criteria file were not found in the Input file. Please fix and re-upload."

        if "could not be downloaded" in normalized_lower or "could not be read" in normalized_lower:
            return f"We could not read the {file_label_lower}. Please re-upload the file and try again."

        if "unable to decode csv file" in normalized_lower:
            return f"The {file_label_lower} is not UTF-8 compatible. Please upload a UTF-8 CSV file."

        if normalized_lower.endswith("is empty."):
            return f"The {file_label_lower} is empty. Please upload a CSV with headers and data."

        if "missing a valid header row" in normalized_lower:
            return f"The {file_label_lower} header row is missing or invalid. Please fix and re-upload."

        if "exceeds max rows limit" in normalized_lower:
            return (
                f"The {file_label_lower} has more rows than allowed. "
                "Please reduce the row count and re-upload."
            )

        return f"We found an issue while validating the {file_label_lower}. Please review and re-upload."

    @staticmethod
    def _validate_scope_metadata(
        request_data: Any,
        source_type: CsvSourceType,
    ) -> None:
        if source_type.has_geo and not (request_data.states or []):
            raise HTTPException(
                status_code=status.HTTP_400_BAD_REQUEST,
                detail="At least one state is required for the selected CSV source type.",
            )

    def _mark_execution_failed(self, execution: Execution, reason: str) -> None:
        failed_at = datetime.utcnow()
        execution.status = "failed"
        execution.failure_reason = reason
        execution.processing_completed_at = failed_at
        execution.completed_at = failed_at
        self.db.commit()
        self.db.refresh(execution)
        EmailService.notify_execution_status(self.db, execution)

    @staticmethod
    def _ensure_not_started_for_file_changes(execution: Execution) -> None:
        """Allow file uploads/validation changes only before analysis is started."""
        normalized_status = (execution.status or "").strip().lower()
        if normalized_status in {"draft", "validated"}:
            return

        raise HTTPException(
            status_code=status.HTTP_400_BAD_REQUEST,
            detail=(
                "Analysis has already started or ended. "
                "You can re-upload files only before starting analysis."
            ),
        )

    @staticmethod
    def _normalize_file_type(file_type: str) -> str:
        normalized = (file_type or "").strip().lower()
        if normalized == "criterias":
            return "questions"
        if normalized in {"input", "questions"}:
            return normalized
        raise HTTPException(
            status_code=status.HTTP_400_BAD_REQUEST,
            detail="file_type must be one of: input, questions, criterias",
        )

    @staticmethod
    def _get_file_path_by_type(execution: Execution, file_type: str) -> Optional[str]:
        if file_type == "input":
            return execution.input_file_url
        if file_type in {"questions", "criterias"}:
            return execution.criterias_file_url
        return None

    @staticmethod
    def _set_file_path_by_type(execution: Execution, file_type: str, file_path: str) -> None:
        if file_type == "input":
            execution.input_file_url = file_path
            return
        if file_type in {"questions", "criterias"}:
            execution.criterias_file_url = file_path
            return
        raise HTTPException(
            status_code=status.HTTP_400_BAD_REQUEST,
            detail="Unsupported file type.",
        )

    @staticmethod
    def _set_file_size_by_type(execution: Execution, file_type: str, size_bytes: int) -> None:
        if file_type == "input":
            execution.input_file_size = size_bytes
            return
        if file_type in {"questions", "criterias"}:
            execution.criterias_file_size = size_bytes
            return
        raise HTTPException(
            status_code=status.HTTP_400_BAD_REQUEST,
            detail="Unsupported file type.",
        )

    @staticmethod
    def _read_single_checkpoint_file(checkpoint_file: Path) -> dict[str, Any]:
        """Read one processor checkpoint file's `_metadata` block, if it exists."""
        if not checkpoint_file.exists():
            return {}

        with open(checkpoint_file, 'r') as f:
            checkpoint_data = json.load(f)

        metadata = checkpoint_data.get('_metadata', {})
        return {
            'total_processed': metadata.get('total_processed', 0),
            'total_files': metadata.get('total_files', 0),
            'last_updated': metadata.get('last_updated'),
        }

    @staticmethod
    def _read_processor_checkpoint_file(execution_id: UUID) -> dict[str, Any]:
        """
        Read real-time row progress for an in-progress execution.

        Without main-batch splitting there is a single processor_output/ checkpoint.
        With it on, each main batch has its own processor_output/batch_NNN/ checkpoint —
        completed batches are counted from their merged_output.csv (exact; the checkpoint
        for a finished batch is already gone), and the first batch without a
        merged_output.csv yet is the one currently in flight, whose own checkpoint
        supplies the live in-progress count.
        """
        try:
            execution_root = Path(settings.EXECUTION_WORKSPACE_ROOT) / str(execution_id)
            manifest_file = execution_root / "main_batches" / "batch_manifest.json"

            if not manifest_file.exists():
                return ExecutionService._read_single_checkpoint_file(
                    execution_root / "processor_output" / ".processing_checkpoint.json"
                )

            from services.execution_processor import _count_csv_rows

            with open(manifest_file, 'r') as f:
                total_batches = json.load(f).get('total_batches', 0)

            total_processed = 0
            last_updated = None
            for batch_index in range(total_batches):
                batch_output_dir = execution_root / "processor_output" / f"batch_{batch_index:03d}"
                merged_output_csv = batch_output_dir / "merged_output.csv"
                if merged_output_csv.exists():
                    row_count, _ = _count_csv_rows(merged_output_csv)
                    total_processed += row_count
                    continue

                # First batch without a merged output yet is the one in flight.
                live_checkpoint = ExecutionService._read_single_checkpoint_file(
                    batch_output_dir / ".processing_checkpoint.json"
                )
                total_processed += live_checkpoint.get('total_processed', 0)
                last_updated = live_checkpoint.get('last_updated')
                break

            return {
                'total_processed': total_processed,
                'total_files': 1,
                'last_updated': last_updated,
            }
        except Exception as e:
            logger.debug(f"Could not read processor checkpoint for execution {execution_id}: {e}")
            return {}

    @staticmethod
    async def _read_processor_checkpoint_file_async(execution_id: UUID) -> dict[str, Any]:
        """Non-blocking wrapper: offloads sync file I/O to a thread pool."""
        return await asyncio.to_thread(
            ExecutionService._read_processor_checkpoint_file, execution_id
        )

    @staticmethod
    def _checkpoint(execution: Execution) -> dict[str, Any]:
        checkpoint = deepcopy(execution.checkpoint_data) if isinstance(execution.checkpoint_data, dict) else {}
        files = checkpoint.get("files")
        if not isinstance(files, dict):
            files = {}
        for file_type in ("input", "questions"):
            file_state = files.get(file_type)
            if not isinstance(file_state, dict):
                files[file_type] = {}
        checkpoint["files"] = files
        return checkpoint

    @staticmethod
    def _checkpoint_file_status(checkpoint: dict[str, Any], file_type: str) -> ExecutionFileCheckpointState:
        files = checkpoint.get("files")
        if not isinstance(files, dict):
            files = {}
        file_data = files.get(file_type)
        if not isinstance(file_data, dict):
            file_data = {}

        raw_columns = file_data.get("columns_detected")
        columns_detected = [str(column).strip() for column in raw_columns if str(column).strip()] if isinstance(raw_columns, list) else []

        raw_missing = file_data.get("missing_columns")
        missing_columns = [str(column).strip() for column in raw_missing if str(column).strip()] if isinstance(raw_missing, list) else []

        rows_value = file_data.get("rows_detected")
        rows_detected = int(rows_value) if isinstance(rows_value, (int, float)) else None

        return ExecutionFileCheckpointState(
            uploaded=bool(file_data.get("uploaded")),
            validated=bool(file_data.get("validated")),
            rows_detected=rows_detected,
            columns_detected=columns_detected,
            message=str(file_data.get("message")).strip() if file_data.get("message") is not None else None,
            missing_columns=missing_columns,
            updated_at=str(file_data.get("updated_at")).strip() if file_data.get("updated_at") is not None else None,
        )

    def _update_file_checkpoint(
        self,
        execution: Execution,
        *,
        file_type: str,
        uploaded: Optional[bool] = None,
        validated: Optional[bool] = None,
        rows_detected: Optional[int] = None,
        columns_detected: Optional[list[str]] = None,
        message: Optional[str] = None,
        missing_columns: Optional[list[str]] = None,
    ) -> None:
        checkpoint = self._checkpoint(execution)
        file_checkpoint = checkpoint["files"][file_type]

        if uploaded is not None:
            file_checkpoint["uploaded"] = uploaded
        if validated is not None:
            file_checkpoint["validated"] = validated
        if rows_detected is not None:
            file_checkpoint["rows_detected"] = rows_detected
        if columns_detected is not None:
            file_checkpoint["columns_detected"] = columns_detected
        if message is not None:
            file_checkpoint["message"] = message
        if missing_columns is not None:
            file_checkpoint["missing_columns"] = missing_columns

        file_checkpoint["updated_at"] = datetime.utcnow().isoformat()
        execution.checkpoint_data = checkpoint

    async def _resolve_input_rows_for_estimation(self, execution: Execution) -> Optional[int]:
        checkpoint = self._checkpoint(execution)
        input_rows = checkpoint["files"]["input"].get("rows_detected")
        if isinstance(input_rows, (int, float)) and int(input_rows) > 0:
            return int(input_rows)

        if not execution.input_file_url:
            return None

        input_bytes = await self.storage_service.download_file(execution.input_file_url)
        if not input_bytes:
            return None

        _, row_count = self._extract_headers_and_row_count(input_bytes, "Input file")
        return row_count if row_count > 0 else None

    async def _apply_execution_estimates(self, execution: Execution) -> None:
        input_rows = await self._resolve_input_rows_for_estimation(execution)
        if input_rows is None:
            execution.estimated_cost = None
            execution.estimated_time_seconds = None
            return

        estimated_cost, estimated_time_seconds = self._build_estimates(input_rows)
        execution.total_rows = input_rows
        execution.estimated_cost = estimated_cost
        execution.estimated_time_seconds = estimated_time_seconds

    def _get_execution_or_404(self, execution_id: UUID, user_id: str) -> Execution:
        execution = self.db.query(Execution).filter(
            Execution.id == execution_id,
            Execution.created_by == user_id,
        ).first()

        if not execution:
            raise HTTPException(
                status_code=status.HTTP_404_NOT_FOUND,
                detail="Execution not found",
            )
        return execution

    async def create_execution_draft(
        self,
        request_data: ExecutionCreateRequest,
        current_user: UserResponse,
    ) -> ExecutionResponse:
        """Step 1: create analysis draft only."""
        tenant_code, organization_code = self._resolve_scope(current_user)
        source_type = self._get_csv_source_type(
            tenant_code=tenant_code,
            organization_code=organization_code,
            type_key=request_data.csv_type_id,
        )
        if not source_type:
            raise HTTPException(
                status_code=status.HTTP_400_BAD_REQUEST,
                detail="Invalid or inactive csv_type_id for this tenant/organization.",
            )
        self._validate_scope_metadata(request_data, source_type)
        criterias_mode = self._resolve_criterias_mode(source_type)
        # threshold_config exists solely to carry the per-(user, task) relevant-evidence
        # cap to the processor; None when no cap was requested (no seeded default).
        threshold_config = (
            {"max_relevant_per_user_task": request_data.evidence_threshold}
            if request_data.evidence_threshold is not None
            else None
        )
        processing_config = self._resolve_processing_config(request_data)

        execution = Execution(
            tenant_code=tenant_code,
            organization_code=organization_code,
            name=request_data.name,
            csv_type_id=source_type.type_key,
            ai_model_id=request_data.ai_model_id or get_llm_model_name(),
            program_ref_id=request_data.program_ref_id,
            program_name=request_data.program_name,
            states=request_data.states or [],
            criterias_mode=criterias_mode,
            threshold_config=threshold_config,
            processing_config=processing_config,
            status="draft",
            created_by=current_user.id,
            checkpoint_data={"files": {"input": {}, "questions": {}}},
        )
        self.db.add(execution)
        self.db.commit()
        self.db.refresh(execution)
        return self._to_execution_response(execution)

    async def get_bulk_downloadable_urls(
        self,
        request_data: CloudDownloadableUrlRequest,
        current_user: UserResponse,
    ) -> CloudDownloadableUrlResponse:
        """
        Generate signed downloadable URLs for existing files.
        Contract:
        {
          "filePaths": [
            "tenant_code/org_code/userId/executions/file1.csv"
          ]
        }
        """
        tenant_code, organization_code = self._resolve_scope(current_user)
        current_user_id = (current_user.id or "").strip()
        if not current_user_id:
            raise HTTPException(
                status_code=status.HTTP_400_BAD_REQUEST,
                detail="Current user id is required to generate downloadable URLs.",
            )

        result: list[dict[str, str]] = []
        for raw_file_path in request_data.filePaths:
            (
                normalized_file_path,
                path_tenant,
                path_organization,
                path_user_id,
            ) = self._validate_downloadable_file_path(raw_file_path)

            self._enforce_download_path_scope(
                tenant_code=tenant_code,
                organization_code=organization_code,
                current_user_id=current_user_id,
                path_tenant=path_tenant,
                path_organization=path_organization,
                path_user_id=path_user_id,
                file_path=normalized_file_path,
            )

            absolute_file_path = f"/{normalized_file_path}"
            try:
                file_metadata = await self.storage_service.get_file_metadata(absolute_file_path)
            except HTTPException:
                raise
            except Exception as exc:
                logger.exception("Failed to load metadata for filePath=%s", normalized_file_path)
                raise HTTPException(
                    status_code=status.HTTP_500_INTERNAL_SERVER_ERROR,
                    detail=f"Failed to read file metadata for: {normalized_file_path}",
                ) from exc

            if file_metadata is None:
                raise HTTPException(
                    status_code=status.HTTP_404_NOT_FOUND,
                    detail=f"File not found in cloud storage: {normalized_file_path}",
                )

            try:
                signed_download = await self.storage_service.generate_download_url(
                    file_path=absolute_file_path,
                    expiration=settings.SIGNED_DOWNLOAD_URL_EXPIRY_SECONDS,
                    response_filename=Path(normalized_file_path).name,
                )
            except HTTPException:
                raise
            except Exception as exc:
                logger.exception("Failed to generate downloadable URL for filePath=%s", normalized_file_path)
                raise HTTPException(
                    status_code=status.HTTP_500_INTERNAL_SERVER_ERROR,
                    detail=f"Failed to generate downloadable URL for: {normalized_file_path}",
                ) from exc

            result.append(
                {
                    "cloudStorage": self._cloud_storage_label(),
                    "filePath": normalized_file_path,
                    "url": signed_download["url"],
                }
            )

        return CloudDownloadableUrlResponse(
            responseCode="OK",
            message="Download Url Generated Successfully.",
            result=result,
            meta={},
        )

    async def get_bulk_signed_upload_urls(
        self,
        request_data: CloudSignedUrlRequest,
        user_id: str,
    ) -> CloudSignedUrlResponse:
        """
        Generate signed upload URLs in common cloud-services format.
        Contract:
        {
          "request": { "<executionId>": { "files": ["a.csv", "b.csv"] } },
          "ref": "execution"
        }
        """
        if (request_data.ref or "").strip().lower() != "execution":
            raise HTTPException(
                status_code=status.HTTP_400_BAD_REQUEST,
                detail="Unsupported ref. Use ref=execution.",
            )

        result: dict[str, dict[str, Any]] = {}
        for execution_key, request_item in request_data.request.items():
            try:
                execution_uuid = UUID(str(execution_key))
            except ValueError as exc:
                raise HTTPException(
                    status_code=status.HTTP_400_BAD_REQUEST,
                    detail=f"Invalid execution id: {execution_key}",
                ) from exc

            execution = self._get_execution_or_404(execution_uuid, user_id)
            self._ensure_not_started_for_file_changes(execution)

            file_names = request_item.files or []
            if not file_names:
                raise HTTPException(
                    status_code=status.HTTP_400_BAD_REQUEST,
                    detail=f"No files provided for execution {execution_key}.",
                )
            if len(file_names) > 2:
                raise HTTPException(
                    status_code=status.HTTP_400_BAD_REQUEST,
                    detail=(
                        f"Execution {execution_key} supports max 2 files "
                        "(input + questions) per request."
                    ),
                )

            used_types: set[str] = set()
            file_results: list[dict[str, Any]] = []
            for original_file_name in file_names:
                safe_file_name = self._validate_file_name_for_signed_url(original_file_name, "File")
                file_type = self._infer_file_type(safe_file_name, used_types)
                used_types.add(file_type)

                prefixed_name = f"{file_type}_{safe_file_name}"
                file_path = self.storage_service.build_execution_file_path(
                    user_id=user_id,
                    execution_id=str(execution.id),
                    file_name=prefixed_name,
                )

                self._set_file_path_by_type(execution, file_type, file_path)
                self._update_file_checkpoint(
                    execution,
                    file_type=file_type,
                    uploaded=False,
                    validated=False,
                    rows_detected=0,
                    columns_detected=[],
                    message=None,
                    missing_columns=[],
                )

                signed_upload = await self.storage_service.generate_upload_url(
                    file_path=file_path,
                    content_type="text/csv",
                    expiration=settings.SIGNED_UPLOAD_URL_EXPIRY_SECONDS,
                )
                signed_download = await self.storage_service.generate_download_url(
                    file_path=file_path,
                    expiration=settings.SIGNED_DOWNLOAD_URL_EXPIRY_SECONDS,
                )

                normalized_path = file_path.lstrip("/")
                file_results.append(
                    {
                        "file": normalized_path,
                        "url": signed_upload["url"],
                        "downloadableUrl": signed_download["url"],
                        "payload": {
                            "sourcePath": normalized_path,
                            "fileType": file_type,
                        },
                        "cloudStorage": self._cloud_storage_label(),
                    }
                )

            execution.status = "draft"
            execution.failure_reason = None
            self.db.commit()
            self.db.refresh(execution)

            result[str(execution_key)] = {"files": file_results}

        return CloudSignedUrlResponse(
            responseCode="OK",
            message="Signed Url Generated Successfully.",
            result=result,
            meta={},
        )

    async def get_file_upload_url(
        self,
        execution_id: UUID,
        file_type: str,
        request_data: FileUploadUrlRequest,
        user_id: str,
    ) -> ExecutionFileUploadUrlResponse:
        """Step 2: create signed upload URL for one file section."""
        normalized_file_type = self._normalize_file_type(file_type)

        execution = self._get_execution_or_404(execution_id, user_id)
        self._ensure_not_started_for_file_changes(execution)

        label = "Input file" if normalized_file_type == "input" else "Questions file"
        safe_file_name, normalized_content_type = self._validate_file_descriptor(
            request_data.file.file_name,
            request_data.file.size_bytes,
            request_data.file.content_type,
            label,
        )

        prefixed_name = f"{normalized_file_type}_{safe_file_name}"
        file_path = self.storage_service.build_execution_file_path(
            user_id=user_id,
            execution_id=str(execution.id),
            file_name=prefixed_name,
        )
        self._set_file_path_by_type(execution, normalized_file_type, file_path)
        self._set_file_size_by_type(execution, normalized_file_type, int(request_data.file.size_bytes))
        self._update_file_checkpoint(
            execution,
            file_type=normalized_file_type,
            uploaded=False,
            validated=False,
            rows_detected=0,
            columns_detected=[],
            message=None,
            missing_columns=[],
        )
        execution.status = "draft"
        self.db.commit()
        self.db.refresh(execution)

        signed_upload = await self.storage_service.generate_upload_url(
            file_path=file_path,
            content_type=normalized_content_type,
            expiration=settings.SIGNED_UPLOAD_URL_EXPIRY_SECONDS,
        )
        return ExecutionFileUploadUrlResponse(
            execution_id=execution.id,
            file_type=normalized_file_type,
            upload=SignedUrlPayload(**signed_upload),
        )

    async def complete_file_upload(
        self,
        execution_id: UUID,
        file_type: str,
        user_id: str,
    ) -> ExecutionFileCompleteResponse:
        """Step 2: confirm upload, detect rows/columns and persist preview metadata."""
        normalized_file_type = self._normalize_file_type(file_type)

        execution = self._get_execution_or_404(execution_id, user_id)
        self._ensure_not_started_for_file_changes(execution)
        file_path = self._get_file_path_by_type(execution, normalized_file_type)
        if not file_path:
            raise HTTPException(
                status_code=status.HTTP_400_BAD_REQUEST,
                detail=f"{normalized_file_type} file path is missing. Request a new upload URL first.",
            )

        metadata = await self.storage_service.get_file_metadata(file_path)
        if not metadata:
            raise HTTPException(
                status_code=status.HTTP_400_BAD_REQUEST,
                detail=f"{normalized_file_type} file upload not found",
            )

        label = "Input file" if normalized_file_type == "input" else "Questions file"
        self._validate_uploaded_metadata(metadata, label)

        file_bytes = await self.storage_service.download_file(file_path)
        if not file_bytes:
            raise HTTPException(
                status_code=status.HTTP_400_BAD_REQUEST,
                detail=f"{normalized_file_type} file could not be read for metadata detection.",
            )

        headers, row_count = self._extract_headers_and_row_count(file_bytes, label)
        self._set_file_size_by_type(execution, normalized_file_type, int(metadata.get("size_bytes", 0)))
        self._update_file_checkpoint(
            execution,
            file_type=normalized_file_type,
            uploaded=True,
            validated=False,
            rows_detected=row_count,
            columns_detected=headers,
            message="File uploaded",
            missing_columns=[],
        )
        execution.status = "draft"
        execution.failure_reason = None
        self.db.commit()
        self.db.refresh(execution)

        return ExecutionFileCompleteResponse(
            execution_id=execution.id,
            file_type=normalized_file_type,
            uploaded=True,
            rows_detected=row_count,
            columns_detected=headers,
        )

    async def upload_file_direct(
        self,
        execution_id: UUID,
        file_type: str,
        file_name: str,
        content_type: Optional[str],
        file_bytes: bytes,
        user_id: str,
    ) -> ExecutionFileCompleteResponse:
        """Direct upload through backend (fallback when browser-to-cloud CORS fails)."""
        normalized_file_type = self._normalize_file_type(file_type)

        execution = self._get_execution_or_404(execution_id, user_id)
        self._ensure_not_started_for_file_changes(execution)

        label = "Input file" if normalized_file_type == "input" else "Questions file"
        safe_file_name, normalized_content_type = self._validate_file_descriptor(
            file_name,
            len(file_bytes),
            content_type,
            label,
        )

        # Build file path and upload to storage
        prefixed_name = f"{normalized_file_type}_{safe_file_name}"
        file_path = self.storage_service.build_execution_file_path(
            user_id=user_id,
            execution_id=str(execution.id),
            file_name=prefixed_name,
        )

        # Upload file to storage
        await self.storage_service.upload_file(
            file_content=file_bytes,
            file_path=file_path,
            content_type=normalized_content_type,
        )

        # Extract headers and row count
        headers, row_count = self._extract_headers_and_row_count(file_bytes, label)

        # Update execution with file path and metadata
        self._set_file_path_by_type(execution, normalized_file_type, file_path)
        self._set_file_size_by_type(execution, normalized_file_type, len(file_bytes))
        self._update_file_checkpoint(
            execution,
            file_type=normalized_file_type,
            uploaded=True,
            validated=False,
            rows_detected=row_count,
            columns_detected=headers,
            message="File uploaded",
            missing_columns=[],
        )
        execution.status = "draft"
        execution.failure_reason = None
        self.db.commit()
        self.db.refresh(execution)

        return ExecutionFileCompleteResponse(
            execution_id=execution.id,
            file_type=normalized_file_type,
            uploaded=True,
            rows_detected=row_count,
            columns_detected=headers,
        )

    async def validate_execution_files(self, execution_id: UUID, user_id: str) -> ExecutionValidationResponse:
        """Step 3: validate both uploaded CSV files against source-type rules."""
        execution = self._get_execution_or_404(execution_id, user_id)
        self._ensure_not_started_for_file_changes(execution)
        source_type = self._get_csv_source_type(
            tenant_code=execution.tenant_code,
            organization_code=execution.organization_code,
            type_key=execution.csv_type_id or "",
        )
        if not source_type:
            raise HTTPException(
                status_code=status.HTTP_400_BAD_REQUEST,
                detail="CSV source type configuration not found or inactive.",
            )

        input_result = FileValidationResult(file_type="input", valid=False)
        questions_result = FileValidationResult(file_type="questions", valid=False)

        input_path = execution.input_file_url
        questions_path = execution.criterias_file_url
        evidence_context_config = source_type.evidence_context_config or {}
        title_column = str(evidence_context_config.get("title_column", "")).strip()
        input_tasks: set[str] = set()
        questions_tasks: set[str] = set()

        if not input_path:
            input_result.message = "Input file is missing. Please upload it and try again."
        if not questions_path:
            questions_result.message = "Questions file is missing. Please upload it and try again."

        if input_path:
            try:
                input_bytes = await self.storage_service.download_file(input_path)
                if not input_bytes:
                    raise HTTPException(status_code=400, detail="Input file could not be downloaded.")
                headers, row_count, preview_rows, input_tasks = self._parse_csv_preview(
                    input_bytes,
                    "Input file",
                    tracked_column=title_column,
                )
                input_result.rows_detected = row_count
                input_result.columns_detected = headers
                input_result.preview_rows = preview_rows
                self._validate_input_csv_metadata(headers, row_count, source_type)
                input_result.valid = True
                input_result.message = f"✓ Input file validated successfully! ({row_count:,} rows, {len(headers)} columns)"
                input_result.missing_columns = []
            except HTTPException as exc:
                detail = str(exc.detail)
                input_result.message = self._to_user_friendly_validation_message(detail, "Input file")
                if detail.startswith("Input file is missing required columns:"):
                    input_result.missing_columns = self._extract_missing_columns_from_detail(detail)

        if questions_path:
            try:
                questions_bytes = await self.storage_service.download_file(questions_path)
                if not questions_bytes:
                    raise HTTPException(status_code=400, detail="Questions file could not be downloaded.")
                headers, row_count, preview_rows, questions_tasks = self._parse_csv_preview(
                    questions_bytes,
                    "Questions file",
                    tracked_column=title_column,
                )
                questions_result.rows_detected = row_count
                questions_result.columns_detected = headers
                questions_result.preview_rows = preview_rows
                self._validate_questions_csv_metadata(headers, source_type)
                questions_result.valid = True
                questions_result.message = f"✓ Criteria file validated successfully! ({row_count:,} rows, {len(headers)} columns)"
                questions_result.missing_columns = []
            except HTTPException as exc:
                detail = str(exc.detail)
                questions_result.message = self._to_user_friendly_validation_message(detail, "Questions file")
                if detail.startswith("Questions file is missing mandatory columns:"):
                    questions_result.missing_columns = self._extract_missing_columns_from_detail(detail)
        
        # Cross-validate: Check if tasks in questions exist in input
        if input_result.valid and questions_result.valid and input_path and questions_path and title_column:
            try:
                self._validate_tasks_cross_reference_from_values(input_tasks, questions_tasks)
            except HTTPException as exc:
                # Mark questions as invalid if cross-validation fails
                questions_result.valid = False
                questions_result.message = self._to_user_friendly_validation_message(str(exc.detail), "Questions file")

        self._update_file_checkpoint(
            execution,
            file_type="input",
            validated=input_result.valid,
            message=input_result.message,
            missing_columns=input_result.missing_columns,
        )
        self._update_file_checkpoint(
            execution,
            file_type="questions",
            validated=questions_result.valid,
            message=questions_result.message,
            missing_columns=questions_result.missing_columns,
        )

        is_valid = input_result.valid and questions_result.valid
        execution.status = "validated" if is_valid else "draft"
        execution.failure_reason = None if is_valid else "File validation failed. Upload valid files and retry."
        self.db.commit()
        self.db.refresh(execution)

        return ExecutionValidationResponse(
            execution_id=execution.id,
            is_valid=is_valid,
            input_file=input_result,
            questions_file=questions_result,
        )

    async def start_execution(self, execution_id: UUID, user_id: str) -> ExecutionResponse:
        """Step 4: start analysis only after file validation success."""
        execution = self._get_execution_or_404(execution_id, user_id)

        checkpoint = self._checkpoint(execution)
        input_valid = bool(checkpoint["files"]["input"].get("validated"))
        questions_valid = bool(checkpoint["files"]["questions"].get("validated"))

        # Backward-compatible fallback: some legacy rows can have status=validated
        # while checkpoint flags were not persisted due to JSONB in-place mutation.
        if not (input_valid and questions_valid) and (execution.status or "").strip().lower() == "validated":
            logger.warning(
                "Healing stale checkpoint validation flags for execution=%s before start.",
                execution.id,
            )
            self._update_file_checkpoint(execution, file_type="input", validated=True)
            self._update_file_checkpoint(execution, file_type="questions", validated=True)
            checkpoint = self._checkpoint(execution)
            input_valid = bool(checkpoint["files"]["input"].get("validated"))
            questions_valid = bool(checkpoint["files"]["questions"].get("validated"))

        if not (input_valid and questions_valid):
            raise HTTPException(
                status_code=status.HTTP_400_BAD_REQUEST,
                detail="Files are not validated. Upload valid files and run validation before starting analysis.",
            )

        if execution.status in {"in_progress", "running"}:
            raise HTTPException(
                status_code=status.HTTP_400_BAD_REQUEST,
                detail="Execution is already in progress",
            )
        if execution.status == "queued":
            raise HTTPException(
                status_code=status.HTTP_400_BAD_REQUEST,
                detail="Execution is already queued.",
            )
        if execution.status == "completed":
            raise HTTPException(
                status_code=status.HTTP_400_BAD_REQUEST,
                detail="Execution is already completed.",
            )
        if execution.status == "failed":
            raise HTTPException(
                status_code=status.HTTP_400_BAD_REQUEST,
                detail="Execution failed previously. Use rerun to start a fresh retry.",
            )

        await self._apply_execution_estimates(execution)
        execution.status = "queued"
        execution.upload_completed_at = datetime.utcnow()
        execution.failure_reason = None
        execution.notification_sent = False
        execution.notification_sent_at = None
        self.db.commit()
        self.db.refresh(execution)

        try:
            self.worker.submit_job(execution.id)
        except Exception as exc:
            logger.exception("Failed to enqueue execution %s", execution.id)
            self._mark_execution_failed(execution, f"Failed to enqueue execution: {exc}")
            raise HTTPException(
                status_code=status.HTTP_503_SERVICE_UNAVAILABLE,
                detail="Execution could not be queued. Please try again.",
            ) from exc

        return self._to_execution_response(execution)

    async def rerun_execution(self, execution_id: UUID, user_id: str) -> ExecutionResponse:
        """Rerun a failed execution after clearing runtime/output state."""
        execution = (
            self.db.query(Execution)
            .filter(
                Execution.id == execution_id,
                Execution.created_by == user_id,
            )
            .with_for_update()
            .first()
        )
        if not execution:
            raise HTTPException(
                status_code=status.HTTP_404_NOT_FOUND,
                detail="Execution not found",
            )

        normalized_status = (execution.status or "").strip().lower()
        if normalized_status != "failed":
            raise HTTPException(
                status_code=status.HTTP_400_BAD_REQUEST,
                detail=(
                    "Only failed executions can be rerun. "
                    f"Current status is '{execution.status or 'unknown'}'."
                ),
            )

        checkpoint = self._checkpoint(execution)
        input_valid = bool(checkpoint["files"]["input"].get("validated"))
        questions_valid = bool(checkpoint["files"]["questions"].get("validated"))
        if not (input_valid and questions_valid):
            raise HTTPException(
                status_code=status.HTTP_400_BAD_REQUEST,
                detail=(
                    "Input and questions files are not marked as validated. "
                    "Please re-upload and validate both files before rerun."
                ),
            )

        await self._apply_execution_estimates(execution)
        execution.status = "queued"
        execution.upload_completed_at = datetime.utcnow()
        execution.failure_reason = None
        execution.error_logs = None
        execution.processed_rows = 0
        execution.completed_at = None
        execution.processing_started_at = None
        execution.processing_completed_at = None
        execution.output_file_url = None
        execution.output_file_size = None
        execution.worker_id = None
        execution.retry_count = 0
        execution.actual_cost = None
        execution.average_processing_time = None
        execution.notification_sent = False
        execution.notification_sent_at = None
        self.db.commit()
        self.db.refresh(execution)

        try:
            self.worker.submit_job(execution.id)
        except Exception as exc:
            logger.exception("Failed to enqueue execution rerun %s", execution.id)
            self._mark_execution_failed(execution, f"Failed to enqueue execution: {exc}")
            raise HTTPException(
                status_code=status.HTTP_503_SERVICE_UNAVAILABLE,
                detail="Execution could not be queued for rerun. Please try again.",
            ) from exc

        return self._to_execution_response(execution)

    async def init_execution_upload(
        self,
        request_data: ExecutionUploadInitRequest,
        current_user: UserResponse,
    ) -> ExecutionUploadInitResponse:
        """Create execution and return short-lived signed upload URLs."""
        input_file_name, input_content_type = self._validate_file_descriptor(
            request_data.input_file.file_name,
            request_data.input_file.size_bytes,
            request_data.input_file.content_type,
            "Input file",
        )
        questions_file_name, questions_content_type = self._validate_file_descriptor(
            request_data.questions_file.file_name,
            request_data.questions_file.size_bytes,
            request_data.questions_file.content_type,
            "Questions file",
        )

        tenant_code, organization_code = self._resolve_scope(current_user)

        source_type = self._get_csv_source_type(
            tenant_code=tenant_code,
            organization_code=organization_code,
            type_key=request_data.csv_type_id,
        )
        if not source_type:
            raise HTTPException(
                status_code=status.HTTP_400_BAD_REQUEST,
                detail="Invalid or inactive csv_type_id for this tenant/organization.",
            )
        self._validate_scope_metadata(request_data, source_type)
        criterias_mode = self._resolve_criterias_mode(source_type)
        # threshold_config exists solely to carry the per-(user, task) relevant-evidence
        # cap to the processor; None when no cap was requested (no seeded default).
        threshold_config = (
            {"max_relevant_per_user_task": request_data.evidence_threshold}
            if request_data.evidence_threshold is not None
            else None
        )
        processing_config = self._resolve_processing_config(request_data)

        execution = Execution(
            tenant_code=tenant_code,
            organization_code=organization_code,
            name=request_data.name,
            csv_type_id=source_type.type_key,
            ai_model_id=request_data.ai_model_id or get_llm_model_name(),
            program_ref_id=request_data.program_ref_id,
            program_name=request_data.program_name,
            states=request_data.states or [],
            criterias_mode=criterias_mode,
            threshold_config=threshold_config,
            processing_config=processing_config,
            status="draft",
            created_by=current_user.id,
            input_file_size=request_data.input_file.size_bytes,
            criterias_file_size=request_data.questions_file.size_bytes,
        )

        self.db.add(execution)
        self.db.commit()
        self.db.refresh(execution)

        try:
            input_file_path = self.storage_service.build_execution_file_path(
                user_id=current_user.id,
                execution_id=str(execution.id),
                file_name=f"input_{input_file_name}",
            )
            questions_file_path = self.storage_service.build_execution_file_path(
                user_id=current_user.id,
                execution_id=str(execution.id),
                file_name=f"questions_{questions_file_name}",
            )

            execution.input_file_url = input_file_path
            execution.criterias_file_url = questions_file_path
            self.db.commit()
            self.db.refresh(execution)

            upload_expiry = settings.SIGNED_UPLOAD_URL_EXPIRY_SECONDS
            input_upload = await self.storage_service.generate_upload_url(
                file_path=input_file_path,
                content_type=input_content_type,
                expiration=upload_expiry,
            )
            questions_upload = await self.storage_service.generate_upload_url(
                file_path=questions_file_path,
                content_type=questions_content_type,
                expiration=upload_expiry,
            )

            return ExecutionUploadInitResponse(
                execution_id=execution.id,
                status=execution.status,
                input_upload=SignedUrlPayload(**input_upload),
                questions_upload=SignedUrlPayload(**questions_upload),
            )

        except HTTPException:
            raise
        except Exception as exc:
            self._mark_execution_failed(execution, f"Upload initialization failed: {exc}")
            raise HTTPException(
                status_code=status.HTTP_500_INTERNAL_SERVER_ERROR,
                detail=f"Failed to initialize upload: {exc}",
            )

    async def complete_execution_upload(self, execution_id: UUID, user_id: str) -> ExecutionResponse:
        """Validate uploaded files and queue the execution (creation-only phase)."""
        execution = self.db.query(Execution).filter(
            Execution.id == execution_id,
            Execution.created_by == user_id,
        ).first()

        if not execution:
            raise HTTPException(
                status_code=status.HTTP_404_NOT_FOUND,
                detail="Execution not found",
            )

        if execution.status in {"in_progress", "running"}:
            raise HTTPException(
                status_code=status.HTTP_400_BAD_REQUEST,
                detail="Execution is already in progress",
            )

        if execution.upload_completed_at is not None:
            return self._to_execution_response(execution)

        if not execution.input_file_url or not execution.criterias_file_url:
            raise HTTPException(
                status_code=status.HTTP_400_BAD_REQUEST,
                detail="Execution file paths are missing",
            )

        input_metadata = await self.storage_service.get_file_metadata(execution.input_file_url)
        if not input_metadata:
            raise HTTPException(
                status_code=status.HTTP_400_BAD_REQUEST,
                detail="Input file upload not found",
            )
        self._validate_uploaded_metadata(input_metadata, "Input file")

        questions_metadata = await self.storage_service.get_file_metadata(execution.criterias_file_url)
        if not questions_metadata:
            raise HTTPException(
                status_code=status.HTTP_400_BAD_REQUEST,
                detail="Questions file upload not found",
            )
        self._validate_uploaded_metadata(questions_metadata, "Questions file")

        source_type = self._get_csv_source_type(
            tenant_code=execution.tenant_code,
            organization_code=execution.organization_code,
            type_key=execution.csv_type_id or "",
        )
        if not source_type:
            failure_reason = "CSV source type configuration not found or inactive."
            self._mark_execution_failed(execution, failure_reason)
            raise HTTPException(
                status_code=status.HTTP_400_BAD_REQUEST,
                detail=failure_reason,
            )

        try:
            input_file_bytes = await self.storage_service.download_file(execution.input_file_url)
            if not input_file_bytes:
                raise HTTPException(
                    status_code=status.HTTP_400_BAD_REQUEST,
                    detail="Input file could not be read for validation.",
                )
            self._validate_input_csv_against_source(input_file_bytes, source_type)

            questions_file_bytes = await self.storage_service.download_file(execution.criterias_file_url)
            if not questions_file_bytes:
                raise HTTPException(
                    status_code=status.HTTP_400_BAD_REQUEST,
                    detail="Questions file could not be read for validation.",
                )
            self._validate_questions_csv_against_source(questions_file_bytes, source_type)
        except HTTPException as exc:
            self._mark_execution_failed(execution, str(exc.detail))
            raise
        except Exception as exc:
            failure_reason = f"CSV validation failed: {exc}"
            self._mark_execution_failed(execution, failure_reason)
            raise HTTPException(
                status_code=status.HTTP_400_BAD_REQUEST,
                detail=failure_reason,
            ) from exc

        await self._apply_execution_estimates(execution)
        execution.input_file_size = int(input_metadata.get("size_bytes", execution.input_file_size or 0))
        execution.criterias_file_size = int(questions_metadata.get("size_bytes", execution.criterias_file_size or 0))
        execution.status = "queued"
        execution.upload_completed_at = datetime.utcnow()
        execution.failure_reason = None
        execution.notification_sent = False
        execution.notification_sent_at = None

        self.db.commit()
        self.db.refresh(execution)

        try:
            self.worker.submit_job(execution.id)
        except Exception as exc:
            logger.exception("Failed to enqueue execution %s", execution.id)
            self._mark_execution_failed(execution, f"Failed to enqueue execution: {exc}")
            raise HTTPException(
                status_code=status.HTTP_503_SERVICE_UNAVAILABLE,
                detail="Execution could not be queued. Please try again.",
            ) from exc

        return self._to_execution_response(execution)

    async def get_execution_file_preview(
        self,
        execution_id: UUID,
        file_type: str,
        user_id: str,
        limit: int = 10,
    ) -> ExecutionFilePreviewResponse:
        """Return a read-only CSV preview for an uploaded execution file."""
        normalized_file_type = self._normalize_file_type(file_type)

        if limit < 1 or limit > 50:
            raise HTTPException(
                status_code=status.HTTP_400_BAD_REQUEST,
                detail="limit must be between 1 and 50",
            )

        execution = self._get_execution_or_404(execution_id, user_id)
        file_path = self._get_file_path_by_type(execution, normalized_file_type)
        if not file_path:
            raise HTTPException(
                status_code=status.HTTP_400_BAD_REQUEST,
                detail=f"{normalized_file_type} file is not uploaded yet.",
            )

        file_bytes = await self.storage_service.download_file(file_path)
        if not file_bytes:
            raise HTTPException(
                status_code=status.HTTP_404_NOT_FOUND,
                detail=f"{normalized_file_type} file was not found in storage.",
            )

        label = "Input file" if normalized_file_type == "input" else "Questions file"
        headers, row_count, preview_rows, _ = self._parse_csv_preview(
            file_bytes,
            label,
            preview_limit=limit,
        )

        return ExecutionFilePreviewResponse(
            execution_id=execution.id,
            file_type=normalized_file_type,
            rows_detected=row_count,
            columns_detected=headers,
            preview_rows=preview_rows,
        )

    def get_execution(self, execution_id: UUID, user_id: str) -> Optional[ExecutionDetail]:
        """Get execution by ID"""
        execution = self.db.query(Execution).filter(
            Execution.id == execution_id,
            Execution.created_by == user_id
        ).first()

        if not execution:
            return None

        checkpoint = self._checkpoint(execution)

        return ExecutionDetail.model_validate(
            {
                **self._to_execution_response(execution).model_dump(),
                "tenant_code": execution.tenant_code,
                "organization_code": execution.organization_code,
                "ai_model_id": execution.ai_model_id,
                "program_ref_id": execution.program_ref_id,
                "program_name": execution.program_name,
                "criterias_mode": execution.criterias_mode,
                "criterias_config": execution.criterias_config,
                "threshold_config": execution.threshold_config,
                "processing_config": execution.processing_config,
                "actual_cost": self._to_float(execution.actual_cost),
                "estimated_cost": self._to_float(execution.estimated_cost),
                "input_file_size": execution.input_file_size,
                "criterias_file_size": execution.criterias_file_size,
                "output_file_size": execution.output_file_size,
                "upload_completed_at": execution.upload_completed_at,
                "checkpoint_data": checkpoint,
                "input_file_status": self._checkpoint_file_status(checkpoint, "input"),
                "questions_file_status": self._checkpoint_file_status(checkpoint, "questions"),
                "worker_id": execution.worker_id,
                "error_logs": execution.error_logs,
                "retry_count": execution.retry_count,
                "processing_started_at": execution.processing_started_at,
                "processing_completed_at": execution.processing_completed_at,
                "notification_sent_at": execution.notification_sent_at,
            }
        )

    def send_execution_notification(
        self,
        execution_id: UUID,
        user_id: str,
        force_resend: bool = False,
    ) -> ExecutionNotificationResponse:
        """Manually trigger status email notification for one execution."""
        if not settings.IS_NOTIFICATION_ENABLED:
            raise HTTPException(
                status_code=status.HTTP_503_SERVICE_UNAVAILABLE,
                detail="Email notifications are disabled. Please enable IS_NOTIFICATION_ENABLED in configuration.",
            )
        
        execution = self._get_execution_or_404(execution_id, user_id)
        normalized_status = (execution.status or "").strip().lower()
        if normalized_status not in {"completed", "failed"}:
            raise HTTPException(
                status_code=status.HTTP_409_CONFLICT,
                detail="Notification can only be sent when execution status is completed or failed.",
            )

        recipient_email = EmailService._resolve_recipient_email(self.db, execution)
        if not recipient_email:
            raise HTTPException(
                status_code=status.HTTP_400_BAD_REQUEST,
                detail="No recipient email found for this execution.",
            )

        if not EmailService._is_smtp_configured():
            raise HTTPException(
                status_code=status.HTTP_503_SERVICE_UNAVAILABLE,
                detail="SMTP is not configured. Please configure SMTP settings and retry.",
            )

        if force_resend:
            execution.notification_sent = False
            execution.notification_sent_at = None
            self.db.commit()
            self.db.refresh(execution)

        sent = EmailService.notify_execution_status(self.db, execution)
        if not sent:
            raise HTTPException(
                status_code=status.HTTP_502_BAD_GATEWAY,
                detail="Email notification failed after retries.",
            )

        self.db.refresh(execution)
        return ExecutionNotificationResponse(
            execution_id=execution.id,
            execution_status=execution.status or normalized_status,
            recipient_email=recipient_email,
            sent=True,
            notification_sent=bool(execution.notification_sent),
            notification_sent_at=execution.notification_sent_at,
            message="Email notification sent successfully.",
        )

    async def list_executions(
        self,
        user_id: str,
        page: int = 1,
        page_size: int = 20,
        status_filter: Optional[str] = None,
        status_group: Optional[str] = None,
        states_filter: Optional[list] = None,
        search_query: Optional[str] = None,
    ) -> ExecutionList:
        """List executions with pagination and optional server-side filters."""
        if page < 1 or page_size < 1:
            raise HTTPException(
                status_code=status.HTTP_400_BAD_REQUEST,
                detail="page and page_size must be greater than 0",
            )

        if page_size > 500:
            raise HTTPException(
                status_code=status.HTTP_400_BAD_REQUEST,
                detail="page_size cannot exceed 500",
            )

        try:
            query = self.db.query(Execution).filter(Execution.created_by == user_id)

            if status_filter:
                query = query.filter(Execution.status == status_filter)

            normalized_status_group = (status_group or "").strip().lower()
            if normalized_status_group:
                draft_group_statuses = {"draft", "validated", "notstarted"}
                completed_or_failed = {"completed", "failed"}

                if normalized_status_group == "draft":
                    query = query.filter(func.lower(Execution.status).in_(draft_group_statuses))
                elif normalized_status_group in completed_or_failed:
                    query = query.filter(func.lower(Execution.status) == normalized_status_group)
                elif normalized_status_group == "in_progress":
                    query = query.filter(
                        ~func.lower(Execution.status).in_(draft_group_statuses | completed_or_failed)
                    )
                else:
                    raise HTTPException(
                        status_code=status.HTTP_400_BAD_REQUEST,
                        detail="status_group must be one of: draft, in_progress, completed, failed",
                    )

            if states_filter:
                query = query.filter(
                    or_(*[Execution.states.contains([s]) for s in states_filter])
                )

            normalized_search_query = (search_query or "").strip()
            if normalized_search_query:
                query = query.filter(Execution.name.ilike(f"%{normalized_search_query}%"))

            # Single DB round-trip: window function returns count alongside paginated rows
            count_col = func.count().over().label("total_count")
            paginated = (
                query
                .add_columns(count_col)
                .order_by(Execution.created_at.desc())
                .offset((page - 1) * page_size)
                .limit(page_size)
                .all()
            )
            total = paginated[0].total_count if paginated else 0
            executions = [row[0] for row in paginated]
        except SQLAlchemyError as exc:
            logger.exception("Failed to query executions")
            raise HTTPException(
                status_code=status.HTTP_500_INTERNAL_SERVER_ERROR,
                detail="Failed to fetch executions. Please verify database schema/configuration.",
            ) from exc

        # Fetch checkpoint files for all in-progress executions concurrently (non-blocking)
        in_progress_execs = [e for e in executions if (e.status or "").strip().lower() == "in_progress"]
        checkpoint_map: dict[str, dict] = {}
        if in_progress_execs:
            checkpoint_results = await asyncio.gather(
                *[ExecutionService._read_processor_checkpoint_file_async(e.id) for e in in_progress_execs],
                return_exceptions=True,
            )
            for exec_obj, result in zip(in_progress_execs, checkpoint_results):
                if not isinstance(result, Exception):
                    checkpoint_map[str(exec_obj.id)] = result

        items: list[ExecutionResponse] = []
        for execution in executions:
            try:
                items.append(self._to_execution_response(
                    execution,
                    checkpoint_override=checkpoint_map.get(str(execution.id)),
                ))
            except ValidationError:
                logger.exception("Skipping malformed execution row: %s", getattr(execution, "id", "unknown"))

        return ExecutionList(
            total=total,
            page=page,
            page_size=page_size,
            items=items,
        )

    def get_execution_status(self, execution_id: UUID, user_id: str) -> Optional[StatusResponse]:
        """Get real-time status of an execution"""
        execution = self.db.query(Execution).filter(
            Execution.id == execution_id,
            Execution.created_by == user_id
        ).first()

        if not execution:
            return None

        # Get real-time progress for in-progress executions
        processed_rows = execution.processed_rows or 0
        if (execution.status or "").strip().lower() == "in_progress":
            checkpoint_progress = self._read_processor_checkpoint_file(execution.id)
            checkpoint_processed = checkpoint_progress.get('total_processed', 0)
            if checkpoint_processed > processed_rows:
                processed_rows = checkpoint_processed

        progress_percentage = None
        if execution.total_rows and execution.total_rows > 0:
            progress_percentage = (processed_rows / execution.total_rows) * 100

        return StatusResponse(
            id=execution.id,
            status=execution.status,
            processed_rows=processed_rows,
            total_rows=execution.total_rows,
            progress_percentage=progress_percentage,
            failure_reason=execution.failure_reason
        )

    def update_execution(
        self,
        execution_id: UUID,
        update_data: ExecutionUpdate,
        user_id: str
    ) -> ExecutionResponse:
        """Update an execution (draft and validated executions can be updated)."""
        execution = self.db.query(Execution).filter(
            Execution.id == execution_id,
            Execution.created_by == user_id
        ).first()

        if not execution:
            raise HTTPException(
                status_code=status.HTTP_404_NOT_FOUND,
                detail="Execution not found"
            )

        if execution.status not in {'draft', 'validated'}:
            raise HTTPException(
                status_code=status.HTTP_400_BAD_REQUEST,
                detail="Only draft or validated executions can be updated"
            )

        # Update only fields that are explicitly provided by the client.
        payload = update_data.model_dump(exclude_unset=True)
        provided_fields = set(update_data.model_fields_set)

        allowed_fields = {
            'name',
            'states',
            'program_ref_id',
            'program_name',
            PROCESSING_CONFIG_KEY_EVIDENCE_TYPES,
        }

        for field in allowed_fields:
            if field not in provided_fields:
                continue

            value = payload.get(field)
            if field == 'name':
                if value is None or not str(value).strip():
                    raise HTTPException(
                        status_code=status.HTTP_400_BAD_REQUEST,
                        detail="Execution name cannot be empty."
                    )
                execution.name = str(value).strip()
                continue

            if field == 'states' and value is not None:
                if not isinstance(value, list) or not value:
                    raise HTTPException(
                        status_code=status.HTTP_400_BAD_REQUEST,
                        detail="states must be a non-empty array if provided."
                    )

            if field == PROCESSING_CONFIG_KEY_EVIDENCE_TYPES:
                # evidence_types doesn't live on the ORM model directly — it rides inside the
                # generic processing_config JSONB blob, merged so other future keys survive.
                current_processing_config = (
                    dict(execution.processing_config) if isinstance(execution.processing_config, dict) else {}
                )
                if value:
                    current_processing_config[PROCESSING_CONFIG_KEY_EVIDENCE_TYPES] = value
                else:
                    current_processing_config.pop(PROCESSING_CONFIG_KEY_EVIDENCE_TYPES, None)
                execution.processing_config = current_processing_config or None
                continue

            # Optional fields support explicit clears via null.
            setattr(execution, field, value)
        
        execution.updated_by = user_id
        
        try:
            self.db.commit()
            self.db.refresh(execution)
        except SQLAlchemyError as exc:
            self.db.rollback()
            logger.exception("Failed to update execution")
            raise HTTPException(
                status_code=status.HTTP_500_INTERNAL_SERVER_ERROR,
                detail="Failed to update execution"
            ) from exc

        return self._to_execution_response(execution)

    def delete_execution(self, execution_id: UUID, user_id: str) -> bool:
        """Delete an execution (only if not in progress)."""
        execution = self.db.query(Execution).filter(
            Execution.id == execution_id,
            Execution.created_by == user_id
        ).first()

        if not execution:
            return False

        if execution.status in {'in_progress', 'running'}:
            raise HTTPException(
                status_code=status.HTTP_400_BAD_REQUEST,
                detail="Cannot delete execution in progress"
            )

        self.db.delete(execution)
        self.db.commit()

        return True
