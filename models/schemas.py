"""
Pydantic Schemas for Request/Response Validation
"""
from pydantic import BaseModel, EmailStr, Field, field_validator
from typing import List, Optional, Dict, Any, Literal
from datetime import datetime
from uuid import UUID

from core.config import settings
from core.constants import (
    ALLOWED_EVIDENCE_TYPES,
    RELEVANCE_TAG_RELEVANT,
    RELEVANCE_TAG_PARTIAL,
    RELEVANCE_TAG_IRRELEVANT,
)


# ============ Authentication Schemas ============

class UserLogin(BaseModel):
    """Login request schema"""
    username: str
    password: str


class Token(BaseModel):
    """JWT token response schema"""
    access_token: str
    token_type: str = "bearer"
    expires_in: int
    expires_at: datetime


class TokenData(BaseModel):
    """Token payload data"""
    username: Optional[str] = None


class UserResponse(BaseModel):
    """User response schema"""
    id: str
    username: str
    email: str
    full_name: Optional[str] = None
    is_active: bool
    tenant_code: Optional[str] = None
    organization_code: Optional[str] = None
    
    class Config:
        from_attributes = True


class MessageResponse(BaseModel):
    """Simple message response schema."""
    message: str


class RootResponse(BaseModel):
    """Root endpoint response schema."""
    message: str
    version: str
    status: str


class HealthResponse(BaseModel):
    """Health endpoint response schema."""
    status: str


class HTTPErrorResponse(BaseModel):
    """Standard HTTP error response body used by FastAPI HTTPException."""
    detail: Any


# ============ Execution Schemas ============

class ExecutionCreate(BaseModel):
    """Schema for creating a new execution"""
    name: str = Field(..., min_length=1, max_length=255)
    csv_type_id: str = Field(..., min_length=1, max_length=100)
    ai_model_id: Optional[str] = None
    program_ref_id: Optional[str] = None
    program_name: Optional[str] = None
    states: Optional[List[str]] = None
    evidence_types: Optional[List[str]] = None
    # Per-(user, task) relevant-evidence cap. When set, the AI stops validating a
    # user's task once this many evidences are tagged "Relevant"; the rest are written
    # as "notValidated" with no API call. Omitted/None = feature off (current behavior).
    evidence_threshold: Optional[int] = Field(None, ge=1, le=100)

    @field_validator("states", mode="before")
    @classmethod
    def validate_states(cls, v: Any) -> Optional[List[str]]:
        if v is None:
            return None
        if not isinstance(v, list):
            raise ValueError("states must be an array of strings")
        cleaned = [str(s).strip() for s in v if str(s).strip()]
        if len(v) > 0 and not cleaned:
            raise ValueError("states must contain at least one non-empty string")
        return cleaned or None

    @field_validator("evidence_types", mode="before")
    @classmethod
    def validate_evidence_types(cls, v: Any) -> Optional[List[str]]:
        if v is None:
            return None
        if not isinstance(v, list):
            raise ValueError("evidence_types must be an array of strings")
        cleaned = sorted({str(t).strip().lower() for t in v if str(t).strip()})
        if not cleaned:
            return None
        invalid = [t for t in cleaned if t not in ALLOWED_EVIDENCE_TYPES]
        if invalid:
            raise ValueError(f"evidence_types must be a subset of {sorted(ALLOWED_EVIDENCE_TYPES)}, got invalid: {invalid}")
        return cleaned


class FileUploadDescriptor(BaseModel):
    """Client-declared file metadata used before signed URL generation."""
    file_name: str = Field(..., min_length=1, max_length=255)
    content_type: Optional[str] = None
    size_bytes: int = Field(..., gt=0)


class ExecutionUploadInitRequest(ExecutionCreate):
    """Schema for creating execution and requesting signed upload URLs."""
    input_file: FileUploadDescriptor
    questions_file: FileUploadDescriptor


class SignedUrlPayload(BaseModel):
    """Signed URL payload for direct upload/download."""
    url: str
    method: str
    headers: Dict[str, str] = Field(default_factory=dict)
    expires_in_seconds: int


class ExecutionUploadInitResponse(BaseModel):
    """Execution creation response with signed upload URLs."""
    execution_id: UUID
    status: str
    input_upload: SignedUrlPayload
    questions_upload: SignedUrlPayload


class ExecutionCreateRequest(ExecutionCreate):
    """Step 1 request schema to create analysis execution draft."""
    pass


class FileUploadUrlRequest(BaseModel):
    """Request signed upload URL for a file section."""
    file: FileUploadDescriptor


class ExecutionFileUploadUrlResponse(BaseModel):
    """Signed upload URL response for input/questions file."""
    execution_id: UUID
    file_type: str
    upload: SignedUrlPayload


class ExecutionFileCompleteResponse(BaseModel):
    """File upload completion response with detected CSV metadata."""
    execution_id: UUID
    file_type: str
    uploaded: bool
    rows_detected: int
    columns_detected: list[str]


class FileValidationResult(BaseModel):
    """Validation result for one file section."""
    file_type: str
    valid: bool
    message: Optional[str] = None
    missing_columns: list[str] = Field(default_factory=list)
    rows_detected: Optional[int] = None
    columns_detected: Optional[list[str]] = None
    preview_rows: Optional[list[Dict[str, str]]] = None


class ExecutionValidationResponse(BaseModel):
    """Combined validation result for both files."""
    execution_id: UUID
    is_valid: bool
    input_file: FileValidationResult
    questions_file: FileValidationResult


class ExecutionFileCheckpointState(BaseModel):
    """Checkpoint state summary for one uploaded file."""
    uploaded: bool = False
    validated: bool = False
    rows_detected: Optional[int] = None
    columns_detected: list[str] = Field(default_factory=list)
    message: Optional[str] = None
    missing_columns: list[str] = Field(default_factory=list)
    updated_at: Optional[str] = None


class ExecutionFilePreviewResponse(BaseModel):
    """Read-only CSV preview for one uploaded file."""
    execution_id: UUID
    file_type: str
    rows_detected: int
    columns_detected: list[str]
    preview_rows: list[Dict[str, str]]


class ExecutionUpdate(BaseModel):
    """Schema for updating an execution"""
    name: Optional[str] = Field(None, min_length=1, max_length=255)
    states: Optional[List[str]] = None
    program_ref_id: Optional[str] = None
    program_name: Optional[str] = None
    status: Optional[str] = None
    failure_reason: Optional[str] = None
    processed_rows: Optional[int] = None
    total_rows: Optional[int] = None
    evidence_types: Optional[List[str]] = None

    @field_validator("states", mode="before")
    @classmethod
    def validate_states(cls, v: Any) -> Optional[List[str]]:
        if v is None:
            return None
        if not isinstance(v, list):
            raise ValueError("states must be an array of strings")
        cleaned = [str(s).strip() for s in v if str(s).strip()]
        if len(v) > 0 and not cleaned:
            raise ValueError("states must contain at least one non-empty string")
        return cleaned or None

    @field_validator("evidence_types", mode="before")
    @classmethod
    def validate_evidence_types(cls, v: Any) -> Optional[List[str]]:
        if v is None:
            return None
        if not isinstance(v, list):
            raise ValueError("evidence_types must be an array of strings")
        cleaned = sorted({str(t).strip().lower() for t in v if str(t).strip()})
        if not cleaned:
            return None
        invalid = [t for t in cleaned if t not in ALLOWED_EVIDENCE_TYPES]
        if invalid:
            raise ValueError(f"evidence_types must be a subset of {sorted(ALLOWED_EVIDENCE_TYPES)}, got invalid: {invalid}")
        return cleaned


class ExecutionResponse(BaseModel):
    """Schema for execution response"""
    id: UUID
    name: str
    csv_type_id: Optional[str] = None
    status: str
    states: Optional[List[str]] = None
    created_by: Optional[str] = None
    created_at: datetime
    updated_at: Optional[datetime] = None
    completed_at: Optional[datetime] = None
    total_rows: Optional[int] = None
    processed_rows: Optional[int] = None
    input_file_url: Optional[str] = None
    criterias_file_url: Optional[str] = None
    output_file_url: Optional[str] = None
    estimated_cost: Optional[float] = None
    actual_cost: Optional[float] = None
    estimated_time_seconds: Optional[int] = None
    failure_reason: Optional[str] = None
    average_processing_time: Optional[float] = None
    notification_sent: bool = False

    class Config:
        from_attributes = True


class ExecutionDetail(ExecutionResponse):
    """Detailed execution response with all fields"""
    tenant_code: Optional[str] = None
    organization_code: Optional[str] = None
    csv_type_id: Optional[str] = None
    ai_model_id: Optional[str] = None
    program_ref_id: Optional[str] = None
    program_name: Optional[str] = None
    criterias_mode: Optional[str] = None
    criterias_config: Optional[Dict[str, Any]] = None
    threshold_config: Optional[Dict[str, Any]] = None
    processing_config: Optional[Dict[str, Any]] = None
    input_file_size: Optional[int] = None
    criterias_file_size: Optional[int] = None
    output_file_size: Optional[int] = None
    school_filter_file_url: Optional[str] = None
    school_filter_file_size: Optional[int] = None
    upload_completed_at: Optional[datetime] = None
    checkpoint_data: Optional[Dict[str, Any]] = None
    input_file_status: Optional[ExecutionFileCheckpointState] = None
    questions_file_status: Optional[ExecutionFileCheckpointState] = None
    worker_id: Optional[str] = None
    error_logs: Optional[str] = None
    retry_count: Optional[int] = None
    processing_started_at: Optional[datetime] = None
    processing_completed_at: Optional[datetime] = None
    notification_sent_at: Optional[datetime] = None
    
    class Config:
        from_attributes = True


class ExecutionList(BaseModel):
    """Paginated execution list response"""
    total: int
    page: int
    page_size: int
    items: list[ExecutionResponse]


# ============ Criteria Validation Schemas ============

class CriteriaValidationRequest(BaseModel):
    """Request schema for one-off evidence criteria validation."""
    evidence_url: str = Field(..., min_length=1, max_length=2048)
    evidence_criteria: list[str] = Field(..., min_length=1)
    prompt: Optional[str] = Field(default=None, max_length=6000)

    @staticmethod
    def _max_items() -> int:
        raw = getattr(settings, "CRITERIA_VALIDATE_MAX_ITEMS", 25)
        return raw if isinstance(raw, int) and raw > 0 else 25

    @field_validator("evidence_url", mode="before")
    @classmethod
    def strip_evidence_url(cls, value: Any) -> str:
        text = str(value or "").strip()
        if not text:
            raise ValueError("evidence_url is required")
        return text

    @field_validator("evidence_criteria", mode="before")
    @classmethod
    def normalize_evidence_criteria(cls, value: Any) -> list[str]:
        if not isinstance(value, list):
            raise ValueError("evidence_criteria must be an array of strings")

        normalized: list[str] = []
        for item in value:
            text = str(item or "").strip()
            if text:
                normalized.append(text)
        if not normalized:
            raise ValueError("At least one non-empty evidence criteria is required")
        max_items = CriteriaValidationRequest._max_items()
        if len(normalized) > max_items:
            raise ValueError(f"Maximum {max_items} evidence criteria are allowed")
        return normalized

    @field_validator("prompt", mode="before")
    @classmethod
    def normalize_prompt(cls, value: Any) -> Optional[str]:
        if value is None:
            return None
        text = str(value).strip()
        return text or None


class CriteriaValidationItem(BaseModel):
    """Single evidence criteria validation output item."""
    evidence_criteria: str
    answer: str
    reasoning: str


class CriteriaValidationResponse(BaseModel):
    """Response schema for criteria validation."""
    source: Literal["gemini", "openrouter"]
    model: str
    relevance_tag: Literal[RELEVANCE_TAG_RELEVANT, RELEVANCE_TAG_PARTIAL, RELEVANCE_TAG_IRRELEVANT]
    criteria_results: list[CriteriaValidationItem]
    answers: list[str]
    reasonings: list[str]


# ============ Report Schemas ============

class ReportResponse(BaseModel):
    """Report response schema"""
    execution_id: UUID
    input_data: Optional[Dict[str, Any]] = None
    output_data: Optional[Dict[str, Any]] = None
    metadata: Optional[Dict[str, Any]] = None


class ReportDownloadResponse(BaseModel):
    """Signed URL response for report download."""
    download_url: str
    expires_in_seconds: int


class ReportDataPageResponse(BaseModel):
    """Paginated report data response — avoids shipping the full CSV to the browser."""
    page: int
    page_size: int
    total_filtered: int
    total_all: int
    rows: list[Dict[str, str]]
    headers: list[str]
    summary: Optional[Dict[str, Any]] = None  # populated on every request


# ============ Status Schemas ============

class StatusResponse(BaseModel):
    """Execution status response"""
    id: UUID
    status: str
    processed_rows: Optional[int] = None
    total_rows: Optional[int] = None
    progress_percentage: Optional[float] = None
    failure_reason: Optional[str] = None


# ============ Notification Schemas ============

class ExecutionNotificationResponse(BaseModel):
    """Manual execution email notification trigger response."""
    execution_id: UUID
    execution_status: str
    recipient_email: str
    sent: bool
    notification_sent: bool
    notification_sent_at: Optional[datetime] = None
    message: str


# ============ Cloud Services Schemas ============

class CloudSignedUrlRequestItem(BaseModel):
    """Grouped file names for a ref id (e.g., execution id)."""
    files: list[str] = Field(..., min_length=1)


class CloudSignedUrlRequest(BaseModel):
    """Common signed URL request format."""
    request: Dict[str, CloudSignedUrlRequestItem]
    ref: str = Field(..., min_length=1, max_length=100)


class CloudSignedUrlFilePayload(BaseModel):
    """Payload metadata returned with each file URL."""
    sourcePath: str
    fileType: Optional[str] = None


class CloudSignedUrlFileResponse(BaseModel):
    """Single signed URL response file entry."""
    file: str
    url: str
    downloadableUrl: str
    payload: CloudSignedUrlFilePayload
    cloudStorage: str


class CloudSignedUrlResultEntry(BaseModel):
    """Per-reference grouped file URL entries."""
    files: list[CloudSignedUrlFileResponse]


class CloudSignedUrlResponse(BaseModel):
    """Common signed URL response format."""
    responseCode: str
    message: str
    result: Dict[str, CloudSignedUrlResultEntry]
    meta: Dict[str, Any] = Field(default_factory=dict)


class CloudDownloadableUrlRequest(BaseModel):
    """Request downloadable URLs for already-uploaded cloud files."""
    filePaths: list[str] = Field(..., min_length=1)

    model_config = {
        "json_schema_extra": {
            "example": {
                "filePaths": [
                    "tenant_code/org_code/userId/executions/file1.csv",
                    "tenant_code/org_code/userId/executions/file2.csv",
                ]
            }
        }
    }

    @field_validator("filePaths")
    @classmethod
    def validate_file_paths(cls, value: list[str]) -> list[str]:
        normalized_paths: list[str] = []
        for index, raw_path in enumerate(value):
            cleaned = (raw_path or "").strip()
            if not cleaned:
                raise ValueError(f"filePaths[{index}] cannot be empty.")
            normalized_paths.append(cleaned.lstrip("/"))
        return normalized_paths


class CloudDownloadableUrlResultItem(BaseModel):
    """Downloadable URL payload for one file path."""
    cloudStorage: str
    filePath: str
    url: str


class CloudDownloadableUrlResponse(BaseModel):
    """Downloadable URL response format for cloud-services API."""
    responseCode: str
    message: str
    result: list[CloudDownloadableUrlResultItem]
    meta: Dict[str, Any] = Field(default_factory=dict)

    model_config = {
        "json_schema_extra": {
            "example": {
                "responseCode": "OK",
                "message": "Download Url Generated Successfully.",
                "result": [
                    {
                        "cloudStorage": "AWS",
                        "filePath": "tenant_code/org_code/userId/executions/file1.csv",
                        "url": "https://<cloud-storage-url>/tenant_code/org_code/userId/executions/file1.csv?...",
                    }
                ],
                "meta": {},
            }
        }
    }
