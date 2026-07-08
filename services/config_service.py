"""
Config Service
Provides configuration list APIs.
"""
from typing import Any

from fastapi import HTTPException, status
from sqlalchemy.orm import Session

from core.config import settings
from models.csv_source_type import CsvSourceType
from models.schemas import ReportDownloadResponse, UserResponse
from services.storage_service import StorageService


class ConfigService:
    """Service for config endpoints."""

    def __init__(self, db: Session):
        self.db = db
        self.storage_service = StorageService()

    @staticmethod
    def _resolve_scope(current_user: UserResponse) -> tuple[str, str]:
        tenant_code = (current_user.tenant_code or settings.DEFAULT_TENANT_CODE or "").strip()
        organization_code = (current_user.organization_code or settings.DEFAULT_ORGANIZATION_CODE or "").strip()

        if not tenant_code or not organization_code:
            raise HTTPException(
                status_code=status.HTTP_500_INTERNAL_SERVER_ERROR,
                detail="Tenant/organization configuration is missing",
            )

        return tenant_code, organization_code

    @staticmethod
    def _resolve_criterias_mode(question_config: Any) -> str | None:
        if not isinstance(question_config, dict):
            return None

        for key in ("criterias_mode", "mode"):
            candidate = str(question_config.get(key, "")).strip()
            if candidate:
                return candidate

        entry_options = question_config.get("entry_options")
        if isinstance(entry_options, list):
            for entry in entry_options:
                if not isinstance(entry, dict):
                    continue
                for option_key in ("key", "value"):
                    candidate = str(entry.get(option_key, "")).strip()
                    if candidate:
                        return candidate
        return None

    @staticmethod
    def _serialize_csv_source_type(source_type: CsvSourceType) -> dict[str, Any]:
        question_config = source_type.question_config or {}
        default_thresholds = source_type.default_thresholds or {}
        return {
            "id": source_type.id,
            "type_key": source_type.type_key,
            "display_name": source_type.display_name,
            "description": source_type.description,
            "has_geo": bool(source_type.has_geo),
            "has_program": bool(source_type.has_program),
            "has_rubric": bool(source_type.has_rubric),
            "has_narrative": bool(source_type.has_narrative),
            "max_rows_per_upload": source_type.max_rows_per_upload,
            "available_filters": source_type.available_filters or [],
            "question_config": question_config,
            "default_thresholds": default_thresholds,
            "criterias_mode": ConfigService._resolve_criterias_mode(question_config),
            "threshold_config": default_thresholds,
        }

    def list(self, config_type: str, current_user: UserResponse) -> list[dict[str, Any]]:
        """
        List config entries for the requested type.
        Supported contract:
        - type=project        -> project CSV source types (type_key=project_report).
        - type=evidence_type  -> allowed evidence types for the evidence-type filter.
        - type=school_filter  -> required column name for the school-filter CSV.
        """
        normalized_type = (config_type or "").strip().lower()
        if normalized_type == "school_filter":
            tenant_code, organization_code = self._resolve_scope(current_user)
            source_type = (
                self.db.query(CsvSourceType)
                .filter(
                    CsvSourceType.tenant_code == tenant_code,
                    CsvSourceType.organization_code == organization_code,
                    CsvSourceType.is_active.is_(True),
                    CsvSourceType.type_key == "project_report",
                )
                .first()
            )
            if not source_type:
                raise HTTPException(
                    status_code=status.HTTP_404_NOT_FOUND,
                    detail="No active project_report CSV source type configured for this tenant/organization.",
                )
            school_filter_config = source_type.school_filter_config
            required_column = (
                school_filter_config.get("required_column")
                if isinstance(school_filter_config, dict)
                else None
            )
            if not required_column:
                raise HTTPException(
                    status_code=status.HTTP_500_INTERNAL_SERVER_ERROR,
                    detail="school_filter_config is not configured for this tenant's CSV source type.",
                )
            return [{"required_column": str(required_column).strip()}]

        if normalized_type == "evidence_type":
            tenant_code, organization_code = self._resolve_scope(current_user)
            source_type = (
                self.db.query(CsvSourceType)
                .filter(
                    CsvSourceType.tenant_code == tenant_code,
                    CsvSourceType.organization_code == organization_code,
                    CsvSourceType.is_active.is_(True),
                    CsvSourceType.type_key == "project_report",
                )
                .first()
            )
            if not source_type:
                raise HTTPException(
                    status_code=status.HTTP_404_NOT_FOUND,
                    detail="No active project_report CSV source type configured for this tenant/organization.",
                )
            if not isinstance(source_type.evidence_types_config, list) or not source_type.evidence_types_config:
                raise HTTPException(
                    status_code=status.HTTP_500_INTERNAL_SERVER_ERROR,
                    detail="evidence_types_config is not configured for this tenant's CSV source type.",
                )
            evidence_types_config = source_type.evidence_types_config
            # extensions are an internal detail for the pre-processor/processor scripts —
            # the public config contract only ever exposed {key, label}. evidence_types_config
            # is tenant-controlled JSONB; malformed entries (manual DB edits, bad defaults)
            # are skipped rather than raising a 500 on missing key/label.
            return [
                {"key": item["key"], "label": item["label"]}
                for item in evidence_types_config
                if isinstance(item, dict) and item.get("key") and item.get("label")
            ]

        if normalized_type != "project":
            raise HTTPException(
                status_code=status.HTTP_400_BAD_REQUEST,
                detail="Unsupported config type. Use type=project, type=evidence_type, or type=school_filter.",
            )

        tenant_code, organization_code = self._resolve_scope(current_user)
        source_types = (
            self.db.query(CsvSourceType)
            .filter(
                CsvSourceType.tenant_code == tenant_code,
                CsvSourceType.organization_code == organization_code,
                CsvSourceType.is_active.is_(True),
                CsvSourceType.type_key == "project_report",
            )
            .order_by(CsvSourceType.display_name.asc())
            .all()
        )

        return [self._serialize_csv_source_type(item) for item in source_types]

    async def get_sample_file_url(
        self, type_id: int, file_type: str, current_user: UserResponse
    ) -> ReportDownloadResponse:
        """
        Get signed download URL for sample CSV file.
        
        Args:
            type_id: CSV source type ID
            file_type: Either "input" or "criteria"
            current_user: Current authenticated user
            
        Returns:
            ReportDownloadResponse with signed download URL
            
        Raises:
            HTTPException: If type not found or sample URL not configured
        """
        # Validate file_type parameter
        if file_type not in ("input", "criteria", "school_filter"):
            raise HTTPException(
                status_code=status.HTTP_400_BAD_REQUEST,
                detail="Invalid file_type. Must be 'input', 'criteria', or 'school_filter'.",
            )

        tenant_code, organization_code = self._resolve_scope(current_user)
        
        # Fetch CSV source type
        source_type = (
            self.db.query(CsvSourceType)
            .filter(
                CsvSourceType.id == type_id,
                CsvSourceType.tenant_code == tenant_code,
                CsvSourceType.organization_code == organization_code,
                CsvSourceType.is_active.is_(True),
            )
            .first()
        )
        
        if not source_type:
            raise HTTPException(
                status_code=status.HTTP_404_NOT_FOUND,
                detail=f"CSV source type with id={type_id} not found.",
            )
        
        # Get the appropriate sample URL
        _sample_url_by_type = {
            "input": source_type.sample_input_file_url,
            "criteria": source_type.sample_criteria_file_url,
            "school_filter": source_type.sample_school_filter_file_url,
        }
        sample_url = _sample_url_by_type[file_type]
        
        if not sample_url:
            raise HTTPException(
                status_code=status.HTTP_404_NOT_FOUND,
                detail=f"Sample {file_type} CSV file not available for this source type.",
            )
        
        # Generate signed download URL
        try:
            signed_download = await self.storage_service.generate_download_url(
                file_path=sample_url,
                expiration=settings.SIGNED_DOWNLOAD_URL_EXPIRY_SECONDS,
                response_filename=f"sample_{file_type}.csv",
            )
            return ReportDownloadResponse(
                download_url=signed_download["url"],
                expires_in_seconds=signed_download["expires_in_seconds"],
            )
        except Exception as exc:
            raise HTTPException(
                status_code=status.HTTP_500_INTERNAL_SERVER_ERROR,
                detail=f"Failed to generate download URL: {str(exc)}",
            )
