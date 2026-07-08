"""
CSV Source Type Model
Registry for CSV parsing and validation configuration.
"""
from sqlalchemy import Boolean, Column, DateTime, ForeignKey, Integer, String, Text, UniqueConstraint, text
from sqlalchemy.dialects.postgresql import JSONB
from sqlalchemy.sql import func

from db.database import Base


class CsvSourceType(Base):
    """CSV source type registry model."""

    __tablename__ = "csv_source_types"
    __table_args__ = (
        UniqueConstraint(
            "tenant_code",
            "organization_code",
            "type_key",
            name="uq_csv_source_types_scope_type_key",
        ),
    )

    id = Column(Integer, primary_key=True, autoincrement=True)
    tenant_code = Column(String(100), nullable=False)
    organization_code = Column(String(100), nullable=False)

    # Metadata
    type_key = Column(String(100), nullable=False)
    display_name = Column(String(255), nullable=False)
    description = Column(Text, nullable=True)

    # Flags
    has_geo = Column(Boolean, nullable=False, server_default=text("false"))
    has_program = Column(Boolean, nullable=False, server_default=text("false"))
    has_rubric = Column(Boolean, nullable=False, server_default=text("false"))
    has_narrative = Column(Boolean, nullable=False, server_default=text("false"))
    max_rows_per_upload = Column(Integer, nullable=True, server_default=text("10000"))

    # Config
    column_mappings = Column(JSONB, nullable=False)
    evidence_columns = Column(JSONB, nullable=False, server_default=text("'[]'::jsonb"))
    # Evidence-type/extension registry — [{key, label, extensions}], e.g. {"key": "image",
    # "label": "Image", "extensions": [".jpg", ...]}. Source of truth for the evidence-type
    # filter; lets a new type/extension be added per tenant without a code deploy.
    evidence_types_config = Column(
        JSONB,
        nullable=False,
        server_default=text(
            "'[{\"key\": \"image\", \"label\": \"Image\", "
            "\"extensions\": [\".jpg\", \".jpeg\", \".png\", \".gif\", \".webp\", \".bmp\"]}, "
            "{\"key\": \"pdf\", \"label\": \"PDF\", \"extensions\": [\".pdf\"]}, "
            "{\"key\": \"excel\", \"label\": \"Excel\", \"extensions\": [\".xlsx\"]}]'::jsonb"
        ),
    )
    # Required header column in an uploaded school-filter CSV — {"required_column": "..."}.
    # Source of truth for school-filter upload validation and the pre-processor's row
    # filter; lets the column name change per tenant without a code deploy.
    school_filter_config = Column(
        JSONB,
        nullable=False,
        server_default=text('\'{"required_column": "UDISE+ SCHOOL CODE"}\'::jsonb'),
    )
    evidence_context_config = Column(JSONB, nullable=False)
    available_filters = Column(JSONB, nullable=True, server_default=text("'[]'::jsonb"))
    question_config = Column(
        JSONB,
        nullable=True,
        server_default=text(
            "'{\"entry_options\":[{\"key\":\"UPLOAD\",\"label\":\"Upload CSV\"},{\"key\":\"COMMON\",\"label\":\"Manual Entry\"}],"
            "\"mandatory_columns\":[],\"optional_columns\":[]}'::jsonb"
        ),
    )
    default_thresholds = Column(
        JSONB,
        nullable=True,
        server_default=text("jsonb_build_object('relevant', 0.8, 'partial', 0.5)"),
    )

    # Sample file URLs
    sample_input_file_url = Column(Text, nullable=True)
    sample_criteria_file_url = Column(Text, nullable=True)
    sample_school_filter_file_url = Column(Text, nullable=True)

    # Status and audit
    is_active = Column(Boolean, nullable=False, server_default=text("true"))
    created_by = Column(String(255), ForeignKey("users.id"), nullable=True)
    updated_by = Column(String(255), ForeignKey("users.id"), nullable=True)
    created_at = Column(DateTime(timezone=True), nullable=False, server_default=func.now())
    updated_at = Column(DateTime(timezone=True), nullable=False, server_default=func.now(), onupdate=func.now())

    def __repr__(self) -> str:
        return f"<CsvSourceType(id={self.id}, type_key={self.type_key}, active={self.is_active})>"
