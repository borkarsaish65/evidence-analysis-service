"""
Execution Model
"""
from sqlalchemy import Column, String, Integer, BigInteger, Boolean, DateTime, Numeric, Text, JSON
from sqlalchemy.dialects.postgresql import UUID, JSONB
from sqlalchemy.sql import func
from db.database import Base
import uuid


class Execution(Base):
    """Execution model for evidence analysis jobs"""
    __tablename__ = "executions"
    
    # Primary identification
    id = Column(UUID(as_uuid=True), primary_key=True, default=uuid.uuid4)
    tenant_code = Column(String(100), nullable=False)
    organization_code = Column(String(100), nullable=False)
    
    # Execution metadata
    name = Column(String(255), nullable=False)
    csv_type_id = Column(String(100), nullable=True)
    ai_model_id = Column(String(100), nullable=True)
    program_ref_id = Column(String(100), nullable=True)
    program_name = Column(String(255), nullable=True)
    states = Column(JSONB, nullable=True)  # list of state name strings; state/district columns kept in DB for history
    
    # Configuration
    criterias_mode = Column(String(50), nullable=True)
    criterias_file_url = Column(Text, nullable=True)
    criterias_config = Column(JSONB, nullable=True)
    threshold_config = Column(JSONB, nullable=True)
    processing_config = Column(JSONB, nullable=True)  # generic per-execution processing options, e.g. {"evidence_types": [...]}
    
    # Status and processing
    status = Column(String(50), nullable=False, default='queued', index=True)  # queued, in_progress, completed, failed
    failure_reason = Column(Text, nullable=True)
    
    # Metrics
    total_rows = Column(Integer, nullable=True)
    processed_rows = Column(Integer, default=0)
    actual_cost = Column(Numeric(10, 4), nullable=True)
    estimated_cost = Column(Numeric(10, 4), nullable=True)
    estimated_time_seconds = Column(Integer, nullable=True)
    
    # File storage fields (store provider-agnostic absolute file paths)
    input_file_url = Column(Text, nullable=True)
    input_file_size = Column(BigInteger, nullable=True)
    criterias_file_size = Column(BigInteger, nullable=True)
    output_file_url = Column(Text, nullable=True)
    output_file_size = Column(BigInteger, nullable=True)
    
    # Processing metadata (Phase 1 additions)
    worker_id = Column(String(100), nullable=True)
    error_logs = Column(Text, nullable=True)
    retry_count = Column(Integer, default=0)
    checkpoint_data = Column(JSONB, nullable=True)
    
    # Timestamps
    created_by = Column(String(100), nullable=True, index=True)
    updated_by = Column(String(100), nullable=True)
    created_at = Column(DateTime(timezone=True), server_default=func.now(), index=True)
    updated_at = Column(DateTime(timezone=True), onupdate=func.now())
    completed_at = Column(DateTime(timezone=True), nullable=True)
    upload_completed_at = Column(DateTime(timezone=True), nullable=True)
    processing_started_at = Column(DateTime(timezone=True), nullable=True)
    processing_completed_at = Column(DateTime(timezone=True), nullable=True)
    
    # New metrics (Phase 1 additions)
    average_processing_time = Column(Numeric(10, 2), nullable=True)
    notification_sent = Column(Boolean, default=False, index=True)
    notification_sent_at = Column(DateTime(timezone=True), nullable=True)
    
    def __repr__(self):
        return f"<Execution(id={self.id}, name={self.name}, status={self.status})>"
