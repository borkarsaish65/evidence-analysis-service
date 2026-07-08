"""
Seed data initialization for Phase 1
Creates default users with properly hashed passwords
"""
import sys
from pathlib import Path
import logging
from sqlalchemy.orm import Session

# Add parent directory to path for direct script execution
sys.path.insert(0, str(Path(__file__).parent.parent))

from core.config import settings
from core.constants import DEFAULT_EVIDENCE_TYPES_CONFIG
from models.csv_source_type import CsvSourceType
from services.auth_service import AuthService
from db.database import SessionLocal

logger = logging.getLogger(__name__)


def seed_default_users(db: Session) -> None:
    """
    Seed default users for Phase 1 development
    This should be called on application startup
    """
    auth_service = AuthService(db)
    
    # Default users configuration (loaded from env in production)
    default_users = [
        {
            "username": "admin",
            "email": "admin@shishalokam.com",
            "password": "admin123",
            "full_name": "System Administrator",
            "is_superuser": True,
            "tenant_code": "default",
            "organization_code": "default_code"
        },
        {
            "username": "program_designer",
            "email": "program_designer@shishalokam.com",
            "password": "user123",
            "full_name": "Program Designer",
            "is_superuser": False,
            "tenant_code": "default",
            "organization_code": "default_code"
        },
        {
            "username": "analyst",
            "email": "analyst@shishalokam.com",
            "password": "user123",
            "full_name": "Analyst",
            "is_superuser": False,
            "tenant_code": "default",
            "organization_code": "default_code"
        }
    ]
    
    for user_data in default_users:
        existing_user = auth_service.get_user_by_username(user_data["username"])
        
        if not existing_user:
            try:
                user = auth_service.create_user(
                    username=user_data["username"],
                    email=user_data["email"],
                    password=user_data["password"],
                    full_name=user_data["full_name"],
                    is_superuser=user_data["is_superuser"],
                    tenant_code=user_data["tenant_code"],
                    organization_code=user_data["organization_code"]
                )
                logger.info(f"Created default user: {user.username}")
            except Exception as e:
                logger.error(f"Failed to create user {user_data['username']}: {str(e)}")
                db.rollback()
        else:
            # Ensure existing seeded users always keep a valid bcrypt hash.
            if not auth_service.is_bcrypt_hash(existing_user.hashed_password):
                try:
                    existing_user.hashed_password = auth_service.get_password_hash(user_data["password"])
                    db.commit()
                    logger.warning(
                        f"Replaced invalid password format with bcrypt hash for user: {user_data['username']}"
                    )
                except Exception as e:
                    logger.error(
                        f"Failed to repair password hash for user {user_data['username']}: {str(e)}"
                    )
                    db.rollback()
            else:
                logger.info(f"User {user_data['username']} already exists with valid bcrypt hash, skipping")
    
    logger.info("Seed data initialization completed")


def seed_default_csv_source_types(db: Session) -> None:
    """Seed default CSV source type configuration for project reports."""
    tenant_code = (settings.DEFAULT_TENANT_CODE or "default").strip() or "default"
    organization_code = (settings.DEFAULT_ORGANIZATION_CODE or "default_code").strip() or "default_code"

    existing = (
        db.query(CsvSourceType)
        .filter(
            CsvSourceType.tenant_code == tenant_code,
            CsvSourceType.organization_code == organization_code,
            CsvSourceType.type_key == "project_report",
        )
        .first()
    )
    if existing:
        logger.info("CSV source type project_report already exists, skipping")
        return

    admin_user = AuthService(db).get_user_by_username("admin")
    admin_user_id = admin_user.id if admin_user else None

    source_type = CsvSourceType(
        tenant_code=tenant_code,
        organization_code=organization_code,
        type_key="project_report",
        display_name="Project Report",
        description="Project report CSV format for evidence validation.",
        has_geo=True,
        has_program=True,
        has_rubric=False,
        has_narrative=False,
        max_rows_per_upload=10000,
        column_mappings={
            "identifier": "UUID",
            "geo": {
                "state": "Declared State",
                "district": "District",
                "block": "Block",
                "school_id": "School ID",
                "school_name": "School Name",
            },
            "user_id": "UUID",
            "program": {"id": "Program ID", "name": "Program Name"},
        },
        evidence_columns=[{"column": "Task Evidence", "rule": "VALIDATE"}],
        evidence_context_config={
            "source": "column",
            "title_column": "Tasks",
        },
        available_filters=["state", "district", "block", "school_name", "relevance_tag"],
        question_config={
            "entry_options": [
                {
                    "key": "UPLOAD",
                    "label": "Upload Checklist CSV",
                    "desc": "Match unique Task IDs to specific evidence_criteria.",
                }
            ],
            "mandatory_columns": ["evidence_context_config.title_column", "Question"],
            "optional_columns": [],
        },
        default_thresholds={"relevant": 0.7, "partial": 0.5},
        evidence_types_config=DEFAULT_EVIDENCE_TYPES_CONFIG,
        is_active=True,
        created_by=admin_user_id,
        updated_by=admin_user_id,
    )

    db.add(source_type)
    db.commit()
    logger.info("Seeded default csv_source_type: project_report")


def run_seed() -> None:
    """Run seed process as a standalone script."""
    db = SessionLocal()
    try:
        seed_default_users(db)
        seed_default_csv_source_types(db)
    finally:
        db.close()


if __name__ == "__main__":
    logging.basicConfig(level=logging.INFO)
    run_seed()
