"""
Storage bootstrap — validates cloud connectivity and uploads sample CSVs.

Called from two places with identical behaviour:
  1. main.py lifespan      → every native / uvicorn / systemd startup
  2. scripts/upload_sample_csvs.py → Docker pre-start step before uvicorn

Both call ``run_bootstrap(db)``, which is safe to invoke on every restart.

Startup sequence enforced by run_bootstrap:
  1. Initialise StorageService (validates provider + credentials at SDK level)
  2. run_deep_validation() — runs on EVERY startup:
       a. write a tiny probe object
       b. read it back via get_file_metadata
       c. generate a signed download URL
       d. delete the probe (best-effort)
     This catches rotated credentials, lost bucket permissions, or provider
     outages at startup rather than silently at runtime.
  3. Upload sample CSVs — runs only if not already uploaded (DB field is NULL):
       a. upload file to cloud bucket
       b. record cloud path in csv_source_types table
       c. verify a signed download URL can be generated
     Once both sample_input_file_url and sample_criteria_file_url are set in
     the database, subsequent startups skip the upload entirely.
"""
from __future__ import annotations

import logging
from pathlib import Path

from sqlalchemy.orm import Session

from core.config import settings
from models.csv_source_type import CsvSourceType
from services.storage_service import StorageError, StorageService

logger = logging.getLogger(__name__)

# ── Sample CSV manifest ───────────────────────────────────────────────────────

_PROJECT_ROOT = Path(__file__).resolve().parent.parent

_SAMPLE_UPLOADS: list[dict] = [
    {
        "local_path": (
            _PROJECT_ROOT / "public" / "sample-csv" / "projects" / "sample_input.csv"
        ),
        "cloud_path": "projects/sample_input.csv",
        "db_field": "sample_input_file_url",
        "label": "sample_input",
    },
    {
        "local_path": (
            _PROJECT_ROOT / "public" / "sample-csv" / "projects" / "sample_criteria.csv"
        ),
        "cloud_path": "projects/sample_criteria.csv",
        "db_field": "sample_criteria_file_url",
        "label": "sample_criteria",
    },
    {
        "local_path": (
            _PROJECT_ROOT / "public" / "sample-csv" / "projects" / "sample_school_filter.csv"
        ),
        "cloud_path": "projects/sample_school_filter.csv",
        "db_field": "sample_school_filter_file_url",
        "label": "sample_school_filter",
    },
]


# ── Internal helpers ──────────────────────────────────────────────────────────

def _resolve_scope() -> tuple[str, str]:
    tenant_code = (settings.DEFAULT_TENANT_CODE or "default").strip() or "default"
    organization_code = (
        (settings.DEFAULT_ORGANIZATION_CODE or "default_code").strip() or "default_code"
    )
    return tenant_code, organization_code


async def _upload_sample_csvs(db: Session, storage: StorageService) -> None:
    """Upload sample CSVs that have not been uploaded yet, sync DB paths, and verify signing.

    Each file is skipped individually if its DB field (sample_input_file_url /
    sample_criteria_file_url) is already populated, meaning it was successfully
    uploaded in a previous startup.
    """
    tenant_code, organization_code = _resolve_scope()

    record: CsvSourceType | None = (
        db.query(CsvSourceType)
        .filter(
            CsvSourceType.tenant_code == tenant_code,
            CsvSourceType.organization_code == organization_code,
            CsvSourceType.type_key == "project_report",
        )
        .first()
    )

    if record is None:
        # Seed hasn't run yet (e.g. bootstrap script invoked before first uvicorn start).
        # The lifespan seeds before calling run_bootstrap, so this only happens from the
        # standalone script on a completely empty database.
        logger.warning(
            "csv_source_type 'project_report' not found — "
            "sample CSV upload skipped.  "
            "Ensure seed data has been applied first (run the application once or "
            "run scripts/upload_sample_csvs.py after applying migrations)."
        )
        return

    provider = storage.storage_provider.upper()
    updated = False

    for spec in _SAMPLE_UPLOADS:
        local_path: Path = spec["local_path"]
        cloud_path: str = spec["cloud_path"]
        db_field: str = spec["db_field"]
        label: str = spec["label"]

        # Skip if this file was already uploaded in a previous startup.
        if getattr(record, db_field, None):
            logger.info("[Bootstrap] %-20s already uploaded — skipping.", label)
            continue

        # Verify the source file exists in the repository
        if not local_path.exists():
            raise FileNotFoundError(
                f"Sample CSV not found at expected repository path: {local_path}\n"
                "Ensure the repository was cloned with the public/sample-csv/ "
                "directory intact."
            )

        logger.info(
            "[Bootstrap] Uploading %-20s  →  %s (provider=%s)",
            local_path.name, cloud_path, provider,
        )
        content = local_path.read_bytes()

        try:
            stored_path = await storage.upload_file(
                file_content=content,
                file_path=cloud_path,
                content_type="text/csv",
            )
        except StorageError as exc:
            raise RuntimeError(
                f"[Bootstrap] Failed to upload {label} to {provider} "
                f"bucket '{storage.bucket_name}'.\n"
                f"  Detail: {exc}"
            ) from exc

        setattr(record, db_field, stored_path)
        updated = True
        logger.info("[Bootstrap]   stored at  %s", stored_path)

        # Post-upload verification: confirm a signed download URL can be generated
        try:
            signed = await storage.generate_download_url(stored_path, expiration=60)
            if not (signed or {}).get("url"):
                raise RuntimeError("generate_download_url returned an empty URL")
            logger.info("[Bootstrap]   signed URL generation: OK")
        except StorageError as exc:
            raise RuntimeError(
                f"[Bootstrap] Post-upload signed URL check failed for {label}.\n"
                f"  File was uploaded but signing is broken — "
                f"the frontend will not be able to download sample files.\n"
                f"  Detail: {exc}"
            ) from exc

    if updated:
        try:
            db.commit()
            logger.info("[Bootstrap] Database synced: sample CSV cloud paths updated.")
        except Exception as exc:
            db.rollback()
            raise RuntimeError(
                f"[Bootstrap] Failed to commit sample CSV paths to database: {exc}"
            ) from exc


# ── Public API ────────────────────────────────────────────────────────────────

async def run_bootstrap(db: Session) -> None:
    """Full startup bootstrap: deep storage validation + sample CSV upload.

    Idempotent — safe to call on every deployment or restart.
    Raises RuntimeError (or FileNotFoundError) on unrecoverable failures so
    the caller can abort startup before any traffic is served.
    """
    logger.info("======== Storage Bootstrap: starting ========")

    # Initialise client — raises ValueError immediately on bad provider/credentials
    try:
        storage = StorageService()
    except Exception as exc:
        raise RuntimeError(
            f"[Bootstrap] StorageService failed to initialise: {exc}\n"
            "  Check CLOUD_STORAGE_PROVIDER, CLOUD_STORAGE_BUCKETNAME, and "
            "CLOUD_STORAGE_SECRET in your environment."
        ) from exc

    logger.info(
        "[Bootstrap] Provider=%s  Bucket=%s",
        storage.storage_provider.upper(), storage.bucket_name,
    )

    # Deep validation: write probe → read → signed URL → delete
    await storage.run_deep_validation()

    # Upload sample CSVs and verify
    await _upload_sample_csvs(db, storage)

    logger.info("======== Storage Bootstrap: complete ========")
