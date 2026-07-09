"""
Application Configuration
Loads environment variables and application settings
"""
import json
import os
from pathlib import Path

from dotenv import load_dotenv
from pydantic import computed_field, field_validator
from pydantic_settings import BaseSettings
from typing import Dict, List
from core.constants import PROVIDER_GEMINI
from env_variables import validate_environment
from utils.env_parsing import parse_model_cost_settings

SERVICE_ROOT = Path(__file__).resolve().parent.parent
ENV_FILE_PATH = SERVICE_ROOT / ".env"

# Populates os.environ from .env so the explicit os.getenv(...) field defaults below can see
# it — pydantic-settings' own env_file mechanism (Config.env_file, further down) reads the
# file directly and does NOT touch os.environ, so without this, os.getenv() calls in this
# class body would silently never see .env values, only real shell-exported ones.
load_dotenv(dotenv_path=ENV_FILE_PATH)


class Settings(BaseSettings):
    """Application settings loaded from environment variables"""
    
    # Application
    APP_NAME: str = os.getenv("APP_NAME", "Evidence Analysis System")
    APP_VERSION: str = os.getenv("APP_VERSION", "1.0.0")
    DEBUG: bool = os.getenv("DEBUG", "false").lower() == "true"

    # Base path prefix the app is mounted under (e.g. "/evidence-analysis"
    # when served behind a reverse proxy/gateway). Leave empty to serve at root.
    API_BASE_PATH: str = os.getenv("API_BASE_PATH", "")

    # Database
    DATABASE_URL: str = os.getenv("DATABASE_URL", "")

    # JWT Authentication
    JWT_SECRET_KEY: str = os.getenv("JWT_SECRET_KEY", "")
    JWT_ALGORITHM: str = os.getenv("JWT_ALGORITHM", "HS256")
    JWT_ACCESS_TOKEN_EXPIRE_MINUTES: int = int(os.getenv("JWT_ACCESS_TOKEN_EXPIRE_MINUTES", "1440"))
    
    # CORS
    CORS_ORIGINS: List[str] = ["http://localhost:5173", "http://localhost:3000"]

    # Execution defaults
    DEFAULT_TENANT_CODE: str = os.getenv("DEFAULT_TENANT_CODE", "default")
    DEFAULT_ORGANIZATION_CODE: str = os.getenv("DEFAULT_ORGANIZATION_CODE", "default_code")

    # Entity Management Service
    ENTITY_MGMT_BASE_URL: str = os.getenv("ENTITY_MGMT_BASE_URL", "")
    ENTITY_MGMT_TENANT_ID: str = os.getenv("ENTITY_MGMT_TENANT_ID", "shikshalokam")
    ENTITY_MGMT_ORIGIN: str = os.getenv("ENTITY_MGMT_ORIGIN", "https://dev.elevate-sandbox.shikshalokam.org")
    ENTITY_MGMT_TIMEOUT_SECONDS: float = float(os.getenv("ENTITY_MGMT_TIMEOUT_SECONDS", "10.0"))
    ENTITY_MGMT_RETRY_ATTEMPTS: int = int(os.getenv("ENTITY_MGMT_RETRY_ATTEMPTS", "2"))
    ENTITY_MGMT_RETRY_BACKOFF_SECONDS: float = float(os.getenv("ENTITY_MGMT_RETRY_BACKOFF_SECONDS", "0.5"))
    ENTITY_MGMT_CACHE_ENABLED: bool = os.getenv("ENTITY_MGMT_CACHE_ENABLED", "true").lower() == "true"
    ENTITY_MGMT_STATES_CACHE_TTL_SECONDS: int = int(os.getenv("ENTITY_MGMT_STATES_CACHE_TTL_SECONDS", "900"))

    # File Storage (standardized across AWS/GCP)
    CLOUD_ENDPOINT: str = os.getenv("CLOUD_ENDPOINT", "")
    CLOUD_STORAGE: str = os.getenv("CLOUD_STORAGE", "GCP")
    CLOUD_STORAGE_ACCOUNTNAME: str = os.getenv("CLOUD_STORAGE_ACCOUNTNAME", "")
    CLOUD_STORAGE_BUCKETNAME: str = os.getenv("CLOUD_STORAGE_BUCKETNAME", "")
    CLOUD_STORAGE_PROVIDER: str = os.getenv("CLOUD_STORAGE_PROVIDER", "gcp")  # gcp or aws
    CLOUD_STORAGE_REGION: str = os.getenv("CLOUD_STORAGE_REGION", "")
    CLOUD_STORAGE_SECRET: str = os.getenv("CLOUD_STORAGE_SECRET", "")
    CLOUD_STORAGE_BUCKET_TYPE: str = os.getenv("CLOUD_STORAGE_BUCKET_TYPE", "private")

    # Local Storage
    LOCAL_STORAGE_PATH: str = os.getenv("LOCAL_STORAGE_PATH", "./uploads")

    # LLM Provider Selection
    LLM_PROVIDER: str = os.getenv("LLM_PROVIDER", PROVIDER_GEMINI)

    # AI Models (Gemini)
    GEMINI_API_KEY_1: str = os.getenv("GEMINI_API_KEY_1", "")
    GEMINI_API_KEY_2: str = os.getenv("GEMINI_API_KEY_2", "")
    GEMINI_API_KEY_3: str = os.getenv("GEMINI_API_KEY_3", "")
    GEMINI_MODEL: str = os.getenv("GEMINI_MODEL", "gemini-2.5-flash")

    # OpenRouter
    OPENROUTER_MODEL: str = os.getenv("OPENROUTER_MODEL", "google/gemini-2.5-flash-lite")
    OPENROUTER_BASE_URL: str = os.getenv("OPENROUTER_BASE_URL", "https://openrouter.ai/api/v1")

    # Notification control
    IS_NOTIFICATION_ENABLED: bool = os.getenv("IS_NOTIFICATION_ENABLED", "true").lower() == "true"

    # Email (SMTP)
    SMTP_HOST: str = os.getenv("SMTP_HOST", "")
    SMTP_PORT: int = int(os.getenv("SMTP_PORT", "587"))
    SMTP_API_KEY: str = os.getenv("SMTP_API_KEY", "")
    SMTP_USER: str = os.getenv("SMTP_USER", "")
    SMTP_PASSWORD: str = os.getenv("SMTP_PASSWORD", "")
    SMTP_USE_TLS: bool = os.getenv("SMTP_USE_TLS", "true").lower() == "true"
    SMTP_TIMEOUT_SECONDS: int = int(os.getenv("SMTP_TIMEOUT_SECONDS", "30"))
    SMTP_MAX_RETRIES: int = int(os.getenv("SMTP_MAX_RETRIES", "3"))
    SMTP_RETRY_BACKOFF_SECONDS: int = int(os.getenv("SMTP_RETRY_BACKOFF_SECONDS", "1"))
    SMTP_FROM_EMAIL: str = os.getenv("SMTP_FROM_EMAIL", "")
    SMTP_FROM_NAME: str = os.getenv("SMTP_FROM_NAME", "Evidence Analysis System")
    PORTAL_BASE_URL: str = os.getenv("PORTAL_BASE_URL", "http://localhost:5173")

    # Server
    APP_PORT: int = int(os.getenv("APP_PORT", "8000"))

    # Background Processing
    MAX_CONCURRENT_JOBS: int = int(os.getenv("MAX_CONCURRENT_JOBS", "5"))
    WORKER_CHECK_INTERVAL: int = int(os.getenv("WORKER_CHECK_INTERVAL", "5"))  # seconds

    # Celery + RabbitMQ queue processing
    CELERY_BROKER_URL: str = os.getenv("CELERY_BROKER_URL", "amqp://guest:guest@localhost:5672//")
    CELERY_RESULT_BACKEND: str = os.getenv("CELERY_RESULT_BACKEND", "rpc://")
    CELERY_TASK_QUEUE: str = os.getenv("CELERY_TASK_QUEUE", "execution_queue")
    CELERY_TASK_ROUTING_KEY: str = os.getenv("CELERY_TASK_ROUTING_KEY", "execution.process")
    CELERY_MAX_RETRIES: int = int(os.getenv("CELERY_MAX_RETRIES", "3"))
    CELERY_RETRY_BACKOFF_SECONDS: int = int(os.getenv("CELERY_RETRY_BACKOFF_SECONDS", "30"))
    CELERY_WORKER_CONCURRENCY: int = int(os.getenv("CELERY_WORKER_CONCURRENCY", "2"))
    CELERY_WORKER_POOL: str = os.getenv("CELERY_WORKER_POOL", "threads")  # "threads" avoids macOS fork-safety SIGABRT; use "prefork" on Linux

    # Execution workspace + script runtime
    EXECUTION_WORKSPACE_ROOT: str = os.getenv("EXECUTION_WORKSPACE_ROOT", "/tmp/evidence_analysis/executions")
    EXECUTION_CLEANUP_ON_SUCCESS: bool = os.getenv("EXECUTION_CLEANUP_ON_SUCCESS", "true").lower() == "true"
    PREPROCESS_SCRIPT_PATH: str = os.getenv("PREPROCESS_SCRIPT_PATH", "scripts/pre-processor/1-pre-processor.py")
    PROCESSOR_SCRIPT_PATH: str = os.getenv("PROCESSOR_SCRIPT_PATH", "scripts/processor/1-main-parallel-script.py")
    CLEANUP_SCRIPT_PATH: str = os.getenv("CLEANUP_SCRIPT_PATH", "scripts/processor/2-remove-nonvalidated-and-empty-evidences.py")
    MERGE_SCRIPT_PATH: str = os.getenv("MERGE_SCRIPT_PATH", "scripts/processor/3-merge-batch-outputs.py")
    PROCESSOR_MAX_ROWS: int = int(os.getenv("PROCESSOR_MAX_ROWS", "0"))
    # Strip rows with no AI-evaluation result (notValidated/Failed/Unsupported/blank-tag) from
    # the delivered output CSV before upload. The unfiltered merged output is always uploaded
    # to cloud storage first regardless of this setting, so disabling it only changes what the
    # deliverable looks like — the full audit trail is never lost.
    REMOVE_INVALID_ROWS_FROM_OUTPUT: bool = os.getenv("REMOVE_INVALID_ROWS_FROM_OUTPUT", "true").lower() == "true"
    # Cost-per-row estimate, keyed by ai_model_id. google/gemini-2.5-flash-lite is the
    # observed average actual_cost/row from a real mixed image+pdf run (82 rows, $0.0224
    # total); gemini-2.0-flash is derived from that same run's token counts priced at its
    # GEMINI_PRICING rate. Still overridable via the MODEL_COST_PER_INPUT_ROW env var (JSON
    # object) for any model, same as every other Settings field. Any ai_model_id not present
    # here falls back to the private _DEFAULT_ESTIMATED_COST_PER_ROW constant in
    # execution_service.py.
    MODEL_COST_PER_INPUT_ROW: Dict[str, float] = {
        "google/gemini-2.5-flash-lite": 0.000273,
        "gemini-2.0-flash": 0.000204,
    }
    ESTIMATED_TIME_SECONDS_PER_INPUT_ROW: float = float(os.getenv("ESTIMATED_TIME_SECONDS_PER_INPUT_ROW", "0.5"))

    # File Splitting Configuration
    # Manual mode: Set SPLIT_FILES and ROWS_PER_FILE to specific values
    # Dynamic mode: Leave unset or empty, system will calculate optimal splits
    SPLIT_FILES: str = os.getenv("SPLIT_FILES", "")  # "yes", "no", or "" for dynamic
    ROWS_PER_FILE: int = int(os.getenv("ROWS_PER_FILE", "0"))  # >0 for manual, 0 for dynamic

    # Dynamic Splitting Settings (used when manual settings not provided)
    ENABLE_DYNAMIC_SPLITTING: bool = os.getenv("ENABLE_DYNAMIC_SPLITTING", "true").lower() == "true"  # Master switch for dynamic logic
    MAX_SPLIT_FILES: int = int(os.getenv("MAX_SPLIT_FILES", "100"))  # Hard cap on number of splits
    MIN_ROWS_FOR_SPLITTING: int = int(os.getenv("MIN_ROWS_FOR_SPLITTING", "200"))  # Files below this use single file (lowered from 1000)
    TARGET_ROWS_PER_SPLIT_MIN: int = int(os.getenv("TARGET_ROWS_PER_SPLIT_MIN", "100"))  # Optimal range lower bound (lowered from 500)
    TARGET_ROWS_PER_SPLIT_MAX: int = int(os.getenv("TARGET_ROWS_PER_SPLIT_MAX", "500"))  # Optimal range upper bound (lowered from 2000)
    OPTIMAL_ROWS_PER_SPLIT: int = int(os.getenv("OPTIMAL_ROWS_PER_SPLIT", "200"))  # Default target rows per split (lowered from 1000)

    # Main-batch sequential processing (one level above the file splitting above): cuts a
    # large upload into sequential main batches, each processed fully (including its own
    # fine-grained split + parallel processing above) before the next one starts.
    # Manual mode: Set MAIN_FILE_SPLIT to "yes"/"no" to force batching on/off.
    # Dynamic mode (default): Leave unset/empty — batching kicks in automatically once the
    # upload exceeds MIN_ROWS_FOR_MAIN_BATCHING, the same manual/dynamic split the fine-grained
    # SPLIT_FILES setting above already uses, instead of a fixed all-or-nothing toggle.
    MAIN_FILE_SPLIT: str = os.getenv("MAIN_FILE_SPLIT", "")  # "yes", "no", or "" for dynamic
    # Below this, single main file even in dynamic mode. Also the target rows per batch once
    # batching kicks in (num_batches = ceil(row_count / this)) — every batch stays at ~this
    # size no matter how large the upload is. See _calculate_optimal_batch_count.
    MIN_ROWS_FOR_MAIN_BATCHING: int = int(os.getenv("MIN_ROWS_FOR_MAIN_BATCHING", "5000"))
    MAIN_BATCH_ROWS_PER_BATCH: int = int(os.getenv("MAIN_BATCH_ROWS_PER_BATCH", "10000"))  # Manual-mode target rows per main batch
    MAX_MAIN_BATCHES: int = int(os.getenv("MAX_MAIN_BATCHES", "200"))  # Manual-mode hard cap; also dynamic mode's cap above 20x threshold
    
    # File Upload Limits
    MAX_UPLOAD_SIZE: int = int(os.getenv("MAX_UPLOAD_SIZE", str(100 * 1024 * 1024)))  # 100MB
    # Raw string, not List[str]: pydantic-settings auto-JSON-decodes any env value for a
    # complex-typed (List/Dict) field before validators ever run, so a real ALLOWED_EXTENSIONS
    # env var in comma-separated format (as documented in .env.example) would crash the app at
    # startup with an uncaught SettingsError — the JSON branch below never gets a chance to
    # fall back to comma-splitting. Storing this as a plain str sidesteps that entirely; the
    # computed_field property below does the actual JSON-or-comma-separated parsing.
    ALLOWED_EXTENSIONS_RAW: str = os.getenv("ALLOWED_EXTENSIONS", ".csv")

    @computed_field
    @property
    def ALLOWED_EXTENSIONS(self) -> List[str]:
        return Settings.parse_list_settings(self.ALLOWED_EXTENSIONS_RAW)

    SIGNED_UPLOAD_URL_EXPIRY_SECONDS: int = int(os.getenv("SIGNED_UPLOAD_URL_EXPIRY_SECONDS", "900"))
    SIGNED_DOWNLOAD_URL_EXPIRY_SECONDS: int = int(os.getenv("SIGNED_DOWNLOAD_URL_EXPIRY_SECONDS", "600"))

    # Interactive criteria validation
    CRITERIA_VALIDATE_MAX_ITEMS: int = int(os.getenv("CRITERIA_VALIDATE_MAX_ITEMS", "25"))

    @field_validator("DEBUG", "IS_NOTIFICATION_ENABLED", mode="before")
    @classmethod
    def parse_debug_value(cls, value):
        """Allow bool-like and environment-style DEBUG/IS_NOTIFICATION_ENABLED values."""
        if isinstance(value, bool):
            return value

        if isinstance(value, str):
            raw = value.strip().lower()
            if raw in {"true", "1", "yes", "on", "debug", "development", "dev", "enabled"}:
                return True
            if raw in {"false", "0", "no", "off", "release", "production", "prod", "disabled"}:
                return False

        return value

    @field_validator("API_BASE_PATH", mode="before")
    @classmethod
    def normalize_api_base_path(cls, value):
        """Normalize to '' or a leading-slash path with no trailing slash."""
        if not isinstance(value, str):
            return value

        raw = value.strip().rstrip("/")
        if not raw:
            return ""

        return raw if raw.startswith("/") else f"/{raw}"

    @field_validator("CORS_ORIGINS", mode="before")
    @classmethod
    def parse_list_settings(cls, value):
        """Accept JSON arrays or comma-separated env values.

        Also called directly (not as a field_validator) by the ALLOWED_EXTENSIONS
        computed_field property above — ALLOWED_EXTENSIONS isn't a field_validator target
        itself since pydantic-settings would auto-JSON-decode its raw env string before this
        validator ever ran, crashing on the documented comma-separated format.
        """
        if isinstance(value, list):
            return value

        if isinstance(value, str):
            raw = value.strip()
            if not raw:
                return []

            if raw.startswith("["):
                return json.loads(raw)

            return [item.strip() for item in raw.split(",") if item.strip()]

        return value

    # Function body lives in utils/env_parsing.py — config.py only registers it.
    _validate_model_cost_per_input_row = field_validator(
        "MODEL_COST_PER_INPUT_ROW", mode="before"
    )(classmethod(parse_model_cost_settings))

    class Config:
        env_file = str(ENV_FILE_PATH)
        case_sensitive = True
        extra = "allow"


# Initialize settings
settings = Settings()
validate_environment(settings=settings, env_file_path=ENV_FILE_PATH)
