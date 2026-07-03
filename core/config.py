"""
Application Configuration
Loads environment variables and application settings
"""
import json
from pathlib import Path

from pydantic import field_validator
from pydantic_settings import BaseSettings
from typing import List
from core.constants import PROVIDER_GEMINI
from env_variables import validate_environment

SERVICE_ROOT = Path(__file__).resolve().parent.parent
ENV_FILE_PATH = SERVICE_ROOT / ".env"


class Settings(BaseSettings):
    """Application settings loaded from environment variables"""
    
    # Application
    APP_NAME: str = "Evidence Analysis System"
    APP_VERSION: str = "1.0.0"
    DEBUG: bool = False

    # Base path prefix the app is mounted under (e.g. "/evidence-analysis"
    # when served behind a reverse proxy/gateway). Leave empty to serve at root.
    API_BASE_PATH: str = ""
    
    # Database
    DATABASE_URL: str = ""
    
    # JWT Authentication
    JWT_SECRET_KEY: str = ""
    JWT_ALGORITHM: str = "HS256"
    JWT_ACCESS_TOKEN_EXPIRE_MINUTES: int = 1440
    
    # CORS
    CORS_ORIGINS: List[str] = ["http://localhost:5173", "http://localhost:3000"]

    # Execution defaults
    DEFAULT_TENANT_CODE: str = "default"
    DEFAULT_ORGANIZATION_CODE: str = "default_code"

    # Entity Management Service
    ENTITY_MGMT_BASE_URL: str = ""
    ENTITY_MGMT_TENANT_ID: str = "shikshalokam"
    ENTITY_MGMT_ORIGIN: str = "https://dev.elevate-sandbox.shikshalokam.org"
    ENTITY_MGMT_TIMEOUT_SECONDS: float = 10.0
    ENTITY_MGMT_RETRY_ATTEMPTS: int = 2
    ENTITY_MGMT_RETRY_BACKOFF_SECONDS: float = 0.5
    ENTITY_MGMT_CACHE_ENABLED: bool = True
    ENTITY_MGMT_STATES_CACHE_TTL_SECONDS: int = 900
    
    # File Storage (standardized across AWS/GCP)
    CLOUD_ENDPOINT: str = ""
    CLOUD_STORAGE: str = "GCP"
    CLOUD_STORAGE_ACCOUNTNAME: str = ""
    CLOUD_STORAGE_BUCKETNAME: str = ""
    CLOUD_STORAGE_PROVIDER: str = "gcp"  # gcp or aws
    CLOUD_STORAGE_REGION: str = ""
    CLOUD_STORAGE_SECRET: str = ""
    CLOUD_STORAGE_BUCKET_TYPE: str = "private"
    
    # Local Storage
    LOCAL_STORAGE_PATH: str = "./uploads"
    
    # LLM Provider Selection
    LLM_PROVIDER: str = PROVIDER_GEMINI

    # AI Models (Gemini)
    GEMINI_API_KEY_1: str = ""
    GEMINI_API_KEY_2: str = ""
    GEMINI_API_KEY_3: str = ""
    GEMINI_MODEL: str = "gemini-2.5-flash"

    # OpenRouter
    OPENROUTER_MODEL: str = "google/gemini-2.5-flash-lite"
    OPENROUTER_BASE_URL: str = "https://openrouter.ai/api/v1"
    
    # Notification control
    IS_NOTIFICATION_ENABLED: bool = True
    
    # Email (SMTP)
    SMTP_HOST: str = ""
    SMTP_PORT: int = 587
    SMTP_API_KEY: str = ""
    SMTP_USER: str = ""
    SMTP_PASSWORD: str = ""
    SMTP_USE_TLS: bool = True
    SMTP_TIMEOUT_SECONDS: int = 30
    SMTP_MAX_RETRIES: int = 3
    SMTP_RETRY_BACKOFF_SECONDS: int = 1
    SMTP_FROM_EMAIL: str = ""
    SMTP_FROM_NAME: str = "Evidence Analysis System"
    PORTAL_BASE_URL: str = "http://localhost:5173"
    
    # Server
    APP_PORT: int = 8000

    # Background Processing
    MAX_CONCURRENT_JOBS: int = 5
    WORKER_CHECK_INTERVAL: int = 5  # seconds

    # Celery + RabbitMQ queue processing
    CELERY_BROKER_URL: str = "amqp://guest:guest@localhost:5672//"
    CELERY_RESULT_BACKEND: str = "rpc://"
    CELERY_TASK_QUEUE: str = "execution_queue"
    CELERY_TASK_ROUTING_KEY: str = "execution.process"
    CELERY_MAX_RETRIES: int = 3
    CELERY_RETRY_BACKOFF_SECONDS: int = 30
    CELERY_WORKER_CONCURRENCY: int = 2
    CELERY_WORKER_POOL: str = "threads"  # "threads" avoids macOS fork-safety SIGABRT; use "prefork" on Linux

    # Execution workspace + script runtime
    EXECUTION_WORKSPACE_ROOT: str = "/tmp/evidence_analysis/executions"
    EXECUTION_CLEANUP_ON_SUCCESS: bool = True
    PREPROCESS_SCRIPT_PATH: str = "scripts/pre-processor/1-pre-processor.py"
    PROCESSOR_SCRIPT_PATH: str = "scripts/processor/1-main-parallel-script.py"
    CLEANUP_SCRIPT_PATH: str = "scripts/processor/2-remove-nonvalidated-and-empty-evidences.py"
    PROCESSOR_MAX_ROWS: int = 0
    # Strip rows with no AI-evaluation result (notValidated/Failed/Unsupported/blank-tag) from
    # the delivered output CSV before upload. The unfiltered merged output is always uploaded
    # to cloud storage first regardless of this setting, so disabling it only changes what the
    # deliverable looks like — the full audit trail is never lost.
    REMOVE_INVALID_ROWS_FROM_OUTPUT: bool = True
    ESTIMATED_COST_PER_INPUT_ROW: float = 0.001
    ESTIMATED_TIME_SECONDS_PER_INPUT_ROW: float = 0.5

    # File Splitting Configuration
    # Manual mode: Set SPLIT_FILES and ROWS_PER_FILE to specific values
    # Dynamic mode: Leave unset or empty, system will calculate optimal splits
    SPLIT_FILES: str = ""  # "yes", "no", or "" for dynamic
    ROWS_PER_FILE: int = 0  # >0 for manual, 0 for dynamic
    
    # Dynamic Splitting Settings (used when manual settings not provided)
    ENABLE_DYNAMIC_SPLITTING: bool = True  # Master switch for dynamic logic
    MAX_SPLIT_FILES: int = 100  # Hard cap on number of splits
    MIN_ROWS_FOR_SPLITTING: int = 200  # Files below this use single file (lowered from 1000)
    TARGET_ROWS_PER_SPLIT_MIN: int = 100  # Optimal range lower bound (lowered from 500)
    TARGET_ROWS_PER_SPLIT_MAX: int = 500  # Optimal range upper bound (lowered from 2000)
    OPTIMAL_ROWS_PER_SPLIT: int = 200  # Default target rows per split (lowered from 1000)

    # Main-batch sequential processing (one level above the file splitting above): cuts a
    # large upload into sequential main batches, each processed fully (including its own
    # fine-grained split + parallel processing above) before the next one starts. Off by
    # default — a single main batch (today's behavior) is byte-identical either way.
    MAIN_FILE_SPLIT: bool = False  # "true" -> cut into main batches; "false" -> one main file
    MAIN_BATCH_ROWS_PER_BATCH: int = 10000  # Target rows per main batch (count is derived)
    MAX_MAIN_BATCHES: int = 200  # Hard cap on number of main batches
    
    # File Upload Limits
    MAX_UPLOAD_SIZE: int = 100 * 1024 * 1024  # 100MB
    ALLOWED_EXTENSIONS: List[str] = [".csv"]
    SIGNED_UPLOAD_URL_EXPIRY_SECONDS: int = 900
    SIGNED_DOWNLOAD_URL_EXPIRY_SECONDS: int = 600

    # Interactive criteria validation
    CRITERIA_VALIDATE_MAX_ITEMS: int = 25

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

    @field_validator("CORS_ORIGINS", "ALLOWED_EXTENSIONS", mode="before")
    @classmethod
    def parse_list_settings(cls, value):
        """Accept JSON arrays or comma-separated env values."""
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
    
    class Config:
        env_file = str(ENV_FILE_PATH)
        case_sensitive = True
        extra = "allow"


# Initialize settings
settings = Settings()
validate_environment(settings=settings, env_file_path=ENV_FILE_PATH)
