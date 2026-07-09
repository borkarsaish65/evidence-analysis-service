"""
Environment variable validation helpers.
Prints startup validation status in table format and fails fast if required values are missing/invalid.
"""
from __future__ import annotations

from pathlib import Path
from typing import Any

from core.constants import OPENROUTER_API_KEY_PREFIX, PROVIDER_GEMINI, PROVIDER_OPENROUTER


_VALID_REQUIRED_IF_OPERATORS = {"EQUALS", "NOT_EQUALS", "IN", "NOT_IN"}

ENVIRONMENT_VARIABLES: dict[str, dict[str, Any]] = {
    # Application
    "APP_NAME": {
        "message": "Application name",
        "optional": True,
        "default": "Evidence Analysis System",
    },
    "APP_VERSION": {
        "message": "Application version",
        "optional": True,
        "default": "1.0.0",
    },
    "DEBUG": {
        "message": "Debug mode",
        "optional": True,
        "default": False,
    },
    "API_BASE_PATH": {
        "message": "Base path prefix the app is mounted under (e.g. '/evidence-analysis')",
        "optional": True,
        "default": "",
    },

    # Core startup requirements
    "DATABASE_URL": {
        "message": "Required database connection URL",
        "optional": False,
    },
    "JWT_SECRET_KEY": {
        "message": "Required JWT secret key",
        "optional": False,
    },
    "JWT_ALGORITHM": {
        "message": "JWT algorithm",
        "optional": True,
        "default": "HS256",
    },
    "JWT_ACCESS_TOKEN_EXPIRE_MINUTES": {
        "message": "JWT expiry in minutes",
        "optional": True,
        "default": 1440,
    },

    # CORS
    "CORS_ORIGINS": {
        "message": "CORS allowed origins",
        "optional": True,
        "default": ["http://localhost:5173", "http://localhost:3000"],
    },

    # Scope defaults (kept optional because defaults exist in config)
    "DEFAULT_TENANT_CODE": {
        "message": "Default tenant code",
        "optional": True,
        "default": "default",
    },
    "DEFAULT_ORGANIZATION_CODE": {
        "message": "Default organization code",
        "optional": True,
        "default": "default_code",
    },

    # Entity service requirements
    "ENTITY_MGMT_BASE_URL": {
        "message": "Required Entity Management base URL",
        "optional": False,
    },
    "ENTITY_MGMT_TENANT_ID": {
        "message": "Required Entity Management tenant ID",
        "optional": False,
    },
    "ENTITY_MGMT_ORIGIN": {
        "message": "Required Entity Management origin",
        "optional": False,
    },
    "ENTITY_MGMT_TIMEOUT_SECONDS": {
        "message": "Entity management timeout seconds",
        "optional": True,
        "default": 10.0,
    },
    "ENTITY_MGMT_RETRY_ATTEMPTS": {
        "message": "Entity management retry attempts",
        "optional": True,
        "default": 2,
    },
    "ENTITY_MGMT_RETRY_BACKOFF_SECONDS": {
        "message": "Entity management retry backoff seconds",
        "optional": True,
        "default": 0.5,
    },
    "ENTITY_MGMT_CACHE_ENABLED": {
        "message": "Entity management cache enabled",
        "optional": True,
        "default": True,
    },
    "ENTITY_MGMT_STATES_CACHE_TTL_SECONDS": {
        "message": "Entity management states cache TTL seconds",
        "optional": True,
        "default": 900,
    },

    # Standardized cloud configuration (single model for AWS/GCP/local)
    "CLOUD_STORAGE_PROVIDER": {
        "message": "Required cloud storage provider",
        "optional": False,
        "possible_values": [
            "gcp",
            "aws",
        ],
    },
    "CLOUD_STORAGE": {
        "message": "Required cloud storage label",
        "optional": False,
    },
    "CLOUD_STORAGE_BUCKETNAME": {
        "message": "Required cloud bucket name for cloud providers",
        "optional": True,
        "required_if": {
            "key": "CLOUD_STORAGE_PROVIDER",
            "operator": "IN",
            "value": ["gcp", "aws"],
        },
    },
    "CLOUD_STORAGE_BUCKET_TYPE": {
        "message": "Required cloud bucket type for cloud providers",
        "optional": True,
        "default": "private",
        "required_if": {
            "key": "CLOUD_STORAGE_PROVIDER",
            "operator": "IN",
            "value": ["gcp", "aws"],
        },
    },
    "CLOUD_STORAGE_ACCOUNTNAME": {
        "message": "Required cloud account identity for cloud providers",
        "optional": True,
        "required_if": {
            "key": "CLOUD_STORAGE_PROVIDER",
            "operator": "IN",
            "value": ["gcp", "aws"],
        },
    },
    "CLOUD_STORAGE_SECRET": {
        "message": "Required cloud secret for cloud providers",
        "optional": True,
        "required_if": {
            "key": "CLOUD_STORAGE_PROVIDER",
            "operator": "IN",
            "value": ["gcp", "aws"],
        },
    },
    "CLOUD_STORAGE_REGION": {
        "message": "Required cloud region for AWS provider",
        "optional": True,
        "required_if": {
            "key": "CLOUD_STORAGE_PROVIDER",
            "operator": "EQUALS",
            "value": "aws",
        },
    },
    "CLOUD_ENDPOINT": {
        "message": "Optional cloud endpoint override",
        "optional": True,
        "default": "",
    },
    "LOCAL_STORAGE_PATH": {
        "message": "Local storage path (unused when cloud provider is aws/gcp)",
        "optional": True,
        "default": "./uploads",
    },

    # LLM provider selection
    "LLM_PROVIDER": {
        "message": "LLM provider selection (gemini or openrouter)",
        "optional": True,
        "default": PROVIDER_OPENROUTER,
        "possible_values": [PROVIDER_GEMINI, PROVIDER_OPENROUTER],
    },

    # AI models

    "GEMINI_API_KEY_1": {
        "message": "Gemini key slot 1",
        "optional": True,
    },
    "GEMINI_MODEL": {
        "message": "Gemini model",
        "optional": True,
        "default": "gemini-2.5-flash",
    },

    # AI models — OpenRouter (additive; keys are discovered dynamically:
    # OPENROUTER_API_KEY_1, OPENROUTER_API_KEY_2, ... OPENROUTER_API_KEY_N)
    "OPENROUTER_MODEL": {
        "message": "OpenRouter model name",
        "optional": True,
        "default": "google/gemini-2.5-flash-lite",
    },

    # Notification control
    "IS_NOTIFICATION_ENABLED": {
        "message": "Enable email notifications",
        "optional": True,
        "default": True,
    },

    # SMTP (required when IS_NOTIFICATION_ENABLED=true)
    "SMTP_HOST": {
        "message": "SMTP host (required when notifications enabled)",
        "optional": True,
        "required_if": {
            "key": "IS_NOTIFICATION_ENABLED",
            "operator": "EQUALS",
            "value": "true",
        },
    },
    "SMTP_PORT": {
        "message": "SMTP port",
        "optional": True,
        "default": 587,
    },
    "SMTP_API_KEY": {
        "message": "SMTP API key (SendGrid compatible)",
        "optional": True,
    },
    "SMTP_USER": {
        "message": "SMTP user (required when notifications enabled)",
        "optional": True,
        "required_if": {
            "key": "IS_NOTIFICATION_ENABLED",
            "operator": "EQUALS",
            "value": "true",
        },
    },
    "SMTP_PASSWORD": {
        "message": "SMTP password (required when notifications enabled)",
        "optional": True,
        "required_if": {
            "key": "IS_NOTIFICATION_ENABLED",
            "operator": "EQUALS",
            "value": "true",
        },
    },
    "SMTP_USE_TLS": {
        "message": "Enable SMTP STARTTLS",
        "optional": True,
        "default": True,
    },
    "SMTP_TIMEOUT_SECONDS": {
        "message": "SMTP connection timeout seconds",
        "optional": True,
        "default": 30,
    },
    "SMTP_MAX_RETRIES": {
        "message": "SMTP retry attempts (max 3)",
        "optional": True,
        "default": 3,
    },
    "SMTP_RETRY_BACKOFF_SECONDS": {
        "message": "SMTP retry base backoff seconds",
        "optional": True,
        "default": 1,
    },
    "SMTP_FROM_EMAIL": {
        "message": "SMTP from email (required when notifications enabled)",
        "optional": True,
        "required_if": {
            "key": "IS_NOTIFICATION_ENABLED",
            "operator": "EQUALS",
            "value": "true",
        },
    },
    "SMTP_FROM_NAME": {
        "message": "SMTP from name",
        "optional": True,
        "default": "Evidence Analysis System",
    },
    "PORTAL_BASE_URL": {
        "message": "Portal base URL for execution/report links in emails",
        "optional": True,
        "default": "http://localhost:5173",
    },

    # Runtime and limits
    "MAX_CONCURRENT_JOBS": {
        "message": "Maximum concurrent jobs",
        "optional": True,
        "default": 5,
    },
    "WORKER_CHECK_INTERVAL": {
        "message": "Worker check interval (seconds)",
        "optional": True,
        "default": 5,
    },
    "MAX_UPLOAD_SIZE": {
        "message": "Max upload size (bytes)",
        "optional": True,
        "default": 104857600,
    },
    "ALLOWED_EXTENSIONS": {
        "message": "Allowed upload file extensions",
        "optional": True,
        "default": [".csv"],
    },

    # Processing pipeline
    "CLEANUP_SCRIPT_PATH": {
        "message": "Post-processing cleanup script path",
        "optional": True,
        "default": "scripts/processor/2-remove-nonvalidated-and-empty-evidences.py",
    },
    "MERGE_SCRIPT_PATH": {
        "message": "Batch-output merge script path",
        "optional": True,
        "default": "scripts/processor/3-merge-batch-outputs.py",
    },
    "MAIN_FILE_SPLIT": {
        "message": "Main-batch toggle: 'yes'/'no' to force on/off, blank for dynamic",
        "optional": True,
        "default": "",
    },
    "MIN_ROWS_FOR_MAIN_BATCHING": {
        "message": "Row-count threshold below which dynamic mode uses a single main file; also the target rows per batch",
        "optional": True,
        "default": 5000,
    },
    "MAIN_BATCH_ROWS_PER_BATCH": {
        "message": "Target rows per main batch",
        "optional": True,
        "default": 10000,
    },
    "MAX_MAIN_BATCHES": {
        "message": "Hard cap on the number of main batches",
        "optional": True,
        "default": 200,
    },
    "REMOVE_INVALID_ROWS_FROM_OUTPUT": {
        "message": "Strip notValidated/Failed/blank-tag rows from the delivered output CSV",
        "optional": True,
        "default": True,
    },
}


def _is_blank(value: Any) -> bool:
    return not str(value or "").strip()


def _stringify(value: Any) -> str:
    if isinstance(value, bool):
        return "true" if value else "false"
    return str(value or "").strip()


def _setting_value(settings: Any, key: str) -> str:
    return _stringify(getattr(settings, key, ""))


def _has_any_key_for_prefix(settings: Any, prefix: str) -> bool:
    """Return True if at least one field matching PREFIX_* is present in settings."""
    return any(
        not _is_blank(str(v or ""))
        for k, v in settings.model_dump().items()
        if k.startswith(prefix + "_")
    )


def _validate_openrouter_key_group(
    settings: Any,
    prefix: str,
    provider_value: str,
    failures: list,
    table_rows: list,
) -> None:
    """Group requirement check for OpenRouter — additive, mirrors GEMINI_KEYS_GROUP's shape."""
    key_present = _has_any_key_for_prefix(settings, prefix)
    if not key_present:
        failures.append(
            f"{prefix}_1 (or _2, _3, ...): "
            f"At least one key is required when LLM_PROVIDER={provider_value}"
        )
    table_rows.append([
        "OPENROUTER_KEYS_GROUP",
        "YES",
        "SET" if key_present else "MISSING",
        "PASSED" if key_present else "FAILED",
        "" if key_present else f"At least one key is required when LLM_PROVIDER={provider_value}",
    ])


def _normalize_compare(value: Any) -> str:
    return _stringify(value).lower()


def _evaluate_required_if(rule: dict[str, Any], values: dict[str, str]) -> bool:
    required_if = rule.get("required_if")
    if not isinstance(required_if, dict):
        return False

    dep_key = _stringify(required_if.get("key"))
    operator = _stringify(required_if.get("operator")).upper()
    expected = required_if.get("value")
    if not dep_key or operator not in _VALID_REQUIRED_IF_OPERATORS:
        return False

    current = _normalize_compare(values.get(dep_key, ""))

    if operator == "EQUALS":
        return current == _normalize_compare(expected)
    if operator == "NOT_EQUALS":
        return current != _normalize_compare(expected)
    if operator == "IN":
        expected_values = expected if isinstance(expected, list) else [expected]
        normalized = {_normalize_compare(item) for item in expected_values}
        return current in normalized
    if operator == "NOT_IN":
        expected_values = expected if isinstance(expected, list) else [expected]
        normalized = {_normalize_compare(item) for item in expected_values}
        return current not in normalized

    return False


def _normalize_provider(raw_provider: str, raw_storage: str) -> str:
    provider = (raw_provider or raw_storage).strip().lower()
    aliases = {
        "gcp": "gcp",
        "aws": "aws",
    }
    return aliases.get(provider, "")


def _format_table(headers: list[str], rows: list[list[str]]) -> str:
    widths = [len(h) for h in headers]
    for row in rows:
        for index, cell in enumerate(row):
            widths[index] = max(widths[index], len(cell))

    def _line(values: list[str]) -> str:
        padded = [values[i].ljust(widths[i]) for i in range(len(values))]
        return "| " + " | ".join(padded) + " |"

    separator = "+-" + "-+-".join("-" * w for w in widths) + "-+"
    parts = [separator, _line(headers), separator]
    parts.extend(_line(row) for row in rows)
    parts.append(separator)
    return "\n".join(parts)


def _load_env_file_values(env_file_path: str | Path) -> dict[str, str]:
    values: dict[str, str] = {}
    path = Path(env_file_path)
    if not path.exists():
        return values

    for raw_line in path.read_text(encoding="utf-8").splitlines():
        line = raw_line.strip()
        if not line or line.startswith("#") or "=" not in line:
            continue
        key, value = line.split("=", 1)
        values[key.strip()] = value.strip()
    return values


def validate_environment(settings: Any, env_file_path: str | Path) -> None:
    """
    Validate required environment configuration and raise RuntimeError on failure.
    Prints a pass/fail table for every env key in ENVIRONMENT_VARIABLES.
    """
    env_file_values = _load_env_file_values(env_file_path)

    def _resolve_value(key: str) -> str:
        # Prefer pydantic-parsed settings values; fallback to raw .env for legacy keys.
        parsed = _setting_value(settings, key)
        if not _is_blank(parsed):
            return parsed
        return _stringify(env_file_values.get(key, ""))

    current_values = {
        key: _resolve_value(key)
        for key in ENVIRONMENT_VARIABLES.keys()
    }

    table_rows: list[list[str]] = []
    failures: list[str] = []

    for key, spec in ENVIRONMENT_VARIABLES.items():
        optional = bool(spec.get("optional", True))
        required = (not optional) or _evaluate_required_if(spec, current_values)
        value = current_values.get(key, "")

        default_value = spec.get("default")
        if _is_blank(value) and default_value is not None and _stringify(default_value) != "":
            value = _stringify(default_value)
            current_values[key] = value
            if hasattr(settings, key):
                setattr(settings, key, value)

        present = not _is_blank(value)
        status = "PASSED"
        notes = ""

        if required and not present:
            status = "FAILED"
            notes = _stringify(spec.get("message")) or f"{key} is required"
            failures.append(f"{key}: {notes}")
        elif present and isinstance(spec.get("possible_values"), list):
            possible_values = [_normalize_compare(item) for item in spec["possible_values"]]
            if _normalize_compare(value) not in possible_values:
                status = "FAILED"
                notes = (
                    f"{_stringify(spec.get('message'))}. "
                    f"Valid values: {', '.join(map(str, spec['possible_values']))}"
                )
                failures.append(f"{key}: {notes}")

        table_rows.append(
            [
                key,
                "YES" if required else "NO",
                "SET" if present else "MISSING",
                status,
                notes,
            ]
        )

    # Group requirement: at least one key must be present for the active provider.
    active_provider = _normalize_compare(current_values.get("LLM_PROVIDER", PROVIDER_GEMINI) or PROVIDER_GEMINI)

    if active_provider == PROVIDER_OPENROUTER:
        _validate_openrouter_key_group(settings, OPENROUTER_API_KEY_PREFIX, PROVIDER_OPENROUTER, failures, table_rows)
    elif active_provider == PROVIDER_GEMINI:
        gemini_keys = [
            _setting_value(settings, "GEMINI_TOKEN"),
            _setting_value(settings, "GEMINI_API_KEY"),
            _setting_value(settings, "GEMINI_API_KEYS"),
            _setting_value(settings, "GEMINI_API_KEY_1"),
            _setting_value(settings, "GEMINI_API_KEY_2"),
            _setting_value(settings, "GEMINI_API_KEY_3"),
        ]
        gemini_present = any(not _is_blank(item) for item in gemini_keys)
        gemini_status = "PASSED" if gemini_present else "FAILED"
        gemini_notes = ""
        if not gemini_present:
            gemini_notes = "At least one Gemini key is required"
            failures.append(
                "GEMINI_API_KEY_1 (or GEMINI_API_KEY / GEMINI_TOKEN / GEMINI_API_KEYS): "
                "At least one Gemini key is required"
            )
        table_rows.append(
            [
                "GEMINI_KEYS_GROUP",
                "YES",
                "SET" if gemini_present else "MISSING",
                gemini_status,
                gemini_notes,
            ]
        )
    else:
        failures.append(f"LLM_PROVIDER={active_provider!r} is not supported. Add a key-group validation block for this provider.")

    provider = _normalize_provider(
        _setting_value(settings, "CLOUD_STORAGE_PROVIDER"),
        _setting_value(settings, "CLOUD_STORAGE"),
    )
    if not provider:
        failures.append("CLOUD_STORAGE_PROVIDER: must resolve to one of gcp, aws")
        table_rows.append(
            [
                "CLOUD_STORAGE_PROVIDER_RESOLVED",
                "YES",
                "MISSING",
                "FAILED",
                "CLOUD_STORAGE_PROVIDER must resolve to one of: gcp, aws",
            ]
        )
    else:
        table_rows.append(
            [
                "CLOUD_STORAGE_PROVIDER_RESOLVED",
                "YES",
                "SET",
                "PASSED",
                f"resolved={provider}",
            ]
        )

    headers = ["ENV VAR", "REQUIRED", "PRESENT", "STATUS", "DETAILS"]
    print("\nEnvironment Validation Summary")
    print(_format_table(headers, table_rows))

    if failures:
        lines = [
            "Environment configuration validation failed. Application startup aborted.",
            f"Expected env file path: {Path(env_file_path)}",
            "Validation failures:",
            *[f"  - {failure}" for failure in failures],
        ]
        raise RuntimeError("\n".join(lines))
