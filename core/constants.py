"""
Application-wide string constants.
Define shared values here so they are never duplicated across files.
"""

# LLM provider identifiers — must match the LLM_PROVIDER env var values.
PROVIDER_GEMINI = "gemini"
PROVIDER_OPENROUTER = "openrouter"

# API key env var prefixes — used for dynamic key discovery (KEY_1, KEY_2, ... KEY_N).
GEMINI_API_KEY_PREFIX = "GEMINI_API_KEY"
OPENROUTER_API_KEY_PREFIX = "OPENROUTER_API_KEY"

# OpenRouter API endpoints.
OPENROUTER_MODELS_URL = "https://openrouter.ai/api/v1/models"

# Evidence types — fallback/default only. The per-tenant source of truth is
# CsvSourceType.evidence_types_config (models/csv_source_type.py), so a new type or
# extension can be added per tenant without a code deploy. This constant is used to seed
# that column's default value and as a backstop for scripts run standalone (no execution
# context to resolve tenant config from), e.g. local/manual script runs.
# "excel" is .xlsx only: requirements.txt pins openpyxl (xlsx reader) but not xlrd, which
# legacy .xls files need — routing a .xls URL to pandas.read_excel() would fail at runtime.
DEFAULT_EVIDENCE_TYPES_CONFIG = [
    {"key": "image", "label": "Image", "extensions": [".jpg", ".jpeg", ".png", ".gif", ".webp", ".bmp"]},
    {"key": "pdf", "label": "PDF", "extensions": [".pdf"]},
    {"key": "excel", "label": "Excel", "extensions": [".xlsx"]},
]

# Canonical set of accepted evidence-type keys (derived from the default config above).
ALLOWED_EVIDENCE_TYPES = {item["key"] for item in DEFAULT_EVIDENCE_TYPES_CONFIG}

# File extensions per evidence type — used by the pre-processor to classify evidence URLs.
EVIDENCE_TYPE_EXTENSIONS = {item["key"]: item["extensions"] for item in DEFAULT_EVIDENCE_TYPES_CONFIG}

# Relevance tag values — written by the processor, read by report_service and cleanup script.
RELEVANCE_TAG_RELEVANT = "Relevant"
RELEVANCE_TAG_PARTIAL = "Partially Relevant"
RELEVANCE_TAG_IRRELEVANT = "Irrelevant"
RELEVANCE_TAG_NOT_VALIDATED = "notValidated"

# Full set of bucketed relevance tags — used for report aggregation.
RELEVANCE_TYPES = {
    RELEVANCE_TAG_RELEVANT,
    RELEVANCE_TAG_PARTIAL,
    RELEVANCE_TAG_IRRELEVANT,
    RELEVANCE_TAG_NOT_VALIDATED,
}

# processing_config JSONB key for the evidence-type filter.
PROCESSING_CONFIG_KEY_EVIDENCE_TYPES = "evidence_types"
