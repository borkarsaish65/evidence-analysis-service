"""
Env-value parsing functions used as pydantic-settings field_validators in core/config.py.
Kept here (not in core/config.py) so config.py stays declarations-only.
"""
import json


def parse_model_cost_settings(cls, value):
    """Accept a JSON object mapping ai_model_id -> cost-per-row from env."""
    if isinstance(value, dict):
        return value

    if isinstance(value, str):
        raw = value.strip()
        if not raw:
            return {}
        return json.loads(raw)

    return value
