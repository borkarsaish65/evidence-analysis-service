"""
Criteria validation service.
Runs one-off evidence criteria validation using Gemini with shared token/model runtime config.
"""
from __future__ import annotations

import asyncio
import json
import logging
import mimetypes
import re
from urllib.parse import urlparse

import httpx
import typing_extensions as typing
from fastapi import HTTPException, status

from core.config import settings
from models.schemas import (
    CriteriaValidationItem,
    CriteriaValidationRequest,
    CriteriaValidationResponse,
)
from core.constants import (
    PROVIDER_GEMINI,
    PROVIDER_OPENROUTER,
    RELEVANCE_TAG_RELEVANT,
    RELEVANCE_TAG_PARTIAL,
    RELEVANCE_TAG_IRRELEVANT,
)
from utils.llm_provider import generate_content, get_llm_model_name, get_llm_provider_name, get_llm_tokens

logger = logging.getLogger(__name__)


class GeminiCriteriaResponse(typing.TypedDict):
    answers: list[str]
    reasonings: list[str]


_DEFAULT_VALIDATION_PROMPT = (
    "You are an educational evidence validator. Analyze this image as field evidence from a "
    "PBL classroom in Bihar, India. Answer each evidence criteria with ONLY 'YES' or 'NO'. "
    "Consider all visible elements and context. Explain your reasoning for each answer briefly."
)


class CriteriaValidationService:
    """Service for validating evidence URLs against ad-hoc evidence criteria."""

    @staticmethod
    def _max_items() -> int:
        raw = getattr(settings, "CRITERIA_VALIDATE_MAX_ITEMS", 25)
        return raw if isinstance(raw, int) and raw > 0 else 25

    @staticmethod
    def _detect_image_mime_from_bytes(image_bytes: bytes) -> str | None:
        # JPEG
        if image_bytes.startswith(b"\xFF\xD8\xFF"):
            return "image/jpeg"
        # PNG
        if image_bytes.startswith(b"\x89PNG\r\n\x1a\n"):
            return "image/png"
        # GIF
        if image_bytes.startswith(b"GIF87a") or image_bytes.startswith(b"GIF89a"):
            return "image/gif"
        # WebP
        if image_bytes.startswith(b"RIFF") and image_bytes[8:12] == b"WEBP":
            return "image/webp"
        # BMP
        if image_bytes.startswith(b"BM"):
            return "image/bmp"
        # TIFF
        if image_bytes.startswith(b"II*\x00") or image_bytes.startswith(b"MM\x00*"):
            return "image/tiff"
        # HEIC/HEIF family
        if len(image_bytes) >= 12 and image_bytes[4:8] == b"ftyp":
            brand = image_bytes[8:12]
            if brand in {b"heic", b"heix", b"hevc", b"hevx", b"mif1", b"msf1"}:
                return "image/heic"
            if brand in {b"avif", b"avis"}:
                return "image/avif"
        # ICO
        if image_bytes.startswith(b"\x00\x00\x01\x00"):
            return "image/x-icon"
        return None

    @staticmethod
    def _looks_like_text_payload(image_bytes: bytes) -> bool:
        sample = image_bytes[:512].lstrip()
        if not sample:
            return False
        lower = sample.lower()
        text_prefixes = (
            b"<html",
            b"<?xml",
            b"<error",
            b"{",
            b"[",
            b"<!doctype html",
        )
        return any(lower.startswith(prefix) for prefix in text_prefixes)

    @staticmethod
    def _validate_evidence_url(evidence_url: str) -> None:
        parsed = urlparse((evidence_url or "").strip())
        if parsed.scheme not in {"http", "https"} or not parsed.netloc:
            raise HTTPException(
                status_code=status.HTTP_400_BAD_REQUEST,
                detail="Invalid evidence_url. Please provide a valid http/https image URL.",
            )

    @staticmethod
    async def _download_image(evidence_url: str) -> tuple[bytes, str]:
        try:
            async with httpx.AsyncClient(timeout=20.0, follow_redirects=True) as client:
                response = await client.get(evidence_url)
        except httpx.TimeoutException as exc:
            raise HTTPException(
                status_code=status.HTTP_400_BAD_REQUEST,
                detail="Evidence URL timed out. Please provide an accessible image URL.",
            ) from exc
        except httpx.HTTPError as exc:
            raise HTTPException(
                status_code=status.HTTP_400_BAD_REQUEST,
                detail="Evidence URL is unreachable. Please provide an accessible image URL.",
            ) from exc

        if response.status_code >= 400:
            raise HTTPException(
                status_code=status.HTTP_400_BAD_REQUEST,
                detail=(
                    "Evidence URL returned an error response. "
                    "Please provide an accessible image URL."
                ),
            )

        image_bytes = response.content or b""
        if not image_bytes:
            raise HTTPException(
                status_code=status.HTTP_400_BAD_REQUEST,
                detail="Evidence URL returned empty content. Please provide a valid image URL.",
            )

        header_content_type = (response.headers.get("content-type") or "").split(";")[0].strip().lower()
        guessed_content_type = (mimetypes.guess_type(evidence_url)[0] or "").strip().lower()
        detected_content_type = CriteriaValidationService._detect_image_mime_from_bytes(image_bytes)

        selected_content_type = ""
        if detected_content_type:
            # Some object stores return application/octet-stream for image objects.
            selected_content_type = detected_content_type
        elif header_content_type.startswith("image/"):
            selected_content_type = header_content_type
        elif (
            guessed_content_type.startswith("image/")
            and header_content_type in {"", "application/octet-stream", "binary/octet-stream"}
            and not CriteriaValidationService._looks_like_text_payload(image_bytes)
        ):
            # Last-resort fallback when headers are generic but URL extension indicates image.
            selected_content_type = guessed_content_type

        if not selected_content_type.startswith("image/"):
            raise HTTPException(
                status_code=status.HTTP_400_BAD_REQUEST,
                detail=(
                    "Evidence URL did not return a valid image payload. "
                    "Please verify the link is public and points directly to an image file."
                ),
            )

        return image_bytes, selected_content_type or "image/jpeg"

    @staticmethod
    def _build_prompt(prompt: str | None, evidence_criteria: list[str]) -> str:
        base_prompt = (prompt or "").strip() or _DEFAULT_VALIDATION_PROMPT
        criteria_lines = "\n".join(f"{index + 1}. {item}" for index, item in enumerate(evidence_criteria))
        return (
            f"{base_prompt}\n\nEvidence Criteria:\n{criteria_lines}\n\n"
            "Return ONLY valid JSON in this shape:\n"
            "{\"answers\": [\"YES|NO\", ...], \"reasonings\": [\"...\", ...]}\n"
            f"Both arrays must contain exactly {len(evidence_criteria)} entries."
        )

    @staticmethod
    def _extract_json_payload(raw_text: str) -> dict:
        text = (raw_text or "").strip()
        if not text:
            return {}

        try:
            parsed = json.loads(text)
            return parsed if isinstance(parsed, dict) else {}
        except json.JSONDecodeError:
            pass

        fence_match = re.search(r"\{.*\}", text, flags=re.DOTALL)
        if not fence_match:
            return {}

        try:
            parsed = json.loads(fence_match.group(0))
            return parsed if isinstance(parsed, dict) else {}
        except json.JSONDecodeError:
            return {}

    @staticmethod
    def _normalize_output(
        payload: dict,
        *,
        evidence_criteria: list[str],
        model_name: str,
        source: str = PROVIDER_GEMINI,
    ) -> CriteriaValidationResponse:
        raw_answers = payload.get("answers")
        raw_reasonings = payload.get("reasonings")

        answers = [str(item or "").strip() for item in raw_answers] if isinstance(raw_answers, list) else []
        reasonings = [str(item or "").strip() for item in raw_reasonings] if isinstance(raw_reasonings, list) else []

        criteria_count = len(evidence_criteria)

        if len(answers) < criteria_count:
            answers.extend(["NO"] * (criteria_count - len(answers)))
        if len(reasonings) < criteria_count:
            reasonings.extend(["Reasoning unavailable."] * (criteria_count - len(reasonings)))

        answers = answers[:criteria_count]
        reasonings = reasonings[:criteria_count]

        yes_count = sum(1 for answer in answers if answer.upper().startswith("YES"))
        ratio = (yes_count / criteria_count) if criteria_count else 0.0

        relevance_tag: str
        if ratio >= 0.5:
            relevance_tag = RELEVANCE_TAG_RELEVANT
        elif yes_count > 0:
            relevance_tag = RELEVANCE_TAG_PARTIAL
        else:
            relevance_tag = RELEVANCE_TAG_IRRELEVANT

        criteria_results = [
            CriteriaValidationItem(
                evidence_criteria=criteria,
                answer=answers[index] or "NO",
                reasoning=reasonings[index] or "Reasoning unavailable.",
            )
            for index, criteria in enumerate(evidence_criteria)
        ]

        return CriteriaValidationResponse(
            source=source,
            model=model_name,
            relevance_tag=relevance_tag,
            criteria_results=criteria_results,
            answers=answers,
            reasonings=reasonings,
        )

    @staticmethod
    def _is_quota_error(message: str) -> bool:
        lower_message = message.lower()
        quota_markers = (
            "rate limit",
            "quota",
            "resource_exhausted",
            "429",
            "too many requests",
        )
        return any(marker in lower_message for marker in quota_markers)

    @staticmethod
    def _is_auth_error(message: str) -> bool:
        lower_message = message.lower()
        auth_markers = (
            "api key not valid",
            "invalid api key",
            "permission denied",
            "unauthenticated",
            "403",
            "401",
        )
        return any(marker in lower_message for marker in auth_markers)

    @staticmethod
    def _is_bad_image_input_error(message: str) -> bool:
        lower_message = message.lower()
        image_markers = (
            "invalid image",
            "unsupported mime",
            "mime type",
            "could not decode",
            "failed to process image",
            "invalid argument",
            "malformed",
            "image bytes",
        )
        if "invalid argument" in lower_message and "response_schema" in lower_message:
            return False
        if "invalid argument" in lower_message and "response_mime_type" in lower_message:
            return False
        return any(marker in lower_message for marker in image_markers)

    async def validate_criteria(
        self,
        request_data: CriteriaValidationRequest,
        *,
        user_id: str,
    ) -> CriteriaValidationResponse:
        del user_id  # Reserved for future auditing.

        evidence_url = request_data.evidence_url.strip()
        evidence_criteria = request_data.evidence_criteria
        max_items = self._max_items()
        if len(evidence_criteria) > max_items:
            raise HTTPException(
                status_code=status.HTTP_400_BAD_REQUEST,
                detail=f"Maximum {max_items} evidence criteria are allowed.",
            )
        self._validate_evidence_url(evidence_url)

        image_bytes, mime_type = await self._download_image(evidence_url)
        prompt_text = self._build_prompt(request_data.prompt, evidence_criteria)

        llm_tokens = get_llm_tokens()
        if not llm_tokens:
            raise HTTPException(
                status_code=status.HTTP_503_SERVICE_UNAVAILABLE,
                detail="LLM configuration is missing. Please contact support.",
            )

        model_name = get_llm_model_name()
        provider_name = get_llm_provider_name()
        last_error: Exception | None = None

        if provider_name == PROVIDER_GEMINI:
            structured_generation_config: dict = {
                "response_mime_type": "application/json",
                "response_schema": GeminiCriteriaResponse,
            }
        elif provider_name == PROVIDER_OPENROUTER:
            structured_generation_config = {"response_format": "json_object"}
        else:
            structured_generation_config = {}

        logger.debug(
            "criteria_validation_request  provider=%s  model=%s  generation_config_keys=%s  criteria_count=%d",
            provider_name, model_name, list(structured_generation_config.keys()), len(evidence_criteria),
        )

        for token in llm_tokens:
            try:
                content_parts = [{"mime_type": mime_type, "data": image_bytes}, prompt_text]
                try:
                    response = await asyncio.to_thread(
                        generate_content,
                        content_parts,
                        api_key=token,
                        model_name=model_name,
                        generation_config=structured_generation_config,
                    )
                except Exception as strict_exc:  # noqa: BLE001
                    strict_error = str(strict_exc).lower()
                    if any(
                        marker in strict_error
                        for marker in ("response_schema", "response_mime_type", "unknown field")
                    ):
                        logger.warning(
                            "%s SDK/config compatibility issue for model=%s. Retrying without schema config.",
                            provider_name, model_name,
                        )
                        response = await asyncio.to_thread(
                            generate_content,
                            content_parts,
                            api_key=token,
                            model_name=model_name,
                        )
                    else:
                        raise

                response_text = getattr(response, "text", "") or ""
                payload = self._extract_json_payload(response_text)
                logger.debug(
                    "criteria_validation_response  provider=%s  model=%s  response_chars=%d  payload_parsed=%s  payload_keys=%s",
                    provider_name, model_name, len(response_text), bool(payload), list(payload.keys()),
                )
                if not payload:
                    logger.warning(
                        "criteria_validation_empty_payload  provider=%s  model=%s  response_preview=%s",
                        provider_name, model_name, response_text[:200],
                    )
                normalized_response = self._normalize_output(
                    payload,
                    evidence_criteria=evidence_criteria,
                    model_name=model_name,
                    source=provider_name,
                )
                return normalized_response
            except Exception as exc:  # noqa: BLE001
                last_error = exc
                error_message = str(exc)
                if self._is_quota_error(error_message) or self._is_auth_error(error_message):
                    continue
                if self._is_bad_image_input_error(error_message):
                    raise HTTPException(
                        status_code=status.HTTP_400_BAD_REQUEST,
                        detail=(
                            "Evidence URL did not return a valid image payload for Gemini processing. "
                            "Please use a direct public image URL."
                        ),
                    ) from exc
                logger.exception("Gemini criteria validation failed with non-retriable error: %s", error_message)
                raise HTTPException(
                    status_code=status.HTTP_502_BAD_GATEWAY,
                    detail="Gemini validation failed. Please retry in a moment.",
                ) from exc

        if last_error and self._is_quota_error(str(last_error)):
            detail = "Gemini quota/rate limit reached across configured keys. Please retry shortly."
        elif last_error and self._is_auth_error(str(last_error)):
            detail = "Gemini authentication failed for configured keys. Please contact support."
        else:
            detail = "Unable to validate criteria with Gemini right now. Please retry shortly."

        raise HTTPException(
            status_code=status.HTTP_503_SERVICE_UNAVAILABLE,
            detail=detail,
        )
