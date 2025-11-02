#!/usr/bin/env python3
"""
Lead List Orchestrator - Production-Ready Single Script Implementation

Production Features:
- Circuit breakers for external service failures
- Comprehensive retry strategies with exponential backoff
- State persistence and recovery from interruptions
- Health checks and validation gates
- Detailed observability and error tracking
- Contact deduplication across enrichment rounds
- Safe failure modes with partial results delivery

Steps:
1. Query Supabase research database for existing companies and apply HubSpot suppression.
2. If more companies are needed, call the discovery webhook repeatedly until the
   1.5x quantity buffer is satisfied (or max attempts reached), applying HubSpot
   suppression after every batch.
3. Enrich surviving companies via the n8n company enrichment webhook. Companies
   without decision makers are discarded.
4. For each decision maker, verify emails through the email verification webhook
   until verified. Only verified contacts move forward.
5. Enrich verified contacts via the contact enrichment webhook.

Environment variables configure API endpoints and credentials. Run with
`python lead_pipeline.py --state KS --pms AppFolio --quantity 20`.
"""

from __future__ import annotations

import argparse
import json
import logging
import math
import os
import sys
import time
import urllib.error
import urllib.parse
import urllib.request
import re
import random
from dataclasses import dataclass, field, replace
from datetime import datetime, timezone
from typing import Any, Dict, Iterable, List, Optional, Set, Tuple
from pathlib import Path
import gc
import asyncio
from concurrent.futures import ThreadPoolExecutor, as_completed
import csv
import uuid
import smtplib
from email.message import EmailMessage
import traceback
import hashlib
from enum import Enum
from contextlib import contextmanager
import signal
from log_capture import RequestLogCapture

US_STATE_ABBREVIATIONS: Dict[str, str] = {
    "AL": "Alabama",
    "AK": "Alaska",
    "AZ": "Arizona",
    "AR": "Arkansas",
    "CA": "California",
    "CO": "Colorado",
    "CT": "Connecticut",
    "DE": "Delaware",
    "FL": "Florida",
    "GA": "Georgia",
    "HI": "Hawaii",
    "ID": "Idaho",
    "IL": "Illinois",
    "IN": "Indiana",
    "IA": "Iowa",
    "KS": "Kansas",
    "KY": "Kentucky",
    "LA": "Louisiana",
    "ME": "Maine",
    "MD": "Maryland",
    "MA": "Massachusetts",
    "MI": "Michigan",
    "MN": "Minnesota",
    "MS": "Mississippi",
    "MO": "Missouri",
    "MT": "Montana",
    "NE": "Nebraska",
    "NV": "Nevada",
    "NH": "New Hampshire",
    "NJ": "New Jersey",
    "NM": "New Mexico",
    "NY": "New York",
    "NC": "North Carolina",
    "ND": "North Dakota",
    "OH": "Ohio",
    "OK": "Oklahoma",
    "OR": "Oregon",
    "PA": "Pennsylvania",
    "RI": "Rhode Island",
    "SC": "South Carolina",
    "SD": "South Dakota",
    "TN": "Tennessee",
    "TX": "Texas",
    "UT": "Utah",
    "VT": "Vermont",
    "VA": "Virginia",
    "WA": "Washington",
    "WV": "West Virginia",
    "WI": "Wisconsin",
    "WY": "Wyoming",
    "DC": "District of Columbia",
}

STATE_NAME_TO_ABBR: Dict[str, str] = {name.lower(): abbr for abbr, name in US_STATE_ABBREVIATIONS.items()}

NON_PM_DISQUALIFIER_KEYWORDS = [
    # Explicit negations (high confidence)
    "not a property management",
    "not property management",
    "not property-manager",
    "no longer manages properties",
    "does not manage properties",
    "discontinued property management",

    # Pure software companies (be more specific)
    "we are a software company",
    "pure software company",
    "saas company only",
    "exclusively software",
    "builds software for property managers",
    "property management software provider",

    # Non-PM business models (be specific)
    "marketing agency only",
    "consulting firm only",
    "exclusively consulting",

    # Specific exclusions
    "hoa management only",
    "homeowners association only",
    "vacation rental only",
    "short-term rental only",
    "hotel management only",
]

PM_POSITIVE_KEYWORDS = [
    "property management",
    "property manager",
    "rental homes",
    "residential management",
    "multifamily management",
    "single family homes",
    "leasing services",
    "rentals",
    "landlord services",
    "tenant services",
]


def normalize_location_text(value: Optional[str]) -> str:
    """Normalize free-form location text for fuzzy comparisons."""
    if not value:
        return ""
    text = value.strip().lower()
    if not text:
        return ""
    return re.sub(r"[^a-z0-9]+", " ", text).strip()


def normalize_state_token(value: Optional[str]) -> Optional[str]:
    """Normalize potential state string to its two-letter abbreviation (lowercase)."""
    if not value:
        return None
    token = value.strip()
    if not token:
        return None

    upper = token.upper()
    if len(upper) == 2 and upper in US_STATE_ABBREVIATIONS:
        return upper.lower()

    lower = token.lower()
    if lower in STATE_NAME_TO_ABBR:
        return STATE_NAME_TO_ABBR[lower].lower()

    # Handle strings like "TN - Tennessee" by splitting on delimiters
    pieces = [piece.strip() for piece in re.split(r"[,/;|\-]", token) if piece.strip()]
    for piece in pieces:
        if piece.lower() == lower:
            continue
        normalized = normalize_state_token(piece)
        if normalized:
            return normalized

    # Attempt to find two-letter abbreviations within whitespace-separated tokens
    for candidate in token.split():
        candidate = candidate.strip()
        if not candidate or candidate.lower() == lower:
            continue
        normalized = normalize_state_token(candidate)
        if normalized:
            return normalized
    return None


def split_location_tokens(value: str) -> List[str]:
    """Split compound location strings into comparable tokens."""
    return [token.strip() for token in re.split(r"[,/;|]", value) if token.strip()]


def value_to_strings(value: Any) -> List[str]:
    """Convert nested location values into a flat list of strings."""
    if not value:
        return []
    if isinstance(value, str):
        return [value]
    if isinstance(value, (int, float)):
        return [str(value)]
    if isinstance(value, list):
        results: List[str] = []
        for item in value:
            if isinstance(item, (str, int, float)):
                results.append(str(item))
        return results
    if isinstance(value, dict):
        results: List[str] = []
        for item in value.values():
            results.extend(value_to_strings(item))
        return results
    return []


def parse_location_to_city_state(location: Optional[str]) -> Tuple[Optional[str], Optional[str]]:
    """Attempt to derive city/state components from a free-form location string."""
    if not location:
        return None, None
    text = location.strip()
    if not text:
        return None, None

    # Handle format "City ST"
    match = re.match(r"^(?P<city>.+?)[,\\s]+(?P<state>[A-Za-z]{2})$", text)
    if match:
        city = match.group("city").rstrip(", ").strip()
        state = match.group("state").upper()
        if state in US_STATE_ABBREVIATIONS:
            return city, state

    # Split on common separators
    parts = [part.strip() for part in re.split(r"[,/;]", text) if part.strip()]
    if not parts:
        return None, None

    derived_city: Optional[str] = None
    derived_state: Optional[str] = None

    for part in reversed(parts):
        cleaned_part = part.strip(", ")
        state_token = normalize_state_token(part)
        if state_token and not derived_state:
            derived_state = state_token.upper()
            continue
        if not derived_city:
            derived_city = cleaned_part

    if not derived_city:
        primary_part = parts[0]
        if derived_state:
            pattern = rf"[,\s]+{re.escape(derived_state)}$"
            trimmed = re.sub(pattern, "", primary_part, flags=re.IGNORECASE).strip()
            if not trimmed and derived_state in US_STATE_ABBREVIATIONS:
                full_state = US_STATE_ABBREVIATIONS[derived_state]
                pattern_full = rf"[,\s]+{re.escape(full_state)}$"
                trimmed = re.sub(pattern_full, "", primary_part, flags=re.IGNORECASE).strip()
            # Don't set derived_city if primary_part is just the state abbreviation
            if primary_part.upper() != derived_state:
                derived_city = trimmed or primary_part
        else:
            derived_city = primary_part

    if derived_city:
        derived_city = derived_city.strip(", ").strip()

    return derived_city, derived_state


def company_matches_location(
    company: Dict[str, Any],
    *,
    city: Optional[str],
    state: Optional[str],
    location_text: Optional[str],
) -> bool:
    """
    Determine whether a company record aligns with the requested geography.

    The function checks state, city, and fallback free-text matches across a variety of
    location-related fields present in both Supabase and discovery payloads.
    """
    if not (city or state or location_text):
        return True

    inferred_city = city
    inferred_state = state
    if location_text:
        derived_city, derived_state = parse_location_to_city_state(location_text)
        if not inferred_city and derived_city:
            inferred_city = derived_city
        if not inferred_state and derived_state:
            inferred_state = derived_state

    state_norm = normalize_state_token(inferred_state)
    city_norm = normalize_location_text(inferred_city)
    location_norm = normalize_location_text(location_text)

    city_sources = (
        value_to_strings(company.get("hq_city"))
        + value_to_strings(company.get("city"))
        + value_to_strings(company.get("region"))
        + value_to_strings(company.get("location"))
    )
    raw_state_sources = (
        value_to_strings(company.get("hq_state"))
        + value_to_strings(company.get("state"))
        + value_to_strings(company.get("state_operations"))
        + value_to_strings(company.get("state_of_operations"))
    )
    state_sources = raw_state_sources or []

    # Ensure state field names with underscores are normalized consistently
    if not state_sources:
        # Some datasets encode state inside region/location field
        state_sources = city_sources

    if state_norm:
        matched_state = False
        saw_state_token = False
        for candidate in state_sources:
            for token in split_location_tokens(candidate):
                normalized = normalize_state_token(token)
                if normalized:
                    saw_state_token = True
                if normalized == state_norm:
                    matched_state = True
                    break
            if matched_state:
                break
        if saw_state_token and not matched_state:
            return False

    if city_norm:
        matched_city = False
        for candidate in city_sources:
            if city_norm and city_norm in normalize_location_text(candidate):
                matched_city = True
                break
        if not matched_city:
            return False

    if not state_norm and not city_norm and location_norm:
        request_tokens = {token for token in location_norm.split() if token}
        long_tokens = {token for token in request_tokens if len(token) > 3}
        if long_tokens:
            request_tokens = long_tokens
        matched_location = False
        for candidate in city_sources + state_sources:
            normalized_candidate = normalize_location_text(candidate)
            if not normalized_candidate:
                continue
            candidate_tokens = set(normalized_candidate.split())
            if request_tokens and request_tokens.issubset(candidate_tokens):
                matched_location = True
                break
            if any(token in normalized_candidate for token in request_tokens):
                matched_location = True
                break
        if not matched_location:
            return False

    return True


def evaluate_property_management_status(
    company: Dict[str, Any],
    *,
    strict: bool = False,
) -> Tuple[bool, str]:
    """
    Determine whether a company appears to be a genuine property management firm.

    Args:
        company: Company metadata from Supabase, discovery, or enrichment.
        strict: When True, require affirmative property management signals.

    Returns:
        (allowed, reason) where allowed indicates the company should remain
        in the pipeline. When allowed is False, reason provides context for logs.
    """
    def _as_str(value: Any) -> str:
        return str(value or "").strip()

    company_name = _as_str(company.get("company_name") or company.get("name") or company.get("domain"))

    icp_fit = _as_str(company.get("icp_fit")).lower()
    icp_fit_no = icp_fit.startswith("no")

    icp_tier = _as_str(company.get("icp_tier")).upper()
    if icp_tier in {"D", "F"}:
        return False, f"icp_tier={icp_tier}"

    disqualifiers = company.get("disqualifiers")
    if isinstance(disqualifiers, str):
        disqualifiers_iter = [disqualifiers]
    elif isinstance(disqualifiers, list):
        disqualifiers_iter = disqualifiers
    else:
        disqualifiers_iter = []

    for entry in disqualifiers_iter:
        text = _as_str(entry).lower()
        if any(keyword in text for keyword in NON_PM_DISQUALIFIER_KEYWORDS):
            return False, "disqualifier"

    summary_fields = [
        company.get("agent_summary"),
        company.get("business_model"),
        company.get("description"),
        company.get("profile_summary"),
        company.get("notes"),
    ]
    summary_text = " ".join(_as_str(field) for field in summary_fields if field)
    summary_lower = summary_text.lower()

    # Check for disqualifiers, but we'll override them if there are strong positive signals
    has_disqualifier = False
    disqualifier_found = None
    if summary_lower:
        for keyword in NON_PM_DISQUALIFIER_KEYWORDS:
            if keyword in summary_lower:
                has_disqualifier = True
                disqualifier_found = keyword
                break

    qualitative_flags = [
        _as_str(company.get("single_family")).lower(),
        _as_str(company.get("single_family_units")).lower(),
    ]
    if any(flag.startswith("no") for flag in qualitative_flags) and not _as_str(company.get("unit_count")):
        return False, "single_family=no_and_units_missing"

    # Count positive signals
    positive_score = 0
    positive_reasons = []

    if icp_fit.startswith("yes"):
        positive_score += 2
        positive_reasons.append("icp_fit=yes")
    if icp_tier in {"A", "B", "C"}:
        positive_score += 2
        positive_reasons.append(f"icp_tier={icp_tier}")

    # Check for PM keywords with stronger weighting (in summary, name, and domain)
    pm_keyword_count = sum(1 for keyword in PM_POSITIVE_KEYWORDS if keyword in summary_lower)
    name_lower = company_name.lower()
    domain_lower = _as_str(company.get("domain")).lower()
    if "property management" in name_lower or "propertymanagement" in domain_lower or (
        "management" in name_lower and "property" in name_lower
    ):
        pm_keyword_count += 2
    if pm_keyword_count > 0:
        positive_score += pm_keyword_count
        positive_reasons.append(f"pm_keywords={pm_keyword_count}")

    pms_value = _as_str(company.get("pms")).lower()
    if pms_value and pms_value not in {"", "n/a", "na", "none"}:
        positive_score += 2
        positive_reasons.append(f"pms={pms_value}")

    unit_count_value = _as_str(company.get("unit_count") or company.get("units"))
    if unit_count_value:
        try:
            units = int(float(unit_count_value))
            if units > 0:
                positive_score += 1 if units < 100 else 2 if units < 1000 else 3
                positive_reasons.append(f"units={units}")
        except ValueError:
            pass

    # Strong positive signals override disqualifiers
    if positive_score >= 2:  # Lowered threshold from 3 to 2
        # Company has PM indicators, accept it
        if has_disqualifier:
            logging.debug(
                "Company has disqualifier '%s' but overridden by positive score %d: %s",
                disqualifier_found, positive_score, positive_reasons
            )
        return True, f"positive_signals({positive_score})"

    # Only reject on disqualifiers if there are NO positive signals
    if has_disqualifier and positive_score == 0:
        return False, f"disqualifier({disqualifier_found})"

    # Treat explicit icp_fit=no as a negative only when there are no positives
    if icp_fit_no and positive_score == 0:
        if strict:
            return False, "icp_fit=no"
        # non-strict: allow through but with low confidence

    # Accept companies with any positive signal, even if score is 1
    if positive_score > 0:
        return True, f"some_positive_signals({positive_score})"

    return True, ""

# ---------------------------------------------------------------------------
# HTTP utilities
# ---------------------------------------------------------------------------

def _http_request(
    method: str,
    url: str,
    *,
    headers: Optional[Dict[str, str]] = None,
    json_body: Optional[Dict[str, Any]] = None,
    params: Optional[Dict[str, Any]] = None,
    timeout: float = 60.0,
    max_retries: int = 4,
    retry_backoff: float = 2.0,
) -> Any:
    """
    Perform an HTTP request with JSON support and lightweight retry handling.

    Args:
        method: HTTP method (GET/POST/PATCH/etc.)
        url: Base URL (without query params)
        headers: Optional request headers
        json_body: Optional payload; serialized to JSON if provided
        params: Optional dict appended as query string
        timeout: Request timeout in seconds
        max_retries: Total attempts before failing
        retry_backoff: Base backoff (seconds) for retryable errors

    Returns:
        Parsed JSON response (dict/list) when possible, else decoded text.

    Raises:
        urllib.error.URLError / urllib.error.HTTPError if all retries fail.
    """
    headers = dict(headers or {})
    data: Optional[bytes] = None

    if json_body is not None:
        headers.setdefault("Content-Type", "application/json")
        data = json.dumps(json_body).encode("utf-8")

    if params:
        encoded = urllib.parse.urlencode({k: v for k, v in params.items() if v is not None})
        url = f"{url}?{encoded}"

    attempt = 0
    while True:
        attempt += 1
        req = urllib.request.Request(url, data=data, headers=headers, method=method.upper())
        try:
            with urllib.request.urlopen(req, timeout=timeout) as resp:
                raw = resp.read()
                if not raw:
                    return {}

                content_type = resp.headers.get("Content-Type", "")
                text = raw.decode("utf-8", errors="ignore")

                if "application/json" in content_type.lower():
                    try:
                        return json.loads(text)
                    except json.JSONDecodeError:
                        logging.debug("Failed to decode JSON despite header; returning text")
                else:
                    # Attempt JSON decode regardless; fallback to text
                    try:
                        return json.loads(text)
                    except json.JSONDecodeError:
                        pass
                return text

        except urllib.error.HTTPError as exc:  # type: ignore[assignment]
            if exc.code == 429 and attempt < max_retries:
                base_wait = _retry_after_delay(exc) or (retry_backoff * attempt)
                # Add jitter: multiply by random value between 0.5 and 1.5 to avoid thundering herd
                wait_for = base_wait * (0.5 + random.random())
                logging.warning("HTTP 429 from %s; retrying in %.1fs (attempt %d/%d)", url, wait_for, attempt, max_retries)
                time.sleep(wait_for)
                continue
            if exc.code >= 500 and attempt < max_retries:
                base_wait = retry_backoff * attempt
                # Add jitter to avoid synchronized retries across concurrent requests
                wait_for = base_wait * (0.5 + random.random())
                logging.warning(
                    "Server error %s from %s; retrying in %.1fs (attempt %d/%d)",
                    exc.code,
                    url,
                    wait_for,
                    attempt,
                    max_retries,
                )
                time.sleep(wait_for)
                continue
            raise

        except urllib.error.URLError as exc:
            if attempt < max_retries:
                base_wait = retry_backoff * attempt
                # Add jitter for network errors to reduce retry collisions
                wait_for = base_wait * (0.5 + random.random())
                logging.warning("Network error %s; retrying in %.1fs (attempt %d/%d)", exc, wait_for, attempt, max_retries)
                time.sleep(wait_for)
                continue
            raise


def _retry_after_delay(error: urllib.error.HTTPError) -> Optional[float]:
    """Helper to parse Retry-After header."""
    retry_after = error.headers.get("Retry-After") if hasattr(error, "headers") else None
    if not retry_after:
        return None
    try:
        return float(retry_after)
    except ValueError:
        try:
            parsed = datetime.strptime(retry_after, "%a, %d %b %Y %H:%M:%S %Z")
            delta = parsed.replace(tzinfo=timezone.utc) - datetime.now(timezone.utc)
            return max(delta.total_seconds(), 0.0)
        except ValueError:
            return None


# ---------------------------------------------------------------------------
# Circuit breaker pattern for external service resilience
# ---------------------------------------------------------------------------

class CircuitState(Enum):
    """Circuit breaker states."""
    CLOSED = "closed"      # Normal operation
    OPEN = "open"          # Failing, reject requests
    HALF_OPEN = "half_open"  # Testing if service recovered


class CircuitBreaker:
    """
    Circuit breaker to prevent cascading failures from external services.
    
    States:
    - CLOSED: Normal operation, requests pass through
    - OPEN: Service failing, reject requests immediately
    - HALF_OPEN: Test if service recovered with limited requests
    """
    
    def __init__(
        self,
        name: str,
        failure_threshold: int = 5,
        recovery_timeout: float = 60.0,
        expected_exception: type = Exception,
    ):
        self.name = name
        self.failure_threshold = failure_threshold
        self.recovery_timeout = recovery_timeout
        self.expected_exception = expected_exception
        self.failure_count = 0
        self.last_failure_time: Optional[float] = None
        self.state = CircuitState.CLOSED
        
    def call(self, func, *args, **kwargs):
        """Execute function with circuit breaker protection."""
        if self.state == CircuitState.OPEN:
            if self.last_failure_time and time.time() - self.last_failure_time > self.recovery_timeout:
                logging.info("Circuit breaker %s entering HALF_OPEN state", self.name)
                self.state = CircuitState.HALF_OPEN
            else:
                raise RuntimeError(f"Circuit breaker {self.name} is OPEN")
        
        try:
            result = func(*args, **kwargs)
            self._on_success()
            return result
        except self.expected_exception as exc:
            self._on_failure()
            raise exc
    
    def _on_success(self):
        """Reset circuit breaker on successful call."""
        if self.state == CircuitState.HALF_OPEN:
            logging.info("Circuit breaker %s recovered, entering CLOSED state", self.name)
        self.failure_count = 0
        self.state = CircuitState.CLOSED
        
    def _on_failure(self):
        """Handle failure and potentially open circuit."""
        self.failure_count += 1
        self.last_failure_time = time.time()
        
        if self.failure_count >= self.failure_threshold:
            logging.error(
                "Circuit breaker %s OPEN after %d failures",
                self.name,
                self.failure_count,
            )
            self.state = CircuitState.OPEN


# ---------------------------------------------------------------------------
# State persistence for recovery
# ---------------------------------------------------------------------------

class StateManager:
    """Manage pipeline state for recovery from interruptions."""
    
    def __init__(self, run_dir: Path):
        self.run_dir = run_dir
        self.state_file = run_dir / "state.json"
        self.checkpoint_interval = 300  # 5 minutes
        self.last_checkpoint = time.time()
        
    def save_checkpoint(self, state: Dict[str, Any]) -> None:
        """Save current pipeline state."""
        try:
            state["checkpoint_time"] = datetime.now(timezone.utc).isoformat()
            state["checkpoint_timestamp"] = time.time()
            temp_file = self.state_file.with_suffix(".tmp")
            temp_file.write_text(json.dumps(state, indent=2))
            temp_file.replace(self.state_file)
            self.last_checkpoint = time.time()
            logging.debug("Saved checkpoint to %s", self.state_file)
        except Exception as exc:
            logging.warning("Failed to save checkpoint: %s", exc)
    
    def load_checkpoint(self) -> Optional[Dict[str, Any]]:
        """Load last saved state if exists."""
        if not self.state_file.exists():
            return None
        try:
            state = json.loads(self.state_file.read_text())
            logging.info("Loaded checkpoint from %s", state.get("checkpoint_time"))
            return state
        except Exception as exc:
            logging.warning("Failed to load checkpoint: %s", exc)
            return None
    
    def should_checkpoint(self) -> bool:
        """Check if enough time passed for next checkpoint."""
        return time.time() - self.last_checkpoint >= self.checkpoint_interval


# ---------------------------------------------------------------------------
# Nano QA Validator (uses OpenAI "gpt-5-nano" or configured model)
# ---------------------------------------------------------------------------

class _NanoValidator:
    """Lightweight LLM-assisted validator to improve reliability.

    It combines simple heuristics with a minimal LLM call to classify batches as
    PASS/RETRY/DROP and optionally returns a small fix_hint. Retries are bounded
    per phase via qa_max_retry_per_phase.
    """

    def __init__(self, config: Config):
        self.config = config
        self.retries: Dict[str, int] = {}
        self._client = None
        self._init_client()

    def _init_client(self) -> None:
        try:
            api_key = os.getenv("OPENAI_API_KEY")
            if not api_key:
                return
            try:
                from openai import OpenAI  # type: ignore
                self._client = OpenAI(api_key=api_key)
            except Exception:
                import openai  # type: ignore
                openai.api_key = api_key
                self._client = openai
        except Exception:
            self._client = None

    def should_retry(self, phase: str, attempt: int) -> bool:
        used = self.retries.get(phase, 0)
        return used < max(0, self.config.qa_max_retry_per_phase)

    def log_decision(self, phase: str, items: List[Dict[str, Any]], context: Dict[str, Any]) -> None:
        try:
            _ = self._decide(phase, items, context, log_only=True)
        except Exception:
            pass

    def decide(self, phase: str, items: List[Dict[str, Any]], context: Dict[str, Any]) -> Dict[str, Any]:
        decision = self._decide(phase, items, context, log_only=False)
        if decision.get("decision") == "RETRY":
            self.retries[phase] = self.retries.get(phase, 0) + 1
        return decision

    def _decide(self, phase: str, items: List[Dict[str, Any]], context: Dict[str, Any], *, log_only: bool) -> Dict[str, Any]:
        # Quick heuristics first
        if not items:
            return {"decision": "RETRY", "reason": "empty_batch", "fix_hint": "Increase diversity and ensure location constraint."}
        # Basic uniqueness and shape checks
        domains = set()
        bad_shape = 0
        for it in items:
            dom = (it.get("domain") or it.get("company_domain") or "").strip().lower()
            if dom:
                domains.add(dom)
            else:
                bad_shape += 1
        if bad_shape >= len(items):
            return {"decision": "RETRY", "reason": "no_domains", "fix_hint": "Ensure results include valid company domains."}
        if len(domains) <= max(1, len(items) // 3):
            return {"decision": "RETRY", "reason": "low_variety", "fix_hint": "Use different subregions to broaden coverage."}

        # LLM refinement if available and not in log-only mode
        if self._client is None or log_only:
            return {"decision": "PASS", "reason": "rules_ok"}

        try:
            sample_json = json.dumps({"phase": phase, "context": context, "sample": items[: self.config.qa_sample_size]}, ensure_ascii=False)
            system = (
                "You are a strict QA gate. Output only JSON with keys: decision (PASS|RETRY|DROP), "
                "reason, and optional fix_hint (<=120 chars). Keep it brief."
            )
            user_msg = (
                "Given this partial batch, decide if it's adequate for the next pipeline step. "
                "If RETRY, include a short fix_hint instructing how to refine location or criteria (e.g., neighborhoods).\n\n" + sample_json
            )
            content = None
            if hasattr(self._client, "chat") and hasattr(self._client.chat, "completions"):
                resp = self._client.chat.completions.create(
                    model=self.config.qa_model,
                    messages=[{"role": "system", "content": system}, {"role": "user", "content": user_msg}],
                    temperature=0.1,
                )
                content = resp.choices[0].message.content if resp and resp.choices else None
            elif hasattr(self._client, "responses"):
                resp = self._client.responses.create(
                    model=self.config.qa_model,
                    input=f"SYSTEM: {system}\nUSER: {user_msg}",
                    temperature=0.1,
                )
                content = getattr(resp, "output_text", None) or getattr(resp, "content", None)
            if not content:
                return {"decision": "PASS", "reason": "no_llm_content"}
            try:
                parsed = json.loads(content)
                # Normalize
                dec = str(parsed.get("decision", "PASS")).upper()
                if dec not in {"PASS", "RETRY", "DROP"}:
                    dec = "PASS"
                return {
                    "decision": dec,
                    "reason": parsed.get("reason", ""),
                    "fix_hint": parsed.get("fix_hint", ""),
                }
            except Exception:
                return {"decision": "PASS", "reason": "llm_parse_failed"}
        except Exception:
            return {"decision": "PASS", "reason": "llm_error"}


# ---------------------------------------------------------------------------
# Contact deduplication
# ---------------------------------------------------------------------------

class ContactDeduplicator:
    """Ensure contacts are not processed multiple times."""
    
    def __init__(self):
        self.seen_keys: Set[str] = set()
        self.attempt_count: Dict[str, int] = {}
        
    @staticmethod
    def _contact_key(contact: Dict[str, Any], company: Dict[str, Any]) -> str:
        """Generate unique key for contact."""
        email = (contact.get("email") or "").strip().lower()
        if email:
            return f"email:{email}"
        
        linkedin = (contact.get("linkedin") or "").strip().lower()
        if linkedin:
            return f"linkedin:{linkedin}"
        
        name = (contact.get("full_name") or contact.get("name") or "").strip().lower()
        company_name = (company.get("company_name") or "").strip().lower()
        if name and company_name:
            return f"name:{name}@{company_name}"
        
        return ""
    
    def is_duplicate(self, contact: Dict[str, Any], company: Dict[str, Any]) -> bool:
        """Check if contact already processed."""
        key = self._contact_key(contact, company)
        if not key:
            return False
        return key in self.seen_keys
    
    def mark_seen(self, contact: Dict[str, Any], company: Dict[str, Any]) -> None:
        """Mark contact as processed."""
        key = self._contact_key(contact, company)
        if key:
            self.seen_keys.add(key)
            self.attempt_count[key] = self.attempt_count.get(key, 0) + 1
    
    def attempt_count_for(self, contact: Dict[str, Any], company: Dict[str, Any]) -> int:
        """Get number of processing attempts for contact."""
        key = self._contact_key(contact, company)
        return self.attempt_count.get(key, 0) if key else 0


# ---------------------------------------------------------------------------
# Health checks
# ---------------------------------------------------------------------------

class HealthCheck:
    """Validate system health before and during pipeline execution."""
    
    def __init__(self, config: Config):
        self.config = config
        
    def check_all(self) -> Tuple[bool, List[str]]:
        """Run all health checks, return (success, errors)."""
        errors: List[str] = []
        
        # Configuration validation
        if not self.config.supabase_key:
            errors.append("Missing Supabase credentials")
        if not self.config.hubspot_token:
            errors.append("Missing HubSpot token")
        if not self.config.discovery_webhook_url:
            errors.append("Missing discovery webhook URL")
        if not self.config.company_enrichment_webhook:
            errors.append("Missing company enrichment webhook URL")
            
        # Network connectivity
        for name, url in [
            ("Supabase", self.config.supabase_url),
            ("HubSpot", self.config.hubspot_base_url),
        ]:
            if not self._check_connectivity(url):
                errors.append(f"{name} unreachable at {url}")
        
        return len(errors) == 0, errors
    
    def _check_connectivity(self, url: str, timeout: float = 5.0) -> bool:
        """Test basic connectivity to URL."""
        try:
            _http_request("GET", url, timeout=timeout, max_retries=1)
            return True
        except urllib.error.HTTPError as exc:  # type: ignore[attr-defined]
            # Treat client errors as reachable; we only care about network access.
            if 400 <= exc.code < 500:
                logging.debug("Connectivity check for %s returned HTTP %s, treating as reachable", url, exc.code)
                return True
            logging.debug("Connectivity check for %s failed with HTTP %s", url, exc.code)
            return False
        except Exception as exc:  # noqa: BLE001
            logging.debug("Connectivity check for %s failed: %s", url, exc)
            return False


def _load_env_file(path: str = ".env.local") -> None:
    """
    Load environment variables from a simple KEY=VALUE file if present.

    Existing environment variables take precedence.

    The loader searches the following locations in order until a file is found:
    1. Explicit override via LEAD_PIPELINE_ENV_FILE environment variable.
    2. The provided `path` relative to the current working directory.
    3. The same path relative to this script's directory.
    4. The same path relative to the project root (parent of this file).
    """
    if not path:
        return

    candidates: List[Path] = []
    override = os.getenv("LEAD_PIPELINE_ENV_FILE")
    if override:
        candidates.append(Path(override).expanduser())

    raw_path = Path(path)
    if raw_path.is_absolute():
        candidates.append(raw_path)
    else:
        candidates.append(Path.cwd() / raw_path)

    script_dir = Path(__file__).resolve().parent
    candidates.append(script_dir / raw_path)
    candidates.append(script_dir.parent / raw_path)

    seen: Set[Path] = set()
    for candidate in candidates:
        try:
            resolved = candidate.resolve()
        except FileNotFoundError:
            continue
        if resolved in seen or not resolved.exists():
            continue
        seen.add(resolved)

        try:
            with resolved.open("r", encoding="utf-8") as handle:
                for raw_line in handle:
                    line = raw_line.strip()
                    if not line or line.startswith("#"):
                        continue
                    if "=" not in line:
                        continue
                    key, value = line.split("=", 1)
                    key = key.strip()
                    value = value.strip()
                    if not key:
                        continue
                    if value and len(value) >= 2 and value[0] == value[-1] and value[0] in {"'", '"'}:
                        value = value[1:-1]
                    os.environ.setdefault(key, value)
            # Stop after the first successfully loaded file.
            return
        except Exception as exc:  # pylint: disable=broad-except
            print(f"Warning: failed to load environment file {resolved}: {exc}", file=sys.stderr)


# ---------------------------------------------------------------------------
# Configuration
# ---------------------------------------------------------------------------

@dataclass
class Config:
    """Environment-driven configuration container with validation."""

    supabase_url: str = field(
        default_factory=lambda: (
            os.getenv("SUPABASE_URL")
            or os.getenv("NEXT_PUBLIC_SUPABASE_URL")
            or "http://10.0.131.72:8000"
        )
    )
    supabase_key: str = field(
        default_factory=lambda: (
            os.getenv("SUPABASE_SERVICE_KEY")
            or os.getenv("SUPABASE_ANON_KEY")
            or os.getenv("NEXT_PUBLIC_SUPABASE_ANON_KEY")
            or ""
        )
    )
    supabase_research_table: str = field(
        default_factory=lambda: os.getenv("SUPABASE_RESEARCH_TABLE", "research_database")
    )
    supabase_state_column: str = field(
        default_factory=lambda: os.getenv("SUPABASE_STATE_COLUMN", "hq_state")
    )
    supabase_city_column: str = field(
        default_factory=lambda: os.getenv("SUPABASE_CITY_COLUMN", "hq_city")
    )
    supabase_unit_column: str = field(
        default_factory=lambda: os.getenv("SUPABASE_UNIT_COLUMN", "unit_count_numeric")
    )
    supabase_hubspot_column: str = field(
        default_factory=lambda: os.getenv("SUPABASE_HUBSPOT_COLUMN", "hubspot_object_id")
    )
    hubspot_base_url: str = field(default_factory=lambda: os.getenv("HUBSPOT_BASE_URL", "https://api.hubspot.com"))
    hubspot_token: str = field(
        default_factory=lambda: os.getenv("HUBSPOT_PRIVATE_APP_TOKEN") or os.getenv("HUBSPOT_API_KEY", "")
    )
    discovery_webhook_url: str = field(
        default_factory=lambda: (
            os.getenv("N8N_COMPANY_DISCOVERY_WEBHOOK")
            or os.getenv("DISCOVERY_WEBHOOK_URL")
            or "http://10.0.131.72:5678/webhook/a8545407-7931-4e82-9bcc-b63c0b5658c0"
        )
    )
    company_enrichment_webhook: str = field(
        default_factory=lambda: (
            os.getenv("N8N_COMPANY_ENRICHMENT_WEBHOOK")
            or os.getenv("COMPANY_ENRICHMENT_WEBHOOK")
            or "http://10.0.131.72:5678/webhook/46251867-4994-4278-b4a9-317aebe624bd"
        )
    )
    contact_enrichment_webhook: str = field(
        default_factory=lambda: (
            os.getenv("N8N_CONTACT_ENRICH_WEBHOOK")
            or os.getenv("CONTACT_ENRICHMENT_WEBHOOK")
            or "http://10.0.131.72:5678/webhook/1d472a40-ae58-4db9-a35b-f877eb43fd91"
        )
    )
    email_verification_webhook: str = field(
        default_factory=lambda: (
            os.getenv("N8N_EMAIL_DISCOVERY_VERIFY")
            or os.getenv("EMAIL_VERIFICATION_WEBHOOK")
            or "http://10.0.131.72:5678/webhook/c9ee7e5d-7c33-4b10-ab2e-d61170a00e9f"
        )
    )
    discovery_request_timeout: float = field(
        default_factory=lambda: float(os.getenv("DISCOVERY_REQUEST_TIMEOUT", "1800"))  # 30 minutes (restored from aggressive 15min reduction)
    )
    company_enrichment_timeout: float = field(
        default_factory=lambda: float(os.getenv("COMPANY_ENRICHMENT_REQUEST_TIMEOUT", "3600"))  # 1 hour (balanced: faster than 2h, safer than 15min)
    )
    contact_enrichment_timeout: float = field(
        default_factory=lambda: float(os.getenv("CONTACT_ENRICHMENT_REQUEST_TIMEOUT", "3600"))  # 1 hour (balanced: faster than 2h, safer than 15min)
    )
    email_verification_timeout: float = field(
        default_factory=lambda: float(os.getenv("EMAIL_VERIFICATION_REQUEST_TIMEOUT", "120"))
    )
    contact_discovery_webhook: str = field(
        default_factory=lambda: os.getenv("N8N_CONTACT_DISCOVERY_WEBHOOK", "")
    )
    contact_discovery_timeout: float = field(
        default_factory=lambda: float(os.getenv("CONTACT_DISCOVERY_REQUEST_TIMEOUT", "7200"))  # 2 hours
    )
    enrichment_concurrency: int = field(
        default_factory=lambda: int(os.getenv("ENRICHMENT_CONCURRENCY", "3"))
    )
    contact_concurrency: int = field(
        default_factory=lambda: int(os.getenv("CONTACT_CONCURRENCY", "3"))
    )
    contact_min_personal_anecdotes: int = field(
        default_factory=lambda: int(os.getenv("CONTACT_MIN_PERSONAL_ANECDOTES", "0"))
    )
    contact_min_professional_anecdotes: int = field(
        default_factory=lambda: int(os.getenv("CONTACT_MIN_PROFESSIONAL_ANECDOTES", "0"))
    )
    contact_min_total_anecdotes: int = field(
        default_factory=lambda: int(os.getenv("CONTACT_MIN_TOTAL_ANECDOTES", "1"))
    )
    contact_allow_personalization_fallback: bool = field(
        default_factory=lambda: os.getenv("CONTACT_ALLOW_PERSONALIZATION_FALLBACK", "true").lower() == "true"
    )
    contact_allow_seed_url_fallback: bool = field(
        default_factory=lambda: os.getenv("CONTACT_ALLOW_SEED_URL_FALLBACK", "true").lower() == "true"
    )
    hubspot_account_id: str = field(
        default_factory=lambda: os.getenv("HUBSPOT_ACCOUNT_ID", "")
    )
    notify_email: str = field(
        default_factory=lambda: os.getenv("NOTIFICATION_EMAIL", os.getenv("RUN_REPORT_EMAIL", ""))
    )
    failsafe_email: str = field(
        default_factory=lambda: os.getenv("FAILSAFE_EMAIL", "mark@nevereverordinary.com")
    )
    owner_email: str = field(
        default_factory=lambda: os.getenv("OWNER_NOTIFICATION_EMAIL", "mlerner@rebarhq.ai")
    )
    smtp_host: str = field(default_factory=lambda: os.getenv("SMTP_HOST", ""))
    smtp_port: int = field(default_factory=lambda: int(os.getenv("SMTP_PORT", "587")))
    smtp_user: str = field(default_factory=lambda: os.getenv("SMTP_USER", ""))
    smtp_password: str = field(default_factory=lambda: os.getenv("SMTP_PASSWORD", ""))
    email_from: str = field(default_factory=lambda: os.getenv("EMAIL_FROM", "automation@rentvine.com"))
    hubspot_recent_activity_days: int = field(default_factory=lambda: int(os.getenv("HUBSPOT_RECENT_ACTIVITY_DAYS", "120")))
    hubspot_parallelism: int = field(default_factory=lambda: int(os.getenv("HUBSPOT_PARALLELISM", "3")))
    discovery_max_rounds: int = field(default_factory=lambda: int(os.getenv("DISCOVERY_MAX_ROUNDS", "6")))
    discovery_round_delay: float = field(default_factory=lambda: float(os.getenv("DISCOVERY_ROUND_DELAY", "2.0")))
    # Discovery chunking controls
    discovery_parallel_chunks: int = field(
        default_factory=lambda: int(os.getenv("DISCOVERY_PARALLEL_CHUNKS", "3"))
    )
    discovery_chunk_size: int = field(
        default_factory=lambda: int(os.getenv("DISCOVERY_CHUNK_SIZE", "10"))
    )
    discovery_chunk_timeout: float = field(
        default_factory=lambda: float(os.getenv("DISCOVERY_CHUNK_TIMEOUT", "180"))
    )
    discovery_chunk_max_retries: int = field(
        default_factory=lambda: int(os.getenv("DISCOVERY_CHUNK_MAX_RETRIES", "1"))
    )
    discovery_rate_limit_rpm: int = field(
        default_factory=lambda: int(os.getenv("DISCOVERY_RATE_LIMIT_RPM", "0"))  # 0 = disabled
    )
    email_verification_attempts: int = field(default_factory=lambda: int(os.getenv("EMAIL_VERIFICATION_ATTEMPTS", "1")))
    email_verification_delay: float = field(default_factory=lambda: float(os.getenv("EMAIL_VERIFICATION_DELAY", "2.5")))
    
    # Circuit breaker settings
    circuit_breaker_enabled: bool = field(default_factory=lambda: os.getenv("CIRCUIT_BREAKER_ENABLED", "true").lower() == "true")
    circuit_breaker_threshold: int = field(default_factory=lambda: int(os.getenv("CIRCUIT_BREAKER_THRESHOLD", "5")))
    circuit_breaker_timeout: float = field(default_factory=lambda: float(os.getenv("CIRCUIT_BREAKER_TIMEOUT", "300")))
    
    # Safety limits
    max_companies_per_run: int = field(default_factory=lambda: int(os.getenv("MAX_COMPANIES_PER_RUN", "500")))
    max_contacts_per_company: int = field(default_factory=lambda: int(os.getenv("MAX_CONTACTS_PER_COMPANY", "10")))
    max_enrichment_retries: int = field(default_factory=lambda: int(os.getenv("MAX_ENRICHMENT_RETRIES", "2")))
    request_processing_stale_seconds: int = field(
        default_factory=lambda: int(os.getenv("REQUEST_PROCESSING_STALE_SECONDS", "900"))
    )

    # QA nano validator settings
    qa_validator_enabled: bool = field(
        default_factory=lambda: os.getenv("QA_VALIDATOR_ENABLED", "false").lower() == "true"
    )
    qa_model: str = field(
        default_factory=lambda: os.getenv("QA_MODEL", os.getenv("OPENAI_MODEL", "gpt-5-nano"))
    )
    qa_sample_size: int = field(
        default_factory=lambda: int(os.getenv("QA_SAMPLE_SIZE", "5"))
    )
    qa_max_retry_per_phase: int = field(
        default_factory=lambda: int(os.getenv("QA_MAX_RETRY_PER_PHASE", "1"))
    )

    def validate(self) -> None:
        """Ensure critical configuration exists and is valid."""
        missing = []
        invalid = []
        
        # Required credentials
        if not self.supabase_key:
            missing.append("SUPABASE_SERVICE_KEY or SUPABASE_ANON_KEY")
        if not self.hubspot_token:
            missing.append("HUBSPOT_PRIVATE_APP_TOKEN")
            
        # Required endpoints
        if not self.discovery_webhook_url:
            missing.append("N8N_COMPANY_DISCOVERY_WEBHOOK")
        if not self.company_enrichment_webhook:
            missing.append("N8N_COMPANY_ENRICHMENT_WEBHOOK")
            
        # Validate timeouts are reasonable
        if self.discovery_request_timeout < 60:
            invalid.append(f"DISCOVERY_REQUEST_TIMEOUT too low: {self.discovery_request_timeout}s (min 60s)")
        if self.discovery_request_timeout > 14400:  # 4 hours max
            invalid.append(f"DISCOVERY_REQUEST_TIMEOUT too high: {self.discovery_request_timeout}s (max 4h)")
            
        # Validate concurrency limits
        if self.enrichment_concurrency < 1 or self.enrichment_concurrency > 20:
            invalid.append(f"ENRICHMENT_CONCURRENCY out of range: {self.enrichment_concurrency} (1-20)")
        if self.contact_concurrency < 1 or self.contact_concurrency > 20:
            invalid.append(f"CONTACT_CONCURRENCY out of range: {self.contact_concurrency} (1-20)")
            
        # Validate safety limits
        if self.max_companies_per_run < 1:
            invalid.append(f"MAX_COMPANIES_PER_RUN too low: {self.max_companies_per_run}")
        if self.max_contacts_per_company < 1:
            invalid.append(f"MAX_CONTACTS_PER_COMPANY too low: {self.max_contacts_per_company}")
        if self.max_enrichment_retries < 0:
            invalid.append(f"MAX_ENRICHMENT_RETRIES cannot be negative (got {self.max_enrichment_retries})")
        for name, value in [
            ("CONTACT_MIN_PERSONAL_ANECDOTES", self.contact_min_personal_anecdotes),
            ("CONTACT_MIN_PROFESSIONAL_ANECDOTES", self.contact_min_professional_anecdotes),
            ("CONTACT_MIN_TOTAL_ANECDOTES", self.contact_min_total_anecdotes),
        ]:
            if value < 0:
                invalid.append(f"{name} cannot be negative (got {value})")
        if (
            self.contact_min_total_anecdotes
            < self.contact_min_personal_anecdotes + self.contact_min_professional_anecdotes
        ):
            invalid.append(
                "CONTACT_MIN_TOTAL_ANECDOTES must be at least the sum of personal and professional minimums "
                f"(got total={self.contact_min_total_anecdotes}, required>={self.contact_min_personal_anecdotes + self.contact_min_professional_anecdotes})"
            )

        if missing:
            raise ValueError(f"Missing required configuration: {', '.join(missing)}")
        if invalid:
            raise ValueError(f"Invalid configuration: {'; '.join(invalid)}")


# ---------------------------------------------------------------------------
# Contact quality evaluation
# ---------------------------------------------------------------------------

def _normalize_string_list(value: Any) -> List[str]:
    """Normalize a list-like field into non-empty strings."""
    items: List[str] = []
    if not isinstance(value, list):
        return items
    for item in value:
        if isinstance(item, str):
            stripped = item.strip()
            if stripped:
                items.append(stripped)
        elif isinstance(item, dict):
            for key in ("value", "url", "href", "link", "text"):
                candidate = item.get(key)  # type: ignore[arg-type]
                if isinstance(candidate, str):
                    stripped = candidate.strip()
                    if stripped:
                        items.append(stripped)
                        break
    return items


def evaluate_contact_quality(contact: Dict[str, Any], config: Config) -> Tuple[bool, Dict[str, Any]]:
    """
    Determine whether an enriched contact meets anecdote quality requirements.

    Returns:
        (passed, stats) where stats includes counts and the applied decision reason.
    """
    personal_required = max(0, config.contact_min_personal_anecdotes)
    professional_required = max(0, config.contact_min_professional_anecdotes)
    total_min = max(0, config.contact_min_total_anecdotes)
    combined_required = max(total_min, personal_required + professional_required)

    email = str(contact.get("email") or "").strip().lower()
    disposable_domains = {
        "gmail.com",
        "yahoo.com",
        "hotmail.com",
        "outlook.com",
        "icloud.com",
        "aol.com",
        "protonmail.com",
        "pm.me",
    }
    if not email or email.split("@")[1] in disposable_domains:
        return False, {"reason": "non_business_email"}

    personal_count = len(_normalize_string_list(contact.get("personal_anecdotes")))
    professional_count = len(_normalize_string_list(contact.get("professional_anecdotes")))
    total_count = personal_count + professional_count
    seed_count = len(_normalize_string_list(contact.get("seed_urls")))
    personalization = contact.get("personalization") or contact.get("custom_personalization")
    has_personalization = bool(isinstance(personalization, str) and personalization.strip())

    if (
        personal_count >= personal_required
        and professional_count >= professional_required
        and total_count >= combined_required
    ):
        reason = "thresholds_met"
    elif (
        personal_required == 0
        and professional_required == 0
        and total_count >= total_min
    ):
        reason = "total_minimum"
    elif config.contact_allow_personalization_fallback and has_personalization:
        reason = "personalization_fallback"
    elif config.contact_allow_seed_url_fallback and seed_count > 0:
        reason = "seed_url_fallback"
    else:
        reason = "insufficient"

    passed = reason != "insufficient"
    stats = {
        "personal": personal_count,
        "professional": professional_count,
        "total": total_count,
        "seed_urls": seed_count,
        "has_personalization": has_personalization,
        "reason": reason,
    }
    return passed, stats


# ---------------------------------------------------------------------------
# Supabase client
# ---------------------------------------------------------------------------

class SupabaseResearchClient:
    """Lightweight Supabase REST client to fetch cached companies."""

    def __init__(self, config: Config):
        self.base_url = config.supabase_url.rstrip("/")
        self.table = config.supabase_research_table
        self.state_column = config.supabase_state_column or None
        self.city_column = config.supabase_city_column or None
        self.unit_count_column = config.supabase_unit_column or None
        self.hubspot_column = config.supabase_hubspot_column or None
        self.headers = {
            "apikey": config.supabase_key,
            "Authorization": f"Bearer {config.supabase_key}",
            "Content-Type": "application/json",
            "Prefer": "return=representation",
        }
        self._hubspot_filter_supported = True
        self.contacts_table = "contacts"

    def find_existing_companies(
        self,
        *,
        state: Optional[str],
        pms: Optional[str],
        city: Optional[str],
        unit_min: Optional[int],
        unit_max: Optional[int],
        limit: int,
        exclude_hubspot_synced: bool = True,
    ) -> List[Dict[str, Any]]:
        """Fetch matching companies from the research database."""
        def build_url(include_hubspot_filter: bool) -> str:
            import urllib.parse
            filters: List[str] = ["select=*"]
            if state and self.state_column:
                filters.append(f"{self.state_column}=eq.{state}")
            if pms:
                filters.append(f"pms=ilike.*{urllib.parse.quote(pms)}*")
            if city and self.city_column:
                filters.append(f"{self.city_column}=ilike.*{urllib.parse.quote(city)}*")
            if unit_min is not None and self.unit_count_column:
                filters.append(f"{self.unit_count_column}=gte.{unit_min}")
            if unit_max is not None and self.unit_count_column:
                filters.append(f"{self.unit_count_column}=lte.{unit_max}")
            if include_hubspot_filter and self.hubspot_column:
                filters.append(f"{self.hubspot_column}=is.null")
            filters.append(f"limit={limit}")
            if self.unit_count_column:
                filters.append(f"order={self.unit_count_column}.desc")
            filters.append("order=updated_at.desc")
            query_string = "&".join(filters)
            return f"{self.base_url}/rest/v1/{self.table}?{query_string}"

        include_hubspot = exclude_hubspot_synced and self._hubspot_filter_supported and bool(self.hubspot_column)

        logging.info("Querying Supabase research DB table '%s'", self.table)
        max_attempts = 4
        for _ in range(max_attempts):
            url = build_url(include_hubspot)
            try:
                response = _http_request("GET", url, headers=self.headers)
                break
            except urllib.error.HTTPError as exc:
                if exc.code == 404:
                    logging.warning("Supabase %s table not found or inaccessible (404)", self.table)
                    return []
                error_detail = ""
                try:
                    error_detail = exc.read().decode("utf-8", errors="ignore")
                except Exception:  # pylint: disable=broad-except
                    error_detail = ""
                if (
                    exc.code == 400
                    and include_hubspot
                    and error_detail
                    and self.hubspot_column
                    and self.hubspot_column in error_detail
                ):
                    logging.warning(
                        "Supabase table '%s' lacks %s column; retrying without HubSpot filter",
                        self.table,
                        self.hubspot_column,
                    )
                    self._hubspot_filter_supported = False
                    include_hubspot = False
                    continue
                # Disable specific filters if their columns are missing
                handled_missing_column = False
                if exc.code == 400 and error_detail:
                    for column_attr in ("state_column", "city_column", "unit_count_column"):
                        column_name = getattr(self, column_attr)
                        if column_name and column_name in error_detail:
                            logging.warning(
                                "Supabase table '%s' lacks %s column; removing related filter",
                                self.table,
                                column_name,
                            )
                            setattr(self, column_attr, None)
                            handled_missing_column = True
                            break
                if handled_missing_column:
                    continue
                logging.error(
                    "Supabase request failed: %s%s",
                    exc,
                    f" | detail: {error_detail}" if error_detail else "",
                )
                return []
        else:
            return []
        if not isinstance(response, list):
            logging.warning("Unexpected Supabase response: %s", response)
            return []

        formatted: List[Dict[str, Any]] = []
        for company in response:
            if not isinstance(company, dict):
                continue
            state_value = (
                company.get(self.state_column) if self.state_column else company.get("state") or company.get("hq_state")
            )
            city_value = (
                company.get(self.city_column) if self.city_column else company.get("city") or company.get("hq_city")
            )
            unit_value = (
                company.get(self.unit_count_column)
                if self.unit_count_column
                else company.get("unit_count_numeric") or company.get("unit_count")
            )
            formatted.append(
                {
                    "source": "supabase",
                    "supabase_id": company.get("id"),
                    "company_name": company.get("company_name") or company.get("name") or "",
                    "domain": company.get("domain") or company.get("website") or "",
                    "city": city_value or "",
                    "state": state_value or state or "",
                    "pms": company.get("pms") or pms or "",
                    "unit_count": unit_value,
                    "employee_count": company.get("employee_count"),
                    "hq_city": company.get("hq_city") or city_value or "",
                    "hq_state": company.get("hq_state") or state_value or "",
                    "company_url": company.get("company_url") or company.get("website") or "",
                    "contacts": company.get("contacts") if isinstance(company.get("contacts"), list) else [],
                    "enriched_data": company.get("enriched_data") if isinstance(company.get("enriched_data"), dict) else {},
                    "hubspot_object_id": company.get(self.hubspot_column) if self.hubspot_column else None,
                    "icp_tier": company.get("icp_tier"),
                    "icp_score": company.get("icp_score"),
                    "agent_summary": company.get("agent_summary"),
                }
            )
        logging.info("Supabase returned %d companies before suppression", len(formatted))
        return formatted

    def _request(
        self,
        method: str,
        path: str,
        payload: Optional[Dict[str, Any]] = None,
        *,
        prefer: Optional[str] = None,
    ) -> Tuple[int, Optional[Any]]:
        data: Optional[bytes] = None
        if payload is not None:
            filtered = {
                key: value
                for key, value in payload.items()
                if value not in (None, "", [], {}, float("inf"), float("-inf"))
            }
            if filtered:
                data = json.dumps(filtered).encode("utf-8")
        headers = dict(self.headers)
        if prefer:
            headers["Prefer"] = prefer
        req = urllib.request.Request(path, data=data, headers=headers, method=method)
        try:
            with urllib.request.urlopen(req, timeout=15) as resp:
                body = resp.read()
                if not body:
                    return resp.status, None
                try:
                    return resp.status, json.loads(body.decode("utf-8"))
                except json.JSONDecodeError:
                    return resp.status, body.decode("utf-8")
        except urllib.error.HTTPError as exc:  # type: ignore[attr-defined]
            error_body = exc.read().decode("utf-8", errors="ignore")
            logging.warning("Supabase %s %s failed (%s): %s", method, path, exc.code, error_body)
            return exc.code, None
        except Exception as exc:  # noqa: BLE001
            logging.warning("Supabase %s %s error: %s", method, path, exc)
            return 0, None

    @staticmethod
    def _quote(value: str) -> str:
        return urllib.parse.quote(value, safe="")

    def _patch(self, table: str, filters: Dict[str, str], payload: Dict[str, Any]) -> Optional[List[Dict[str, Any]]]:
        if not payload or not filters:
            return None
        clauses = [f"{key}=eq.{self._quote(value)}" for key, value in filters.items() if value]
        if not clauses:
            return None
        url = f"{self.base_url}/rest/v1/{table}?{'&'.join(clauses)}"
        status, data = self._request("PATCH", url, payload)
        if status in (200, 201) and isinstance(data, list):
            return data
        return None

    def _insert(self, table: str, payload: Dict[str, Any]) -> Optional[Dict[str, Any]]:
        if not payload:
            return None
        url = f"{self.base_url}/rest/v1/{table}"
        status, data = self._request(
            "POST",
            url,
            payload,
            prefer="return=representation,resolution=merge-duplicates",
        )
        if status in (200, 201):
            if isinstance(data, list) and data:
                return data[0]
            if isinstance(data, dict):
                return data
        return None

    def _upsert(self, table: str, filters: Dict[str, str], payload: Dict[str, Any]) -> Optional[Dict[str, Any]]:
        patched = self._patch(table, filters, payload)
        if patched:
            return patched[0]
        merged = dict(filters)
        merged.update(payload)
        return self._insert(table, merged)

    def fetch_pending_requests(self, limit: int = 1) -> List[Dict[str, Any]]:
        """Retrieve pending enrichment requests."""
        return self._fetch_requests_by_status(["pending", "queued"], limit)

    def fetch_processing_requests(self, limit: int = 1) -> List[Dict[str, Any]]:
        """Retrieve processing enrichment requests."""
        return self._fetch_requests_by_status(["processing"], limit)

    def _fetch_requests_by_status(
        self,
        statuses: Iterable[str],
        limit: int,
        *,
        newest_first: bool = True,
    ) -> List[Dict[str, Any]]:
        status_filter = ",".join(statuses)
        order = "request_time.desc" if newest_first else "request_time.asc"
        url = (
            f"{self.base_url}/rest/v1/enrichment_requests"
            f"?select=*&workflow_status=in.({status_filter})&order={order}&limit={limit}"
        )
        try:
            response = _http_request("GET", url, headers=self.headers, timeout=15)
            if isinstance(response, list):
                # Filter out smoke test requests
                filtered_requests = []
                for request in response:
                    request_payload = request.get("request") or {}
                    parameters = request_payload.get("parameters") or {}

                    # Check for smoke test indicators in various places
                    is_smoke_test = (
                        request_payload.get("is_smoke_test") or
                        parameters.get("is_smoke_test") or
                        request_payload.get("test_mode") or
                        parameters.get("test_mode") or
                        "smoke test" in str(request_payload.get("notes", "")).lower() or
                        "smoke test" in str(parameters.get("notes", "")).lower() or
                        "smoke_test" in str(request_payload.get("tags", "")).lower() or
                        "smoke test" in str(request.get("notes", "")).lower()
                    )

                    if is_smoke_test:
                        logging.info(
                            "Filtering out smoke test request ID %s",
                            request.get("id")
                        )
                    else:
                        filtered_requests.append(request)

                return filtered_requests
        except Exception as exc:  # noqa: BLE001
            logging.warning("Failed to fetch pending requests: %s", exc)
        return []

    def update_request_record(
        self,
        request_id: int,
        *,
        status: Optional[str] = None,
        request_payload: Optional[Dict[str, Any]] = None,
        run_logs: Optional[Dict[str, Any]] = None,
    ) -> None:
        payload: Dict[str, Any] = {}
        if status is not None:
            payload["workflow_status"] = status
        if request_payload is not None:
            payload["request"] = request_payload
        if run_logs is not None:
            payload["run_logs"] = run_logs
        if not payload:
            return
        self._patch("enrichment_requests", {"id": str(request_id)}, payload)

    def persist_company(
        self,
        original: Dict[str, Any],
        enriched: Dict[str, Any],
        contact_count: int,
    ) -> Optional[Dict[str, Any]]:
        domain = (enriched.get("domain") or original.get("domain") or "").strip().lower()
        if not domain:
            return None
        payload: Dict[str, Any] = {
            "company_name": enriched.get("company_name") or original.get("company_name"),
            "domain": domain,
            "company_url": enriched.get("company_url") or original.get("company_url"),
            "hq_city": enriched.get("hq_city") or original.get("hq_city") or original.get("city"),
            "hq_state": enriched.get("hq_state") or original.get("hq_state") or original.get("state"),
            "pms": enriched.get("pms") or original.get("pms"),
            "unit_count": enriched.get("unit_count") or original.get("unit_count"),
            "employee_count": enriched.get("employee_count"),
            "agent_summary": enriched.get("agent_summary"),
            "contact_count": contact_count,
            "contacts_found": contact_count,
            "updated_at": datetime.utcnow().isoformat(),
        }
        unit_val = payload.get("unit_count")
        if isinstance(unit_val, str):
            stripped = re.sub(r"[^0-9]", "", unit_val)
            if stripped:
                payload["unit_count"] = int(stripped)
            else:
                payload.pop("unit_count", None)
        elif isinstance(unit_val, (int, float)):
            payload["unit_count"] = int(unit_val)
        payload = {k: v for k, v in payload.items() if v not in (None, "", [], {})}
        payload.pop("unit_count_numeric", None)
        return self._upsert(self.table, {"domain": domain}, payload)

    def persist_contact(
        self,
        company_record: Optional[Dict[str, Any]],
        contact: Dict[str, Any],
    ) -> Optional[Dict[str, Any]]:
        email = (contact.get("email") or "").strip().lower()
        if not email:
            return None
        full_name = (contact.get("full_name") or "").strip()
        first_name = ""
        last_name = ""
        if full_name:
            tokens = full_name.split()
            if tokens:
                first_name = tokens[0]
                if len(tokens) > 1:
                    last_name = " ".join(tokens[1:])
        payload: Dict[str, Any] = {
            "contact_id": f"CONT-{datetime.utcnow().strftime('%Y%m%d%H%M%S')}{uuid.uuid4().hex[:6]}",
            "company_id": company_record.get("id") if isinstance(company_record, dict) else None,
            "company_name": company_record.get("company_name") if isinstance(company_record, dict) else contact.get("company_name"),
            "domain": contact.get("domain"),
            "first_name": first_name or None,
            "last_name": last_name or None,
            "email": email,
            "email_verified": bool(contact.get("email_verified")),
            "job_title": contact.get("title"),
            "linkedin_url": contact.get("linkedin"),
            "is_decision_maker": True,
            "research_status": "basic_complete",
            "outreach_status": "not_started",
            "personalization_notes": contact.get("personalization"),
            "agent_summary": contact.get("agent_summary"),
            "updated_at": datetime.utcnow().isoformat(),
        }
        payload = {k: v for k, v in payload.items() if v not in (None, "", [])}
        payload.pop("full_name", None)
        return self._upsert(self.contacts_table, {"email": email}, payload)


# ---------------------------------------------------------------------------
# HubSpot suppression
# ---------------------------------------------------------------------------

class HubSpotClient:
    """Minimal HubSpot client for suppression checks."""

    def __init__(self, config: Config):
        self.base_url = config.hubspot_base_url.rstrip("/")
        self.token = config.hubspot_token
        self.recent_activity_days = config.hubspot_recent_activity_days
        # Per-run cache for suppression decisions (domain → is_allowed boolean)
        self._suppression_cache: Dict[str, bool] = {}

    def _request(
        self,
        method: str,
        endpoint: str,
        *,
        params: Optional[Dict[str, Any]] = None,
        payload: Optional[Dict[str, Any]] = None,
        timeout: float = 30.0,
    ) -> Any:
        url = f"{self.base_url}{endpoint}"
        headers = {
            "Authorization": f"Bearer {self.token}",
            "Content-Type": "application/json",
        }
        return _http_request(method, url, headers=headers, json_body=payload, params=params, timeout=timeout)

    def search_company_by_domain(self, domain: str) -> Optional[Dict[str, Any]]:
        """Return HubSpot company record for given domain, if it exists."""
        payload = {
            "filterGroups": [
                {
                    "filters": [
                        {
                            "propertyName": "domain",
                            "operator": "EQ",
                            "value": domain,
                        }
                    ]
                }
            ],
            "properties": [
                "name",
                "domain",
                "lifecyclestage",
                "notes_last_contacted",
                "notes_last_updated",
                "hs_last_sales_activity_date",
            ],
        }
        try:
            result = self._request("POST", "/crm/v3/objects/companies/search", payload=payload)
        except urllib.error.HTTPError as exc:
            if exc.code == 404:
                return None
            raise

        if isinstance(result, dict):
            items = result.get("results") or []
            if items:
                return items[0]
        return None

    def has_recent_activity(self, company_id: str) -> bool:
        """Determine if company engaged recently."""
        params = {
            "properties": [
                "notes_last_contacted",
                "notes_last_updated",
                "hs_last_sales_activity_date",
            ]
        }
        try:
            result = self._request("GET", f"/crm/v3/objects/companies/{company_id}", params=params)
        except urllib.error.HTTPError as exc:
            if exc.code == 404:
                return False
            raise

        properties = {}
        if isinstance(result, dict):
            properties = result.get("properties", {}) or {}

        cutoff = datetime.now(timezone.utc).timestamp() - self.recent_activity_days * 86400
        for prop_name in (
            "notes_last_contacted",
            "notes_last_updated",
            "hs_last_sales_activity_date",
        ):
            value = properties.get(prop_name)
            if not value:
                continue
            try:
                if isinstance(value, (int, float)):
                    ts = float(value) / (1000 if value > 10_000_000_000 else 1)
                elif str(value).isdigit():
                    val = float(value)
                    ts = val / (1000 if val > 10_000_000_000 else 1)
                else:
                    normalized = str(value)
                    if normalized.endswith("Z"):
                        normalized = normalized[:-1] + "+00:00"
                    ts = datetime.fromisoformat(normalized).timestamp()
                if ts >= cutoff:
                    return True
            except Exception:
                continue
        return False

    def is_allowed(self, company: Dict[str, Any]) -> bool:
        """Run HubSpot suppression logic for a single company."""
        domain = (company.get("domain") or "").strip().lower()
        if not domain:
            logging.debug("Allowing %s (no domain present)", company.get("company_name"))
            return True

        # Check cache first for O(1) lookup
        if domain in self._suppression_cache:
            return self._suppression_cache[domain]

        existing = self.search_company_by_domain(domain)
        if not existing:
            self._suppression_cache[domain] = True
            return True

        properties = existing.get("properties") or {}
        lifecycle = (properties.get("lifecyclestage") or "").lower()
        if lifecycle in {"customer", "opportunity", "salesqualifiedlead"}:
            logging.info(
                "Suppressing %s (%s) due to lifecycle stage %s",
                company.get("company_name"),
                domain,
                lifecycle or "unknown",
            )
            self._suppression_cache[domain] = False
            return False

        hubspot_id = existing.get("id")
        if hubspot_id and self.has_recent_activity(hubspot_id):
            logging.info("Suppressing %s (%s) due to recent activity", company.get("company_name"), domain)
            self._suppression_cache[domain] = False
            return False

        self._suppression_cache[domain] = True
        return True

    def filter_companies(self, companies: Iterable[Dict[str, Any]]) -> List[Dict[str, Any]]:
        """Filter a list of companies down to the ones not suppressed."""
        allowed: List[Dict[str, Any]] = []
        for idx, company in enumerate(companies, start=1):
            try:
                if self.is_allowed(company):
                    allowed.append(company)
                else:
                    logging.debug("Company suppressed by HubSpot: %s", company.get("company_name"))
            except urllib.error.HTTPError as exc:
                logging.error("HubSpot error for %s: %s", company.get("company_name"), exc)
            except Exception as exc:
                logging.error("Unexpected HubSpot error for %s: %s", company.get("company_name"), exc)
            time.sleep(0.25)  # polite pacing

        logging.info("HubSpot suppression: %d allowed / %d checked", len(allowed), idx if 'idx' in locals() else 0)
        return allowed

    # --- Additions for list + ID helpers ---
    async def search_contact_by_email(self, email: str) -> Optional[str]:
        endpoint = "/crm/v3/objects/contacts/search"
        payload = {
            "filterGroups": [{
                "filters": [{
                    "propertyName": "email",
                    "operator": "EQ",
                    "value": email
                }]
            }],
            "properties": ["email"]
        }
        try:
            result = self._request("POST", endpoint, payload=payload)
            if isinstance(result, dict):
                items = result.get("results") or []
                if items:
                    return items[0].get("id")
        except Exception:
            return None
        return None

    def create_static_list(self, name: str, object_type: str) -> Optional[str]:
        # object_type: "contacts" or "companies" (HubSpot object type IDs: 0-1 contacts, 0-2 companies)
        type_id = "0-1" if object_type == "contacts" else "0-2"
        try:
            result = self._request("POST", "/crm/v3/lists", payload={
                "name": name,
                "objectTypeId": type_id,
                "processingType": "MANUAL"
            })
            if isinstance(result, dict):
                return str(result.get("listId") or result.get("id"))
        except Exception as exc:
            logging.warning("Failed to create %s list: %s", object_type, exc)
            return None
        return None

    def add_members_to_list(self, list_id: str, object_type: str, ids: List[str]) -> bool:
        if not ids:
            return True
        type_id = "0-1" if object_type == "contacts" else "0-2"
        try:
            body = {
                "objectTypeId": type_id,
                "inputs": [{"id": _id} for _id in ids]
            }
            self._request("POST", f"/crm/v3/lists/{list_id}/memberships/batch/add", payload=body)
            return True
        except Exception as exc:
            logging.warning("Failed adding %s members to list %s: %s", object_type, list_id, exc)
            return False


# ---------------------------------------------------------------------------
# Discovery webhook
# ---------------------------------------------------------------------------

class DiscoveryWebhookClient:
    """Wrapper around the discovery webhook that seeds new companies."""

    def __init__(self, url: str, timeout: float):
        self.url = url
        self.timeout = max(60.0, timeout)

    def discover(
        self,
        *,
        location: Optional[str],
        state: Optional[str],
        pms: Optional[str],
        quantity: int,
        unit_count_min: Optional[int],
        unit_count_max: Optional[int],
        suppression_domains: Iterable[str],
        extra_requirements: Optional[str],
        attempt: int,
        chunk_filters: Optional[Dict[str, Any]] = None,
        override_timeout: Optional[float] = None,
    ) -> List[Dict[str, Any]]:
        payload = {
            "location": location,
            "state": state,
            "pms": pms,
            "quantity": quantity,
            "suppression_list": list(sorted({d for d in suppression_domains if d})),
            "requirements": extra_requirements,
            "attempt": attempt,
        }
        if unit_count_min is not None:
            payload["unit_count_min"] = unit_count_min
        if unit_count_max is not None:
            payload["unit_count_max"] = unit_count_max
        if chunk_filters:
            payload["chunk_filters"] = chunk_filters
        logging.info("Calling discovery webhook (attempt %d) for %d companies", attempt, quantity)
        try:
            result = _http_request(
                "POST",
                self.url,
                json_body=payload,
                timeout=(override_timeout or self.timeout),
            )
        except Exception as exc:  # pylint: disable=broad-except
            logging.error("Discovery webhook attempt %d failed: %s", attempt, exc)
            return []
        return self._parse_companies(result)

    def _parse_companies(self, response: Any) -> List[Dict[str, Any]]:
        """Normalize arbitrary webhook responses into company records."""
        collected: List[Dict[str, Any]] = []

        def collect_from_node(node: Any) -> None:
            if isinstance(node, list):
                for item in node:
                    collect_from_node(item)
                return
            if not isinstance(node, dict):
                return

            # Direct company list
            companies = node.get("companies")
            if isinstance(companies, list):
                for company in companies:
                    if isinstance(company, dict):
                        collected.append(company)

            final_results = node.get("final_results")
            if isinstance(final_results, list):
                for company in final_results:
                    if isinstance(company, dict):
                        collected.append(company)

            # Results payload may be list or dict
            results = node.get("results")
            if isinstance(results, dict):
                collect_from_node(results)
            elif isinstance(results, list):
                collect_from_node(results)

            # Some payloads tuck entities under message.content.*
            if "message" in node and isinstance(node["message"], dict):
                collect_from_node(node["message"])
            if "content" in node and isinstance(node["content"], dict):
                collect_from_node(node["content"])

        collect_from_node(response)

        if collected:
            return [self._normalize_company(company) for company in collected]

        if isinstance(response, list):
            # n8n flows often wrap in a single-element list
            if len(response) == 1 and isinstance(response[0], dict):
                return self._parse_companies(response[0])
            companies = [item for item in response if isinstance(item, dict)]
            return [self._normalize_company(company) for company in companies]
        if isinstance(response, dict):
            if "companies" in response and isinstance(response["companies"], list):
                return [self._normalize_company(company) for company in response["companies"] if isinstance(company, dict)]
            if "message" in response and isinstance(response["message"], dict):
                content = response["message"].get("content")
                if isinstance(content, dict):
                    normalized: List[Dict[str, Any]] = []
                    if isinstance(content.get("companies"), list):
                        normalized.extend(
                            self._normalize_company(company)
                            for company in content["companies"]
                            if isinstance(company, dict)
                        )
                    if isinstance(content.get("results"), list):
                        normalized.extend(
                            self._normalize_company(company)
                            for company in content["results"]
                            if isinstance(company, dict)
                        )
                    if normalized:
                        return normalized
        logging.warning("Discovery webhook response not understood: %s", response)
        return []

    def _normalize_company(self, company: Dict[str, Any]) -> Dict[str, Any]:
        """Convert discovery output into the internal company schema."""
        location = company.get("location")
        location_text = ""
        if isinstance(location, dict):
            location_city = (
                location.get("headquarters_city")
                or location.get("hq_city")
                or location.get("city")
            )
            location_state = (
                location.get("state")
                or location.get("headquarters_state")
                or location.get("region")
            )
            location_region = location.get("region")
        else:
            location_city = None
            location_state = None
            location_region = None
            if isinstance(location, str):
                location_text = location.strip()

        portal_url = company.get("portal_url")
        website_provenance = company.get("website_provenance")

        domain = (
            company.get("domain")
            or company.get("website")
            or (location.get("website") if isinstance(location, dict) else "")
            or ""
        )
        if domain.startswith("http"):
            parsed = urllib.parse.urlparse(domain)
            domain = parsed.netloc or parsed.path
        if domain.startswith("www."):
            domain = domain[4:]

        if not domain and isinstance(portal_url, str):
            parsed_portal = urllib.parse.urlparse(portal_url)
            domain = (parsed_portal.netloc or parsed_portal.path or "").lower()
        if not domain and isinstance(website_provenance, str):
            parsed_prov = urllib.parse.urlparse(website_provenance)
            domain = (parsed_prov.netloc or parsed_prov.path or "").lower()

        website = company.get("website")
        if website and not website.startswith("http"):
            website = f"https://{website}"
        elif not website and domain:
            website = f"https://{domain}"
        if not website and isinstance(portal_url, str):
            website = portal_url
        if not website and isinstance(website_provenance, str):
            website = website_provenance

        service_areas: List[str] = []
        raw_service_areas = company.get("service_areas")
        if isinstance(raw_service_areas, list):
            service_areas = [item.strip() for item in raw_service_areas if isinstance(item, str) and item.strip()]

        estimated_units = None
        units_obj = company.get("estimated_units_managed")
        if isinstance(units_obj, dict):
            estimated_units = units_obj.get("estimate") or units_obj.get("range")
        elif "units" in company:
            estimated_units = company.get("units")

        estimated_employees = None
        employees_obj = company.get("estimated_employee_count")
        if isinstance(employees_obj, dict):
            estimated_employees = employees_obj.get("estimate") or employees_obj.get("range")
        elif "employees" in company:
            estimated_employees = company.get("employees")

        pms_name = ""
        identified_pms = company.get("identified_pms")
        if isinstance(identified_pms, dict):
            pms_name = identified_pms.get("name") or identified_pms.get("pms")
        elif isinstance(identified_pms, list) and identified_pms and isinstance(identified_pms[0], str):
            pms_name = identified_pms[0]

        region = (
            company.get("region")
            or location_region
            or (service_areas[0] if service_areas else None)
            or location_text
            or ""
        )

        return {
            "source": "discovery",
            "company_name": company.get("company_name") or company.get("name") or "",
            "domain": domain.lower(),
            "city": company.get("city") or location_city or location_text,
            "state": company.get("state") or location_state or "",
            "region": region,
            "pms": company.get("pms") or company.get("software") or pms_name or "",
            "unit_count": company.get("unit_count") or estimated_units,
            "employee_count": company.get("employee_count") or estimated_employees,
            "company_url": website or (f"https://{domain}" if domain else ""),
            "state_of_operations": service_areas or None,
            "missing_fields": company.get("missing_fields") if isinstance(company.get("missing_fields"), list) else None,
        }


# ---------------------------------------------------------------------------
# Enrichment client
# ---------------------------------------------------------------------------

class N8NEnrichmentClient:
    """Calls the enrichment and verification webhooks."""

    def __init__(self, config: Config):
        self.company_webhook = config.company_enrichment_webhook
        self.contact_webhook = config.contact_enrichment_webhook
        self.verification_webhook = config.email_verification_webhook
        self.max_verification_attempts = max(1, config.email_verification_attempts)
        self.verification_delay = max(0.5, config.email_verification_delay)
        self.company_timeout = max(120.0, config.company_enrichment_timeout)
        self.contact_timeout = max(120.0, config.contact_enrichment_timeout)
        self.verification_timeout = max(60.0, config.email_verification_timeout)
        self.contact_discovery_webhook = config.contact_discovery_webhook
        self.contact_discovery_timeout = max(60.0, config.contact_discovery_timeout)

    @staticmethod
    def is_pms_portal_host(host: str) -> bool:
        host = (host or "").lower()
        if not host:
            return False
        pms_hosts = (
            "appfolio.com",
            "rentcafe.com",
            "yardi.com",
            "yardibreeze.com",
            "activebuilding.com",
            "buildium.com",
            "managebuilding.com",
            "propertyware.com",
            "doorloop.com",
            "realpage.com",
            "entrata.com",
            "residentportal.com",
            "rentmanager.com",
            "rmwebaccess.com",
        )
        return any(host == d or host.endswith("." + d) for d in pms_hosts)

    @staticmethod
    def hostname_from_url(url: Optional[str]) -> str:
        if not url:
            return ""
        try:
            parsed = urllib.parse.urlparse(url)
            host = parsed.netloc or parsed.path
            return host.lower()
        except Exception:
            return ""

    def canonical_domain_for_verification(self, company: Dict[str, Any], original: Optional[Dict[str, Any]] = None) -> str:
        # Prefer verified homepage from enrichment/company fields
        candidates: List[str] = []
        for key in ("company_url", "website"):
            v = company.get(key)
            if isinstance(v, str) and v:
                candidates.append(v)
        if original:
            for key in ("company_url", "website"):
                v = original.get(key)
                if isinstance(v, str) and v:
                    candidates.append(v)
        # Add https://<domain> fallback
        dom = company.get("domain") or (original or {}).get("domain")
        if isinstance(dom, str) and dom:
            candidates.append(f"https://{dom}")

        # Choose first non-PMS host
        for url in candidates:
            host = self.hostname_from_url(url)
            if host and not self.is_pms_portal_host(host):
                return host
        # If all are PMS hosts, return the original domain string (may still be PMS)
        return (dom or "").lower()

    def discover_contacts(self, company: Dict[str, Any]) -> List[Dict[str, Any]]:
        if not self.contact_discovery_webhook:
            return []
        # Match expected n8n input contract: company_name, company_domain, company_city, company_state
        payload = {
            "company_name": company.get("company_name"),
            "company_domain": company.get("domain"),
            "company_city": company.get("hq_city") or company.get("city"),
            "company_state": company.get("hq_state") or company.get("state"),
            # Keep legacy keys for broader compatibility
            "domain": company.get("domain"),
            "website": company.get("company_url"),
            "hq_city": company.get("hq_city") or company.get("city"),
            "hq_state": company.get("hq_state") or company.get("state"),
        }
        try:
            resp = _http_request(
                "POST",
                self.contact_discovery_webhook,
                json_body=payload,
                timeout=self.contact_discovery_timeout,
            )
        except Exception as exc:  # noqa: BLE001
            logging.warning("Contact discovery webhook failed: %s", exc)
            return []

        contacts: List[Dict[str, Any]] = []
        seen: Set[Tuple[str, str, str]] = set()

        def _sanitize(value: Optional[str]) -> str:
            if not isinstance(value, str):
                return ""
            return value.strip()

        def _add(c: Dict[str, Any]):
            name = _sanitize(c.get("full_name") or c.get("name"))
            if not isinstance(name, str) or not name.strip():
                return
            title = _sanitize(c.get("title") or c.get("role") or c.get("job_title"))
            linkedin = _sanitize(c.get("linkedin") or c.get("linkedin_url"))
            email = _sanitize(c.get("email"))
            key = (name.lower(), email.lower(), linkedin.lower())
            if key in seen:
                return
            seen.add(key)
            contacts.append(
                {
                    "full_name": name,
                    "title": title or None,
                    "email": email or None,
                    "linkedin": linkedin,
                    "domain": _sanitize(c.get("domain")) or None,
                }
            )

        for raw in self._extract_contacts_from_response(resp):
            if isinstance(raw, dict):
                _add(raw)
        return contacts

    @staticmethod
    def _extract_contacts_from_response(resp: Any) -> List[Dict[str, Any]]:
        """
        Traverse arbitrary LLM/n8n response structures and pull out contact dicts.
        """
        extracted: List[Dict[str, Any]] = []

        def looks_like_contact(node: Dict[str, Any]) -> bool:
            if not isinstance(node, dict):
                return False
            if node.get("contacts") and isinstance(node.get("contacts"), list):
                # container dict, not individual contact
                return False
            keys = set(k.lower() for k in node.keys())
            return bool({"name", "full_name"} & keys) and (
                "job_title" in keys
                or "title" in keys
                or "role" in keys
                or "email" in keys
                or "linkedin" in keys
                or "linkedin_url" in keys
            )

        def walk(node: Any) -> None:
            if isinstance(node, dict):
                for key, value in node.items():
                    if key == "contacts" and isinstance(value, list):
                        for item in value:
                            if isinstance(item, dict):
                                extracted.append(item)
                        continue
                    if isinstance(value, dict) or isinstance(value, list):
                        walk(value)
            elif isinstance(node, list):
                if node and all(isinstance(item, dict) for item in node) and any(looks_like_contact(item) for item in node):
                    for item in node:
                        if isinstance(item, dict):
                            extracted.append(item)
                else:
                    for item in node:
                        if isinstance(item, (dict, list)):
                            walk(item)

        walk(resp)
        return extracted

    def enrich_company(self, company: Dict[str, Any]) -> Optional[Dict[str, Any]]:
        company_name = company.get("company_name") or company.get("name")
        domain = company.get("domain")
        website = company.get("company_url") or company.get("website")
        city = company.get("city") or company.get("hq_city")
        state = company.get("state") or company.get("hq_state")

        payload = {
            "company_name": company_name,
            "domain": domain,
            "website": website,
            "company_url": website,
            "city": city,
            "state": state,
            "hq_city": company.get("hq_city") or city,
            "hq_state": company.get("hq_state") or state,
        }

        missing_fields = company.get("missing_fields")
        if missing_fields is None:
            missing_fields = [
                "units",
                "single_family_flag",
                "property_mix",
                "agent_summary",
                "pms",
                "employees",
                "decision_makers",
            ]
        if missing_fields:
            payload["missing_fields"] = missing_fields

        payload = {key: value for key, value in payload.items() if value not in (None, "")}
        logging.info("Enriching company via n8n: %s", company.get("company_name"))
        response = _http_request("POST", self.company_webhook, json_body=payload, timeout=self.company_timeout)
        enriched = self._parse_company_response(response, company)
        if not enriched:
            return None
        if not enriched.get("decision_makers"):
            logging.warning(
                "Company lacked decision makers after enrichment: %s",
                company.get("company_name"),
            )
        return enriched

    def _parse_company_response(self, response: Any, original: Dict[str, Any]) -> Optional[Dict[str, Any]]:
        """Extract structured company fields from n8n output."""
        data: Dict[str, Any] = {}
        raw_packet: Dict[str, Any] = {}
        extra_fields: Dict[str, Any] = {}
        metadata: Dict[str, Any] = {}
        decisions: List[Dict[str, Any]] = []
        summary = ""
        missing_fields: List[str] = []
        low_confidence_fields: List[str] = []

        def _extract_from_dict(blob: Dict[str, Any]) -> None:
            nonlocal data, raw_packet, extra_fields, decisions, summary, missing_fields, low_confidence_fields, metadata
            if not isinstance(blob, dict):
                return

            if "research_packet" in blob and isinstance(blob["research_packet"], dict):
                raw_packet = blob["research_packet"]
                data.update(raw_packet)
                packet_decisions = raw_packet.get("decision_makers")
                if isinstance(packet_decisions, list) and packet_decisions:
                    decisions = [dm for dm in packet_decisions if isinstance(dm, dict)]

            if "company" in blob and isinstance(blob["company"], dict):
                extra_fields.update(blob["company"])
                data.update(blob["company"])

            if "extra_fields" in blob and isinstance(blob["extra_fields"], dict):
                extra_fields.update(blob["extra_fields"])
                data.update(blob["extra_fields"])

            if "decision_makers" in blob and isinstance(blob["decision_makers"], list):
                decisions = [dm for dm in blob["decision_makers"] if isinstance(dm, dict)]

            if "summary" in blob and isinstance(blob["summary"], str):
                summary = blob["summary"]
            if "agent_summary" in blob and isinstance(blob["agent_summary"], str) and not summary:
                summary = blob["agent_summary"]

            if "missing_fields" in blob and isinstance(blob["missing_fields"], list):
                missing_fields = blob["missing_fields"]
            if "low_confidence_fields" in blob and isinstance(blob["low_confidence_fields"], list):
                low_confidence_fields = blob["low_confidence_fields"]
            if "metadata" in blob and isinstance(blob["metadata"], dict):
                metadata.update(blob["metadata"])

        if isinstance(response, dict):
            if "data" in response and isinstance(response["data"], list):
                for item in response["data"]:
                    if isinstance(item, dict):
                        message = item.get("message", {})
                        content = message.get("content")
                        if isinstance(content, dict):
                            _extract_from_dict(content)
                        elif isinstance(content, str) and not summary:
                            summary = content
                    else:
                        _extract_from_dict(item)
            else:
                _extract_from_dict(response)
        elif isinstance(response, list) and response:
            return self._parse_company_response(response[0], original)

        def _coerce_int(value: Any) -> Any:
            try:
                if isinstance(value, bool):
                    return value
                if isinstance(value, (int, float)) and not isinstance(value, bool):
                    return int(value)
                if isinstance(value, str):
                    stripped = value.replace(",", "").strip()
                    if stripped.isdigit():
                        return int(stripped)
                    return int(float(stripped))
            except (ValueError, TypeError):
                return value
            return value

        summary = summary or data.get("notes") or extra_fields.get("notes") or ""

        company_name = (
            data.get("company")
            or data.get("company_name")
            or extra_fields.get("company_name")
            or original.get("company_name")
        )
        domain = data.get("domain") or extra_fields.get("domain") or original.get("domain")
        website = data.get("website") or data.get("company_url") or extra_fields.get("website") or original.get("company_url")
        hq_city = data.get("hq_city") or extra_fields.get("hq_city") or original.get("hq_city") or original.get("city")
        hq_state = data.get("hq_state") or extra_fields.get("hq_state") or original.get("hq_state") or original.get("state")
        units = _coerce_int(data.get("units_estimate") or data.get("unit_count") or extra_fields.get("unit_count"))
        employee_count = _coerce_int(data.get("employees_estimate") or data.get("employee_count"))

        company = {
            "company_name": company_name,
            "domain": domain,
            "company_url": website,
            "hq_city": hq_city,
            "hq_state": hq_state,
            "city": extra_fields.get("city") or original.get("city"),
            "state": extra_fields.get("state") or original.get("state"),
            "pms": data.get("pms_vendor") or data.get("pms") or extra_fields.get("pms") or original.get("pms"),
            "unit_count": units,
            "employee_count": employee_count,
            "narpm_member": data.get("narpm_member"),
            "single_family": data.get("single_family"),
            "icp_fit": data.get("icp_fit"),
            "icp_confidence": data.get("icp_confidence"),
            "icp_tier": data.get("icp_tier"),
            "disqualifiers": data.get("disqualifiers") or [],
            "reasons_for_confidence": data.get("reasons_for_confidence") or [],
            "assumptions": data.get("assumptions") or [],
            "sources": data.get("sources") or [],
            "agent_summary": summary.strip(),
            "research_packet": raw_packet or data or {},
            "extra_fields": extra_fields or {},
            "metadata": metadata or {},
            "missing_fields": missing_fields,
            "low_confidence_fields": low_confidence_fields,
        }

        # Normalize decision maker structure
        cleaned_decisions: List[Dict[str, Any]] = []
        for entry in decisions:
            if not isinstance(entry, dict):
                continue
            name = entry.get("full_name") or entry.get("name")
            if not name:
                continue
            cleaned_decisions.append(
                {
                    "full_name": name,
                    "title": entry.get("title") or entry.get("role"),
                    "email": entry.get("email"),
                    "phone": entry.get("phone"),
                    "linkedin": entry.get("linkedin"),
                    "personalization": entry.get("personalization"),
                    "source": entry.get("source"),
                }
            )

        company["decision_makers"] = cleaned_decisions
        if not company["extra_fields"]:
            company.pop("extra_fields")
        if not company["metadata"]:
            company.pop("metadata")
        if not company["missing_fields"]:
            company.pop("missing_fields")
        if not company["low_confidence_fields"]:
            company.pop("low_confidence_fields")
        return company

    def verify_contact(
        self,
        *,
        full_name: str,
        company_name: str,
        domain: str,
    ) -> Optional[Dict[str, Any]]:
        """Verify a contact email via webhook, retrying until success or attempts exhausted."""
        if not self.verification_webhook:
            logging.warning("Verification webhook not configured; skipping email verification")
            return None

        payload = {
            "full_name": full_name,
            "company_name": company_name,
            "domain": domain,
        }

        for attempt in range(1, self.max_verification_attempts + 1):
            logging.info("Verifying email for %s (attempt %d)", full_name, attempt)
            response = _http_request("POST", self.verification_webhook, json_body=payload, timeout=self.verification_timeout)
            parsed = self._parse_verification_response(response)
            if parsed and parsed.get("verified") and parsed.get("email"):
                logging.info("Email verified for %s: %s", full_name, parsed["email"])
                return parsed
            logging.warning("Email not verified for %s (attempt %d)", full_name, attempt)
            time.sleep(self.verification_delay)
        logging.error("Failed to verify email for %s after %d attempts", full_name, self.max_verification_attempts)
        return None

    @staticmethod
    def _parse_verification_response(response: Any) -> Optional[Dict[str, Any]]:
        if isinstance(response, dict):
            email = response.get("email") or response.get("verified_email")
            verified = response.get("verified")
            if verified is None:
                validations = response.get("validations") or {}
                verified = bool(validations.get("mailbox_exists") and validations.get("syntax"))
            return {"email": email, "verified": bool(verified), "raw": response}
        if isinstance(response, list) and response:
            return N8NEnrichmentClient._parse_verification_response(response[0])
        return None

    def enrich_contact(self, contact: Dict[str, Any], company: Dict[str, Any]) -> Optional[Dict[str, Any]]:
        if not self.contact_webhook:
            logging.warning("Contact enrichment webhook not configured; skipping contact enrichment")
            return None

        payload = {
            "contact": {
                "full_name": contact.get("full_name"),
                "title": contact.get("title"),
                "email": contact.get("email"),
                "linkedin": contact.get("linkedin"),
                "seed_urls": contact.get("seed_urls", []),
            },
            "company": {
                "name": company.get("company_name"),
                "domain": company.get("domain"),
                "website": company.get("company_url"),
                "city": company.get("hq_city") or company.get("city"),
                "state": company.get("hq_state") or company.get("state"),
            },
        }
        payload["contact"] = {k: v for k, v in payload["contact"].items() if v not in (None, "", [])}
        payload["company"] = {k: v for k, v in payload["company"].items() if v not in (None, "", [])}
        logging.info("Enriching contact %s", contact.get("full_name"))
        response = _http_request("POST", self.contact_webhook, json_body=payload, timeout=self.contact_timeout)
        parsed: Optional[Dict[str, Any]] = None
        if isinstance(response, dict):
            parsed = response
        elif isinstance(response, list) and response:
            first = response[0]
            if isinstance(first, dict) and "message" in first:
                content = first["message"].get("content")
                if isinstance(content, dict):
                    parsed = content
            elif isinstance(first, dict):
                parsed = first

        if not isinstance(parsed, dict):
            logging.warning("Contact enrichment response not understood for %s: %s", contact.get("full_name"), response)
            return None

        personal = self._extract_enrichment_list(parsed, "personal")
        professional = self._extract_enrichment_list(parsed, "professional")
        seed_urls = self._extract_enrichment_list(parsed, "seed_urls")
        sources = self._extract_enrichment_list(parsed, "sources")
        summary = self._extract_enrichment_value(parsed, ["agent_summary", "summary", "output"])  # output for older flows

        normalized = {
            "full_name": contact.get("full_name"),
            "title": contact.get("title"),
            "email": contact.get("email"),
            "linkedin": contact.get("linkedin"),
            "personal_anecdotes": personal,
            "professional_anecdotes": professional,
            "seed_urls": seed_urls,
            "agent_summary": summary,
            "sources": sources,
            "raw": parsed,
        }
        return normalized

    @staticmethod
    def _extract_enrichment_list(data: Any, key: str) -> List[str]:
        result: List[str] = []

        def walk(node: Any) -> Optional[List[str]]:
            if isinstance(node, dict):
                if key in node and isinstance(node[key], list):
                    values: List[str] = []
                    for item in node[key]:
                        if isinstance(item, str):
                            stripped = item.strip()
                            if stripped:
                                values.append(stripped)
                        elif isinstance(item, dict):
                            for candidate in ("value", "text", "note", "url", "source"):
                                val = item.get(candidate)
                                if isinstance(val, str) and val.strip():
                                    values.append(val.strip())
                                    break
                    if values:
                        return values
                for value in node.values():
                    found = walk(value)
                    if found:
                        return found
            elif isinstance(node, list):
                for item in node:
                    found = walk(item)
                    if found:
                        return found
            return None

        extracted = walk(data)
        if extracted:
            result = extracted
        return result

    @staticmethod
    def _extract_enrichment_value(data: Any, keys: List[str]) -> Optional[str]:
        def walk(node: Any) -> Optional[str]:
            if isinstance(node, dict):
                for key in keys:
                    value = node.get(key)
                    if isinstance(value, str) and value.strip():
                        return value.strip()
                for value in node.values():
                    found = walk(value)
                    if found:
                        return found
            elif isinstance(node, list):
                for item in node:
                    found = walk(item)
                    if found:
                        return found
            return None

        return walk(data)


# ---------------------------------------------------------------------------
# Orchestrator
# ---------------------------------------------------------------------------

class LeadOrchestrator:
    """Coordinates the entire discovery and enrichment pipeline with production resilience."""

    def __init__(self, config: Config):
        config.validate()
        self.config = config

        # Core clients
        self.supabase = SupabaseResearchClient(config)
        self.hubspot = HubSpotClient(config)
        self.discovery = DiscoveryWebhookClient(config.discovery_webhook_url, config.discovery_request_timeout)
        self.enrichment = N8NEnrichmentClient(config)

        # Runtime state
        self.discovery_cache = {}
        self.companies: List[Dict[str, Any]] = []
        self.current_target_quantity = 0

        # Production features
        self.deduplicator = ContactDeduplicator()
        self.state_manager: Optional[StateManager] = None
        self.last_run_id: Optional[str] = None
        self.last_run_dir: Optional[Path] = None

        # Metrics
        self.metrics = {
            "start_time": time.time(),
            "companies_discovered": 0,
            "companies_suppressed": 0,
            "companies_location_filtered": 0,
            "companies_pm_filtered": 0,
            "companies_deduped": 0,
            "companies_enriched": 0,
            "companies_rejected": 0,
            "contacts_discovered": 0,
            "contacts_verified": 0,
            "contacts_enriched": 0,
            "contacts_rejected": 0,
            "contacts_salvaged": 0,
            "contacts_anecdote_rejections": 0,
            "api_calls": {},
            "errors": [],
        }

        # Location request context (set during run)
        self.requested_city: Optional[str] = None
        self.requested_state: Optional[str] = None
        self.requested_location_text: Optional[str] = None

        # Circuit breakers for external services
        self.circuit_breakers = {}
        if config.circuit_breaker_enabled:
            self.circuit_breakers = {
                "supabase": CircuitBreaker("supabase", config.circuit_breaker_threshold, config.circuit_breaker_timeout),
                "hubspot": CircuitBreaker("hubspot", config.circuit_breaker_threshold, config.circuit_breaker_timeout),
                "discovery": CircuitBreaker("discovery", config.circuit_breaker_threshold, config.circuit_breaker_timeout),
                "enrichment": CircuitBreaker("enrichment", config.circuit_breaker_threshold, config.circuit_breaker_timeout),
                "verification": CircuitBreaker("verification", config.circuit_breaker_threshold, config.circuit_breaker_timeout),
            }

        # Initialize nano QA validator lazily
        self._qa_validator = None

    def _qa(self):
        """Lazy-initialize and return the nano validator if enabled."""
        if not self.config.qa_validator_enabled:
            return None
        if self._qa_validator is not None:
            return self._qa_validator
        try:
            self._qa_validator = _NanoValidator(self.config)
        except Exception as exc:
            logging.debug("QA validator init failed: %s", exc)
            self._qa_validator = None
        return self._qa_validator

    def _should_exclude_company_pms(self, company: Dict[str, Any]) -> bool:
        """Check if company should be excluded based on PMS (RentVine always excluded)."""
        pms = str(company.get('pms', '')).lower().strip()

        # RentVine and its variants (they're our client!)
        rentvine_indicators = ['rentvine', 'rent vine', 'other']

        for indicator in rentvine_indicators:
            if indicator in pms:
                logging.debug("Excluding RentVine company: %s", company.get('company_name'))
                return True

        return False

    def _calculate_buffer_target(self, requested: int) -> Tuple[int, float]:
        """
        Determine how many companies to queue up front based on requested quantity.

        Smaller requests need a larger multiplier (3-4x) while larger batches would
        explode if we kept that ratio. This sliding scale keeps enough headroom for
        attrition without overwhelming discovery.
        """
        requested = max(1, requested)
        thresholds: List[Tuple[int, float]] = [
            (3, 4.0),
            (10, 3.0),
            (25, 2.3),
            (50, 1.9),
            (80, 1.6),
            (120, 1.5),
        ]
        multiplier = 1.4
        for limit, candidate in thresholds:
            if requested <= limit:
                multiplier = candidate
                break

        buffer_target = max(requested + 1, math.ceil(requested * multiplier))
        buffer_target = min(buffer_target, self.config.max_companies_per_run)
        return buffer_target, multiplier
    
    def _track_api_call(self, service: str, success: bool = True) -> None:
        """Track API call metrics."""
        if service not in self.metrics["api_calls"]:
            self.metrics["api_calls"][service] = {"success": 0, "failure": 0}
        if success:
            self.metrics["api_calls"][service]["success"] += 1
        else:
            self.metrics["api_calls"][service]["failure"] += 1
    
    def _track_error(self, error: str, context: Dict[str, Any]) -> None:
        """Track error with context."""
        self.metrics["errors"].append({
            "timestamp": datetime.now(timezone.utc).isoformat(),
            "error": error,
            "context": context,
        })
        logging.error("Error: %s | Context: %s", error, json.dumps(context))

    def _final_quality_gate(
        self,
        deliverable: List[Dict[str, Any]],
        target_quantity: int,
        args: argparse.Namespace,
        run_dir: Path,
    ) -> List[Dict[str, Any]]:
        """
        Final quality gate before CSV generation:
        1. Remove any duplicate domains
        2. Remove any companies with icp_fit=no
        3. Backfill from discovery if removals occurred

        This is the last line of defense to ensure quality.
        """
        logging.info("=" * 70)
        logging.info("FINAL QUALITY GATE: Validating deliverable before CSV export")
        logging.info("=" * 70)

        initial_count = len(deliverable)
        rejected: List[Dict[str, Any]] = []

        # Step 1: Enforce strict deduplication by domain
        seen_domains: Set[str] = set()
        deduped: List[Dict[str, Any]] = []
        duplicate_count = 0

        for item in deliverable:
            company = item.get("company") if isinstance(item, dict) else None
            if not isinstance(company, dict):
                rejected.append(item)
                logging.warning("Quality gate: rejecting item without valid company structure")
                continue

            domain = self._domain_key(company)
            if not domain:
                rejected.append(item)
                logging.warning("Quality gate: rejecting company without domain: %s",
                              company.get("company_name") or "unknown")
                continue

            if domain in seen_domains:
                duplicate_count += 1
                rejected.append(item)
                logging.warning("Quality gate: removing duplicate domain: %s", domain)
                continue

            seen_domains.add(domain)
            deduped.append(item)

        # Step 2: Enforce icp_fit != no
        icp_no_count = 0
        final_clean: List[Dict[str, Any]] = []

        for item in deduped:
            company = item.get("company", {})
            icp_fit = str(company.get("icp_fit", "")).strip().lower()
            single_family = str(company.get("single_family", "")).strip().lower()

            # Reject if icp_fit explicitly says "no"
            if icp_fit.startswith("no"):
                icp_no_count += 1
                rejected.append(item)
                logging.warning(
                    "Quality gate: removing company with icp_fit=no: %s (icp_fit=%s)",
                    company.get("company_name") or company.get("domain") or "unknown",
                    icp_fit
                )
                continue

            # Also reject if single_family explicitly says "no" (conservative)
            if single_family.startswith("no"):
                icp_no_count += 1
                rejected.append(item)
                logging.warning(
                    "Quality gate: removing company with single_family=no: %s",
                    company.get("company_name") or company.get("domain") or "unknown"
                )
                continue

            final_clean.append(item)

        removed_count = initial_count - len(final_clean)

        if removed_count > 0:
            logging.warning(
                "Quality gate REJECTED %d/%d companies: %d duplicates, %d icp_fit=no",
                removed_count, initial_count, duplicate_count, icp_no_count
            )

            # Save rejected companies for audit
            try:
                rejected_file = run_dir / "quality_gate_rejected.json"
                rejected_file.write_text(json.dumps(rejected, indent=2))
                logging.info("Rejected companies saved to: %s", rejected_file)
            except Exception as exc:
                logging.warning("Could not save rejected companies: %s", exc)

            # Backfill if we're short
            shortfall = target_quantity - len(final_clean)
            if shortfall > 0:
                logging.info("=" * 70)
                logging.info("BACKFILL: Quality gate removed %d companies, need %d more to meet target",
                           removed_count, shortfall)
                logging.info("=" * 70)

                # Trigger discovery to backfill
                final_clean = self._topup_results(final_clean, shortfall, args, run_dir)

                # Re-apply all filters to backfilled results
                final_clean = self._filter_enriched_results_by_location(final_clean)
                final_clean = self._filter_enriched_results_by_property_type(final_clean)

                # CRITICAL: Run quality gate again on backfilled results (recursive, but with base case)
                if len(final_clean) < len(deliverable):
                    logging.warning("Backfill did not fully recover; delivering %d/%d",
                                  len(final_clean), target_quantity)
        else:
            logging.info("Quality gate: ✓ All %d companies passed validation", initial_count)

        logging.info("=" * 70)
        return final_clean

    def _log_phase_summary(self, delivered: int, requested: int) -> None:
        """Log comprehensive funnel metrics showing the full pipeline flow."""
        logging.info("=" * 70)
        logging.info("PIPELINE PHASE SUMMARY")
        logging.info("=" * 70)

        # Discovery funnel
        discovered = self.metrics["companies_discovered"]
        suppressed = self.metrics["companies_suppressed"]
        location_filtered = self.metrics["companies_location_filtered"]
        deduped = self.metrics["companies_deduped"]
        enriched = self.metrics["companies_enriched"]
        rejected = self.metrics["companies_rejected"]

        logging.info("Company Funnel:")
        logging.info("  Discovered:         %5d", discovered)
        if suppressed > 0:
            logging.info("  - Suppressed (HubSpot): -%4d → %5d remaining", suppressed, discovered - suppressed)
        if deduped > 0:
            after_dedupe = discovered - suppressed - deduped
            logging.info("  - Deduped (domain):     -%4d → %5d unique", deduped, max(0, after_dedupe))
        if location_filtered > 0:
            logging.info("  - Location filtered:    -%4d", location_filtered)
        logging.info("  Enriched successfully:  %5d", enriched)
        if rejected > 0:
            logging.info("  Rejected (no DMs/fail): %5d", rejected)

        # Contact funnel
        contacts_found = self.metrics["contacts_discovered"]
        contacts_verified = self.metrics["contacts_verified"]
        contacts_enriched = self.metrics["contacts_enriched"]
        contacts_rejected = self.metrics["contacts_rejected"]
        anecdote_rejects = self.metrics["contacts_anecdote_rejections"]

        logging.info("")
        logging.info("Contact Funnel:")
        logging.info("  Discovered:         %5d", contacts_found)
        logging.info("  Verified emails:    %5d (%.1f%%)",
                     contacts_verified,
                     100 * contacts_verified / max(1, contacts_found))
        if anecdote_rejects > 0:
            logging.info("  - Anecdote rejects:     -%4d", anecdote_rejects)
        logging.info("  Enriched w/ anecdotes: %5d", contacts_enriched)
        if contacts_rejected > 0:
            logging.info("  Rejected (other):      %5d", contacts_rejected)

        # Final tally
        logging.info("")
        logging.info("Final Delivery:")
        logging.info("  Requested:          %5d", requested)
        logging.info("  Delivered:          %5d", delivered)
        if delivered >= requested:
            logging.info("  Status: ✓ Target met")
        else:
            shortfall = requested - delivered
            logging.info("  Status: ⚠ Short by %d (%.1f%% of target)", shortfall, 100 * shortfall / requested)

        logging.info("=" * 70)

    def _checkpoint_if_needed(self, state: Dict[str, Any]) -> None:
        """Save checkpoint if interval elapsed."""
        if self.state_manager and self.state_manager.should_checkpoint():
            state["metrics"] = self.metrics
            self.state_manager.save_checkpoint(state)

    def _normalize_request_location(self, args: argparse.Namespace) -> None:
        """Derive consistent location filters from the incoming request arguments."""
        location_raw = (args.location or "").strip() or None
        city = (args.city or "").strip() or None
        state = (args.state or "").strip() or None

        derived_city: Optional[str] = None
        derived_state: Optional[str] = None
        if location_raw:
            derived_city, derived_state = parse_location_to_city_state(location_raw)

        updates: List[str] = []
        if not city and derived_city:
            args.city = derived_city
            city = derived_city
            updates.append(f"city='{derived_city}'")
        if not state and derived_state:
            args.state = derived_state
            state = derived_state
            updates.append(f"state='{derived_state}'")
        if updates:
            logging.info(
                "Derived location filters from location string '%s': %s",
                location_raw,
                ", ".join(updates),
            )

        self.requested_location_text = location_raw
        self.requested_city = city
        self.requested_state = state

    def _has_location_filter(self) -> bool:
        return any(
            value
            for value in (self.requested_city, self.requested_state, self.requested_location_text)
        )

    def _filter_companies_by_location(self, companies: List[Dict[str, Any]]) -> List[Dict[str, Any]]:
        if not companies or not self._has_location_filter():
            return companies
        matched: List[Dict[str, Any]] = []
        for company in companies:
            if company_matches_location(
                company,
                city=self.requested_city,
                state=self.requested_state,
                location_text=self.requested_location_text,
            ):
                matched.append(company)
            else:
                logging.debug(
                    "Filtering out company '%s' due to location mismatch (requested city=%s, state=%s)",
                    company.get("company_name") or company.get("name") or company.get("domain") or "unknown",
                    self.requested_city or "",
                    self.requested_state or "",
                )
        if len(matched) != len(companies):
            filtered_count = len(companies) - len(matched)
            self.metrics["companies_location_filtered"] += filtered_count
            logging.info(
                "Location filter removed %d/%d companies",
                filtered_count,
                len(companies),
        )
        return matched

    def _filter_enriched_results_by_location(
        self,
        items: List[Dict[str, Any]],
    ) -> List[Dict[str, Any]]:
        if not items or not self._has_location_filter():
            return items
        matched: List[Dict[str, Any]] = []
        for item in items:
            company = item.get("company") if isinstance(item, dict) else None
            if not isinstance(company, dict):
                continue
            if company_matches_location(
                company,
                city=self.requested_city,
                state=self.requested_state,
                location_text=self.requested_location_text,
            ):
                matched.append(item)
            else:
                logging.info(
                    "Dropping enriched company '%s' due to location mismatch",
                    company.get("company_name") or company.get("domain") or "unknown",
                )
        if len(matched) != len(items):
            logging.info(
                "Location filter removed %d/%d enriched companies",
                len(items) - len(matched),
                len(items),
            )
        return matched

    def _filter_companies_by_property_type(
        self,
        companies: List[Dict[str, Any]],
        *,
        strict: bool = False,
    ) -> List[Dict[str, Any]]:
        if not companies:
            return []
        flagged: List[str] = []
        for company in companies:
            allowed, reason = evaluate_property_management_status(company, strict=strict)
            if not allowed:
                company_name = (
                    company.get("company_name")
                    or company.get("name")
                    or company.get("domain")
                    or "unknown"
                )
                flagged.append(f"{company_name} ({reason or 'unspecified'})")

        if flagged:
            preview = ", ".join(flagged[:5])
            if len(flagged) > 5:
                preview += f", … +{len(flagged) - 5} more"
            logging.info(
                "Property-type gate disabled; allowing %d companies previously flagged as non-property-management: %s",
                len(flagged),
                preview,
            )
        return companies

    def _filter_enriched_results_by_property_type(
        self,
        items: List[Dict[str, Any]],
    ) -> List[Dict[str, Any]]:
        if not items:
            return []
        flagged: List[str] = []
        for item in items:
            company = item.get("company") if isinstance(item, dict) else None
            if not isinstance(company, dict):
                continue  # Skip invalid records entirely
            allowed, reason = evaluate_property_management_status(company, strict=True)
            if not allowed:
                company_name = (
                    company.get("company_name")
                    or company.get("name")
                    or company.get("domain")
                    or "unknown"
                )
                flagged.append(f"{company_name} ({reason or 'unspecified'})")

        if flagged:
            preview = ", ".join(flagged[:5])
            if len(flagged) > 5:
                preview += f", … +{len(flagged) - 5} more"
            logging.info(
                "Property-type gate disabled during enrichment; allowing %d companies previously flagged as non-property-management: %s",
                len(flagged),
                preview,
            )
        return items

    def run(self, args: argparse.Namespace) -> Dict[str, Any]:
        """Execute pipeline with production resilience and recovery."""
        run_id = datetime.now(timezone.utc).strftime("%Y%m%d_%H%M%S") + "_" + uuid.uuid4().hex[:6]
        self.last_run_id = run_id
        run_dir = Path.cwd() / "runs" / run_id
        self.last_run_dir = run_dir
        run_dir.mkdir(parents=True, exist_ok=True)
        
        # Initialize state management
        self.state_manager = StateManager(run_dir)

        # Initialize tracking for ALL attempted companies to prevent infinite loops
        self.all_attempted_domains = set()
        
        # Setup logging
        fh = logging.FileHandler(str(run_dir / "run.log"))
        fh.setLevel(logging.INFO)
        fh.setFormatter(logging.Formatter("%(asctime)s | %(levelname)s | %(message)s"))
        logging.getLogger().addHandler(fh)
        
        # Health check
        logging.info("Running pre-flight health checks...")
        health_check = HealthCheck(self.config)
        healthy, errors = health_check.check_all()
        if not healthy:
            logging.error("Health check failed: %s", "; ".join(errors))
            if not os.getenv("SKIP_HEALTH_CHECK"):
                return {
                    "error": "Health check failed",
                    "details": errors,
                    "run_id": run_id,
                }
        
        logging.info("Starting pipeline run %s", run_id)
        logging.info("Configuration: discovery_timeout=%.0fs, enrichment_concurrency=%d, contact_concurrency=%d",
                     self.config.discovery_request_timeout,
                     self.config.enrichment_concurrency,
                     self.config.contact_concurrency)
        logging.info(
            "Contact quality thresholds: personal>=%d, professional>=%d, total>=%d, personalization_fallback=%s, seed_url_fallback=%s",
            self.config.contact_min_personal_anecdotes,
            self.config.contact_min_professional_anecdotes,
            self.config.contact_min_total_anecdotes,
            "on" if self.config.contact_allow_personalization_fallback else "off",
            "on" if self.config.contact_allow_seed_url_fallback else "off",
        )

        self._normalize_request_location(args)
        logging.info(
            "Location filters applied: state=%s, city=%s, location_text=%s",
            self.requested_state or args.state or "N/A",
            self.requested_city or args.city or "N/A",
            self.requested_location_text or "N/A",
        )
        
        # Persist input
        input_record = {
            "run_id": run_id,
            "args": {
                "state": args.state,
                "city": args.city,
                "location": args.location,
                "pms": args.pms,
                "quantity": args.quantity,
                "unit_min": args.unit_min,
                "unit_max": args.unit_max,
            },
            "env": {
                "discovery_webhook": self.config.discovery_webhook_url,
                "company_enrichment": self.config.company_enrichment_webhook,
                "contact_discovery": self.config.contact_discovery_webhook,
            },
            "timestamp": datetime.now(timezone.utc).isoformat(),
        }
        (run_dir / "input.json").write_text(json.dumps(input_record, indent=2))
        
        # Validate quantity
        target_quantity = max(1, min(args.quantity, self.config.max_companies_per_run))
        if args.quantity > self.config.max_companies_per_run:
            logging.warning(
                "Requested quantity %d exceeds max %d, capping at %d",
                args.quantity,
                self.config.max_companies_per_run,
                target_quantity,
            )
        
        buffer_quantity, buffer_multiplier = self._calculate_buffer_target(target_quantity)
        max_rounds = max(1, args.max_rounds or self.config.discovery_max_rounds)

        logging.info(
            "Target: %d companies (buffer: %d, multiplier: %.2fx, max_rounds: %d)",
            target_quantity,
            buffer_quantity,
            buffer_multiplier,
            max_rounds,
        )
        self.current_target_quantity = target_quantity
        
        try:
            # Phase 1: Load from Supabase
            logging.info("=== Phase 1: Loading candidates from Supabase ===")
            companies = self._load_supabase_candidates(args, buffer_quantity)
            logging.info("Supabase pool: %d companies", len(companies))
            self.metrics["companies_discovered"] = len(companies)

            # QA: quick sanity on Supabase pool (log-only)
            qa = self._qa()
            if qa:
                try:
                    qa.log_decision(
                        phase="supabase_pool",
                        items=companies[: self.config.qa_sample_size],
                        context={"state": args.state, "city": args.city, "location": args.location},
                    )
                except Exception:
                    pass
            
            # Checkpoint after Supabase
            self._checkpoint_if_needed({
                "phase": "supabase_loaded",
                "companies_count": len(companies),
                "companies": companies[:10],  # Sample
            })
            
            # Phase 2: Discovery rounds
            logging.info("=== Phase 2: Discovery rounds (need %d more) ===", max(0, buffer_quantity - len(companies)))

            # Prepare optional chunk plan for discovery-only splitting (no Supabase interaction)
            chunk_plan = []
            remaining_after_supabase = max(0, buffer_quantity - len(companies))
            if args.quantity > 10 and remaining_after_supabase > 0:
                try:
                    from request_splitter import LLMRequestSplitter  # type: ignore
                    splitter = LLMRequestSplitter(llm_provider="openai")
                    base_params = {"quantity": args.quantity}
                    chunk_plan = splitter.split_request(0, base_params) or []
                    if chunk_plan:
                        logging.info("Using discovery-only chunk plan with %d chunks", len(chunk_plan))
                except Exception as exc:  # noqa: BLE001
                    logging.debug("Discovery split unavailable: %s", exc)

            attempt = 0
            # Adaptive parallel limit starting point
            adaptive_limit = max(1, min(self.config.discovery_parallel_chunks, 3))
            while len(companies) < buffer_quantity and attempt < max_rounds:
                attempt += 1
                remaining = buffer_quantity - len(companies)
                
                try:
                    suppression_domains = {self._domain_key(c) for c in companies}
                    suppression_domains.update(args.exclude or [])

                    discovered_round: List[Dict[str, Any]] = []
                    if chunk_plan:
                        # Parallelize chunked discovery (adaptive, bounded)
                        per_chunk = max(1, min(self.config.discovery_chunk_size, remaining))
                        from concurrent.futures import ThreadPoolExecutor, as_completed
                        import time as _time

                        def _discover_for_suffix(req_suffix: Optional[str], qty: int) -> Dict[str, Any]:
                            merged_requirements = args.requirements
                            if req_suffix:
                                merged_requirements = (merged_requirements + "\n" if merged_requirements else "") + str(req_suffix)
                            if self.discovery is None:
                                logging.warning("Discovery client not initialized, skipping parallel discovery")
                                return {"items": [], "duration": 0.0, "error": True}
                            start_ts = _time.time()
                            had_error = False
                            items: List[Dict[str, Any]] = []
                            for sub_try in range(max(1, self.config.discovery_chunk_max_retries)):
                                try:
                                    if self.circuit_breakers.get("discovery"):
                                        items = self.circuit_breakers["discovery"].call(
                                            self.discovery.discover,
                                            location=args.location,
                                            state=args.state,
                                            pms=args.pms,
                                            quantity=qty,
                                            unit_count_min=args.unit_min,
                                            unit_count_max=args.unit_max,
                                            suppression_domains=suppression_domains,
                                            extra_requirements=merged_requirements,
                                            attempt=attempt,
                                            override_timeout=self.config.discovery_chunk_timeout,
                                        ) or []
                                    else:
                                        items = self.discovery.discover(
                                            location=args.location,
                                            state=args.state,
                                            pms=args.pms,
                                            quantity=qty,
                                            unit_count_min=args.unit_min,
                                            unit_count_max=args.unit_max,
                                            suppression_domains=suppression_domains,
                                            extra_requirements=merged_requirements,
                                            attempt=attempt,
                                            override_timeout=self.config.discovery_chunk_timeout,
                                        ) or []
                                    if items:
                                        break
                                except Exception as _exc:
                                    had_error = True
                                    _time.sleep(min(1.0, 0.3 * (sub_try + 1)))
                            duration = max(0.0, _time.time() - start_ts)
                            return {"items": items, "duration": duration, "error": had_error or not items}

                        # Launch in waves up to max_workers
                        max_workers = max(1, min(self.config.discovery_parallel_chunks, adaptive_limit))
                        # Build list of requirement suffixes from chunk plan
                        suffixes: List[Optional[str]] = []
                        for ch in chunk_plan:
                            try:
                                suffixes.append(getattr(ch, "parameters", {}).get("requirements_suffix") if hasattr(ch, "parameters") else None)
                            except Exception:
                                suffixes.append(None)

                        next_index = 0
                        with ThreadPoolExecutor(max_workers=min(max_workers, max(1, len(suffixes)))) as ex:
                            futures = []
                            # Prime initial batch
                            while next_index < len(suffixes) and len(futures) < max_workers:
                                qty = min(per_chunk, remaining - len(discovered_round))
                                if qty <= 0:
                                    break
                                # Simple RPM limiter if configured
                                if self.config.discovery_rate_limit_rpm > 0:
                                    pass  # Placeholder for future token bucket
                                futures.append(ex.submit(_discover_for_suffix, suffixes[next_index], qty))
                                next_index += 1
                            # Consume and keep pipeline filled up to max_workers
                            wave_durations: List[float] = []
                            wave_errors = 0
                            for fut in as_completed(futures):
                                try:
                                    payload = fut.result()
                                    part = payload.get("items") or []
                                    wave_durations.append(float(payload.get("duration") or 0.0))
                                    if payload.get("error"):
                                        wave_errors += 1
                                except Exception as exc:
                                    logging.warning("Parallel discovery chunk failed: %s", exc)
                                    part = []
                                    wave_errors += 1
                                if part:
                                    discovered_round.extend(part)
                                if len(discovered_round) >= remaining:
                                    break
                                # Top up with next chunk if available
                                if next_index < len(suffixes):
                                    qty = min(per_chunk, remaining - len(discovered_round))
                                    if qty > 0:
                                        futures.append(ex.submit(_discover_for_suffix, suffixes[next_index], qty))
                                        next_index += 1
                            # Adapt parallelism for next attempt based on duration/errors
                            if wave_durations:
                                # Use average as a simple proxy
                                avg_dur = sum(wave_durations) / max(1, len(wave_durations))
                                if wave_errors > 0 or avg_dur > 0.75 * self.config.discovery_chunk_timeout:
                                    adaptive_limit = max(1, adaptive_limit - 1)
                                elif avg_dur < 0.5 * self.config.discovery_chunk_timeout and wave_errors == 0:
                                    adaptive_limit = min(self.config.discovery_parallel_chunks, adaptive_limit + 1)
                    else:
                        if self.circuit_breakers.get("discovery"):
                            discovered_round = self.circuit_breakers["discovery"].call(
                                self.discovery.discover,
                                location=args.location,
                                state=args.state,
                                pms=args.pms,
                                quantity=remaining,
                                unit_count_min=args.unit_min,
                                unit_count_max=args.unit_max,
                                suppression_domains=suppression_domains,
                                extra_requirements=args.requirements,
                                attempt=attempt,
                            )
                        else:
                            # Skip discovery if not initialized
                            if self.discovery is None:
                                logging.warning("Discovery client not initialized, skipping discovery round %d", attempt)
                                discovered_round = []
                            else:
                                discovered_round = self.discovery.discover(
                                    location=args.location,
                                    state=args.state,
                                    pms=args.pms,
                                    quantity=remaining,
                                    unit_count_min=args.unit_min,
                                    unit_count_max=args.unit_max,
                                    suppression_domains=suppression_domains,
                                    extra_requirements=args.requirements,
                                    attempt=attempt,
                                )
                    
                    # QA: validate discovery batch; allow one remediation pass per round
                    qa = self._qa()
                    if qa and qa.should_retry("discovery", attempt):
                        try:
                            decision = qa.decide(
                                phase="discovery",
                                items=discovered_round[: self.config.qa_sample_size],
                                context={
                                    "state": args.state,
                                    "city": args.city,
                                    "location": args.location,
                                    "requirements": args.requirements,
                                },
                            )
                            if decision.get("decision") == "RETRY":
                                fix = (decision.get("fix_hint") or "").strip()
                                if fix:
                                    logging.info("QA requested discovery retry with hint: %s", fix)
                                    extra_req = (args.requirements + "\n" if args.requirements else "") + fix
                                    qty2 = max(1, min(10, remaining - len(discovered_round)))
                                    more = self.discovery.discover(
                                        location=args.location,
                                        state=args.state,
                                        pms=args.pms,
                                        quantity=qty2,
                                        unit_count_min=args.unit_min,
                                        unit_count_max=args.unit_max,
                                        suppression_domains=suppression_domains,
                                        extra_requirements=extra_req,
                                        attempt=attempt,
                                    ) or []
                                    discovered_round.extend(more)
                        except Exception as exc:
                            logging.debug("QA discovery check failed: %s", exc)

                    self._track_api_call("discovery", success=True)
                    logging.info("Discovery round %d: %d companies found", attempt, len(discovered_round))
                    self.metrics["companies_discovered"] += len(discovered_round)
                    
                    filtered = self._apply_suppression(discovered_round)
                    logging.info("Discovery round %d: %d companies after suppression", attempt, len(filtered))
                    filtered = self._filter_companies_by_location(filtered)
                    logging.info("Discovery round %d: %d companies after location filter", attempt, len(filtered))
                    # Property-type gate disabled; rely on downstream AI classification instead
                    filtered = self._filter_companies_by_property_type(filtered, strict=True)
                    logging.info("Discovery round %d: %d companies after property filter", attempt, len(filtered))
                    companies = self._merge_companies(companies, filtered)
                    
                    # Checkpoint after each discovery round
                    self._checkpoint_if_needed({
                        "phase": f"discovery_round_{attempt}",
                        "companies_count": len(companies),
                        "attempt": attempt,
                    })
                    
                    if attempt < max_rounds and len(companies) < buffer_quantity:
                        time.sleep(self.config.discovery_round_delay)
                        
                except Exception as exc:
                    self._track_api_call("discovery", success=False)
                    self._track_error(f"Discovery round {attempt} failed", {"error": str(exc), "traceback": traceback.format_exc()})
                    if attempt >= 3:  # Fail fast after 3 failed attempts
                        logging.error("Discovery failing consistently, stopping discovery phase")
                        break
                    time.sleep(min(30, attempt * 5))  # Progressive backoff

            logging.info("Total companies after discovery: %d", len(companies))
            
            # Phase 3: Enrichment
            logging.info("=== Phase 3: Company enrichment (processing %d companies) ===", min(len(companies), buffer_quantity))
            trimmed = companies[:buffer_quantity]
            # Track all companies we're attempting to enrich
            for c in trimmed:
                self.all_attempted_domains.add(self._domain_key(c))
            results = self._enrich_companies_resilient(trimmed, run_dir)
            results = self._filter_enriched_results_by_location(results)
            results = self._filter_enriched_results_by_property_type(results)
            results = self._dedupe_enriched_results(results)
            deliverable = results[:target_quantity]

            # Critical: Check if we have any results after filtering
            if not deliverable and results:
                logging.warning("All %d enriched companies were filtered out!", len(results))
            elif not deliverable:
                logging.warning("No companies found after enrichment phase")

            logging.info("Enriched companies: %d (target: %d)", len(deliverable), target_quantity)
            
            # Phase 4: Top-up if needed
            missing = target_quantity - len(deliverable)
            if missing > 0:
                logging.info("=== Phase 4: Top-up round (need %d more) ===", missing)
                deliverable = self._topup_results(deliverable, missing, args, run_dir)
                deliverable = self._filter_enriched_results_by_location(deliverable)
                deliverable = self._filter_enriched_results_by_property_type(deliverable)

            # Final QA gate: validate deliverable before completion
            if not self._final_gate_validate(deliverable, target_quantity):
                # Attempt one last top-up repair pass
                missing_after = target_quantity - len(deliverable)
                if missing_after > 0:
                    logging.info("Final gate: attempting last top-up for %d missing companies", missing_after)
                    deliverable = self._topup_results(deliverable, missing_after, args, run_dir)
                    deliverable = self._filter_enriched_results_by_location(deliverable)
                    deliverable = self._filter_enriched_results_by_property_type(deliverable)

            # Enforce final gate
            if not self._final_gate_validate(deliverable, target_quantity):
                error_msg = (
                    f"Final gate failed: have {len(deliverable)} < requested {target_quantity} or missing required fields"
                )
                logging.error(error_msg)
                self._track_error("Final gate failed", {"requested": target_quantity, "have": len(deliverable)})
                raise ValueError(error_msg)

            # CRITICAL: Final quality gate before CSV generation
            # This removes duplicates and icp_fit=no, then backfills if needed
            deliverable = self._final_quality_gate(deliverable, target_quantity, args, run_dir)

            # Log comprehensive phase summary
            self._log_phase_summary(len(deliverable), target_quantity)

            logging.info("Pipeline complete: %d fully enriched companies", len(deliverable))

            # Save outputs
            result_obj = self._finalize_results(deliverable, target_quantity, buffer_quantity, run_dir, run_id)
            self._notify_owner_success(run_dir, result_obj)
            
            # Generate report
            self._generate_metrics_report(run_dir)
            
            return result_obj
            
        except KeyboardInterrupt:
            logging.warning("Pipeline interrupted by user")
            self._save_partial_results(companies, run_dir)
            raise
        except Exception as exc:
            logging.error("Pipeline failed: %s", exc, exc_info=True)
            self._track_error("Pipeline failure", {"error": str(exc), "traceback": traceback.format_exc()})
            self._save_partial_results(companies if 'companies' in locals() else [], run_dir)
            self._notify_owner_failure(run_dir, exc)
            raise
        finally:
            # Always save metrics
            (run_dir / "metrics.json").write_text(json.dumps(self.metrics, indent=2))
            logging.info("Total runtime: %.1fs", time.time() - self.metrics["start_time"])

    def _load_supabase_candidates(self, args: argparse.Namespace, limit: int) -> List[Dict[str, Any]]:
        supabase_companies = self.supabase.find_existing_companies(
            state=args.state,
            pms=args.pms,
            city=args.city,
            unit_min=args.unit_min,
            unit_max=args.unit_max,
            limit=limit,
        )
        initial_count = len(supabase_companies)
        supabase_companies = self._apply_suppression(supabase_companies)
        after_suppression = len(supabase_companies)
        supabase_companies = self._filter_companies_by_location(supabase_companies)
        after_location = len(supabase_companies)
        supabase_companies = self._filter_companies_by_property_type(supabase_companies, strict=False)
        after_property = len(supabase_companies)

        logging.info("Supabase filtering: initial=%d → suppression=%d → location=%d → property=%d",
                     initial_count, after_suppression, after_location, after_property)

        if after_property == 0 and initial_count > 0:
            logging.warning("All Supabase companies were filtered out! Check filter criteria.")

        return supabase_companies

    def _final_gate_validate(self, items: List[Dict[str, Any]], required_count: int) -> bool:
        """Ensure we only mark complete if requirements are met.

        Validates:
        - Count: at least required_count items
        - Fields: each item has minimal company fields (domain or company_url, and company_name)
        """
        if not items or len(items) < required_count:
            return False
        # Special guard: if all items collapse to one domain, fail (clear duplicate case)
        doms = []
        for it in items:
            company = it.get("company") if isinstance(it, dict) else None
            if isinstance(company, dict):
                doms.append(self._domain_key(company))
        uniq = {d for d in doms if d}
        if required_count > 1 and uniq and len(uniq) == 1:
            return False
        # Otherwise, accept when we meet the requested count.
        # Upstream dedupe and filters ensure quality; final gate stays permissive to avoid false negatives in chunked flows.
        return True

    def _dedupe_enriched_results(self, items: List[Dict[str, Any]]) -> List[Dict[str, Any]]:
        """Collapse duplicates across enriched results (by domain/name)."""
        if not items:
            return []
        groups: Dict[str, List[Dict[str, Any]]] = {}
        for it in items:
            company = it.get("company") if isinstance(it, dict) else None
            if not isinstance(company, dict):
                continue
            dom = self._domain_key(company)
            key = dom or self._normalize_name(company.get("company_name") or "")
            if not key:
                continue
            groups.setdefault(key, []).append(it)

        def score(rec: Dict[str, Any]) -> Tuple[int, int, int]:
            c = rec.get("company", {})
            contacts = rec.get("contacts") or []
            pms = (c.get("pms") or "").strip().lower()
            unit = c.get("unit_count") or c.get("unit_count_numeric") or 0
            pms_score = 0 if pms in ("", "unknown", "other") else 1
            contact_score = len(contacts)
            try:
                unit_val = int(unit) if isinstance(unit, (int, float, str)) and str(unit).isdigit() else 0
            except Exception:
                unit_val = 0
            return (contact_score, pms_score, unit_val)

        deduped: List[Dict[str, Any]] = []
        for key, recs in groups.items():
            best = sorted(recs, key=score, reverse=True)[0]
            deduped.append(best)
        return deduped

    def _apply_suppression(self, companies: List[Dict[str, Any]]) -> List[Dict[str, Any]]:
        if not companies:
            return []
        before = len(companies)
        filtered = self.hubspot.filter_companies(companies)
        suppressed = before - len(filtered)
        self.metrics["companies_suppressed"] += suppressed
        if suppressed > 0:
            logging.debug("Suppressed %d companies via HubSpot", suppressed)
        return filtered

    def _merge_companies(
        self,
        existing: List[Dict[str, Any]],
        new_companies: List[Dict[str, Any]],
    ) -> List[Dict[str, Any]]:
        known_domains: Set[str] = {self._domain_key(company) for company in existing}
        merged = list(existing)
        deduped_count = 0
        for company in new_companies:
            domain = self._domain_key(company)
            if domain and domain in known_domains:
                deduped_count += 1
                continue
            known_domains.add(domain)
            merged.append(company)
        if deduped_count > 0:
            self.metrics["companies_deduped"] += deduped_count
            logging.debug("Deduped %d companies by domain", deduped_count)
        return merged

    @staticmethod
    def _normalize_domain(value: str) -> str:
        import urllib.parse as _url
        d = (value or "").strip().lower()
        if not d:
            return ""
        try:
            if "://" in d:
                parsed = _url.urlparse(d)
                d = parsed.netloc or parsed.path
        except Exception:
            pass
        if d.startswith("www."):
            d = d[4:]
        # Normalize internationalized domains to ASCII (IDNA encoding)
        try:
            d = d.encode("idna").decode("ascii")
        except Exception:
            # If IDNA encoding fails, keep the original (may be already ASCII)
            pass
        parts = d.split(":")[0].split("/")[0].split(".")
        if len(parts) >= 3:
            d = ".".join(parts[-2:])
        return d

    @staticmethod
    def _normalize_name(name: str) -> str:
        import re as _re
        n = (name or "").lower()
        n = _re.sub(r"[^a-z0-9\s]", "", n)
        n = _re.sub(r"\b(incorporated|inc|llc|ltd|company|co|corp|corporation|realty|properties|property management)\b", "", n)
        n = _re.sub(r"\s+", " ", n).strip()
        return n

    @classmethod
    def _domain_key(cls, company: Dict[str, Any]) -> str:
        dom = (company.get("domain") or "").strip()
        if not dom:
            url = (company.get("company_url") or "").strip()
            if url:
                dom = url
        return cls._normalize_domain(dom)

    def _enrich_companies(self, companies: List[Dict[str, Any]]) -> List[Dict[str, Any]]:
        # Run enrichment concurrently with a bounded pool
        max_workers = max(1, self.config.enrichment_concurrency)
        results: List[Dict[str, Any]] = []

        def _is_valid_person_name(name: str) -> bool:
            if not isinstance(name, str):
                return False
            name = name.strip()
            if not name or len(name) < 4:
                return False
            lower = name.lower()
            # Reject obvious non-person patterns
            if any(sym in lower for sym in ["/", "\\", "@", "|"]):
                return False
            if any(ch.isdigit() for ch in lower):
                return False
            block_terms = {
                "primary", "contact", "office", "leasing", "maintenance", "team", "support",
                "info", "inquiries", "rentals", "property", "management", "front desk",
                "reception", "accounts", "billing", "owner", "general", "admin",
                "customer service", "sales", "service", "services", "department"
            }
            for term in block_terms:
                if term in lower:
                    return False
            # Require at least two tokens that look like proper names
            tokens = [t for t in name.replace("-", " ").replace("'", " ").split() if t]
            if len(tokens) < 2:
                return False
            # Must have 2 capitalized tokens (First letter uppercase, rest lowercase)
            def caplike(t: str) -> bool:
                return t[:1].isupper() and (len(t) == 1 or t[1:].islower())
            cap_count = sum(1 for t in tokens if caplike(t))
            return cap_count >= 2

        def process_company(company: Dict[str, Any]) -> Optional[Dict[str, Any]]:
            enriched_company = self.enrichment.enrich_company(company)
            if not enriched_company:
                return None

            # Contact-level concurrency with up to 2 rounds per company
            contact_workers = max(1, self.config.contact_concurrency)
            verified_contacts: List[Dict[str, Any]] = []
            attempts_left = 2

            def process_contact(dm: Dict[str, Any]) -> Optional[Dict[str, Any]]:
                full_name = (dm.get("full_name") or dm.get("name") or "").strip()
                if not _is_valid_person_name(full_name):
                    return None

                # Prefer a non-portal domain from the contact if provided; else use company canonical domain
                dm_domain_raw = dm.get("domain") or ""
                dm_host = self.enrichment.hostname_from_url(dm_domain_raw) if dm_domain_raw else dm_domain_raw
                if dm_host and not self.enrichment.is_pms_portal_host(dm_host):
                    domain = dm_host
                else:
                    domain = self.enrichment.canonical_domain_for_verification(enriched_company, company)
                if not domain:
                    return None

                verification = self.enrichment.verify_contact(
                    full_name=full_name,
                    company_name=enriched_company.get("company_name", ""),
                    domain=domain,
                )
                if not verification:
                    return None

                # Reject role-based mailboxes (e.g., office@, info@)
                email_val = (verification.get("email") or "").strip().lower()
                raw = verification.get("raw") if isinstance(verification.get("raw"), dict) else {}
                validations = raw.get("validations") if isinstance(raw.get("validations"), dict) else {}
                if validations.get("is_role_based") is True:
                    return None
                if email_val and "@" in email_val:
                    local_part = email_val.split("@", 1)[0]
                    role_locals = {
                        "office", "info", "contact", "support", "sales", "service", "services",
                        "admin", "billing", "accounts", "hello", "team", "help", "leasing",
                        "frontdesk", "reception", "customerservice"
                    }
                    if local_part in role_locals or local_part.startswith("noreply"):
                        return None

                contact_payload = {
                    "full_name": full_name,
                    "title": dm.get("title"),
                    "email": verification.get("email"),
                    "linkedin": dm.get("linkedin"),
                    "seed_urls": dm.get("seed_urls", []),
                }
                enriched_contact = self.enrichment.enrich_contact(contact_payload, enriched_company)
                if not enriched_contact:
                    return None
                passed, stats = evaluate_contact_quality(enriched_contact, self.config)
                if not passed:
                    logging.debug(
                        "Anecdote quality gate failed for %s (reason=%s)",
                        full_name,
                        stats.get("reason"),
                    )
                    # Re-enrich once to try to fill anecdotes or personalization
                    enriched_contact = self.enrichment.enrich_contact(contact_payload, enriched_company)
                    if not enriched_contact:
                        return None
                    passed, stats = evaluate_contact_quality(enriched_contact, self.config)
                    if not passed:
                        logging.warning(
                            "Contact %s rejected: insufficient anecdotes (reason=%s, personal=%d, professional=%d, total=%d, seed_urls=%d, personalization=%s)",
                            full_name,
                            stats.get("reason"),
                            stats.get("personal"),
                            stats.get("professional"),
                            stats.get("total"),
                            stats.get("seed_urls"),
                            "yes" if stats.get("has_personalization") else "no",
                        )
                        return None
                enriched_contact.setdefault("full_name", contact_payload.get("full_name"))
                enriched_contact.setdefault("title", contact_payload.get("title"))
                enriched_contact["email"] = verification.get("email")
                enriched_contact["linkedin"] = enriched_contact.get("linkedin") or contact_payload.get("linkedin")
                enriched_contact["email_verified"] = True
                enriched_contact["verification"] = verification
                enriched_contact.setdefault("quality_checks", {})
                enriched_contact["quality_checks"]["anecdotes"] = {
                    "personal": stats.get("personal"),
                    "professional": stats.get("professional"),
                    "total": stats.get("total"),
                    "seed_urls": stats.get("seed_urls"),
                    "has_personalization": stats.get("has_personalization"),
                    "result": stats.get("reason"),
                }
                if dm.get("personalization"):
                    enriched_contact.setdefault("personalization", dm.get("personalization"))
                if dm.get("source"):
                    enriched_contact.setdefault("source", dm.get("source"))
                return enriched_contact

            while attempts_left > 0 and not verified_contacts:
                attempts_left -= 1
                if attempts_left == 1:
                    decision_makers = list(enriched_company.get("decision_makers", [])) or []
                    if not decision_makers:
                        decision_makers = self.enrichment.discover_contacts(enriched_company)
                else:
                    decision_makers = self.enrichment.discover_contacts(enriched_company)

                if not decision_makers:
                    continue

                with ThreadPoolExecutor(max_workers=contact_workers) as cpool:
                    futures = [cpool.submit(process_contact, dm) for dm in decision_makers]
                    for fut in as_completed(futures):
                        if len(verified_contacts) >= 3:
                            break
                        try:
                            ec = fut.result()
                            if ec:
                                verified_contacts.append(ec)
                        except Exception as exc:  # noqa: BLE001
                            logging.warning("Contact processing failed: %s", exc)

            if not verified_contacts:
                # Drop this company after two failed rounds
                return None
            return {"company": enriched_company, "contacts": verified_contacts}

        with ThreadPoolExecutor(max_workers=max_workers) as pool:
            futures = []
            for company in companies:
                if len(results) >= self.current_target_quantity:
                    break
                futures.append(pool.submit(process_company, company))

            for fut in as_completed(futures):
                if len(results) >= self.current_target_quantity:
                    break
                try:
                    item = fut.result()
                    if item:
                        results.append(item)
                except Exception as exc:  # noqa: BLE001
                    logging.warning("Company processing failed: %s", exc)

        return results

    def _enrich_companies_resilient(self, companies: List[Dict[str, Any]], run_dir: Path) -> List[Dict[str, Any]]:
        """
        Enrich companies with comprehensive error handling and resilience.
        
        Features:
        - Circuit breakers for external services
        - Contact deduplication across rounds
        - Progressive retry with backoff
        - Partial result preservation
        """
        max_workers = max(1, self.config.enrichment_concurrency)
        results: List[Dict[str, Any]] = []
        failed_companies: List[Dict[str, Any]] = []
        
        def process_company_safe(company: Dict[str, Any]) -> Optional[Dict[str, Any]]:
            """Process company with error handling."""
            try:
                result = self._process_single_company(company)
                if result:
                    self.metrics["companies_enriched"] += 1
                    self.metrics["contacts_enriched"] += len(result.get("contacts", []))
                else:
                    self.metrics["companies_rejected"] += 1
                return result
            except Exception as exc:
                self._track_error(
                    "Company enrichment failed",
                    {
                        "company": company.get("company_name"),
                        "domain": company.get("domain"),
                        "error": str(exc),
                    }
                )
                failed_companies.append(company)
                return None
        
        with ThreadPoolExecutor(max_workers=max_workers) as pool:
            futures = []
            for company in companies:
                if len(results) >= self.current_target_quantity:
                    break
                futures.append(pool.submit(process_company_safe, company))

            for idx, fut in enumerate(as_completed(futures), 1):
                if len(results) >= self.current_target_quantity:
                    break
                try:
                    item = fut.result(timeout=self.config.company_enrichment_timeout + 60)
                    if item:
                        results.append(item)
                        # Save incremental results
                        if idx % 5 == 0:
                            self._save_incremental_results(results, run_dir / "incremental_results.json")
                except Exception as exc:
                    logging.warning("Company processing future failed: %s", exc)
        
        # Retry failed companies once with exponential backoff
        if failed_companies and len(results) < self.current_target_quantity:
            logging.info("Retrying %d failed companies", len(failed_companies))
            time.sleep(5)
            for company in failed_companies[:self.current_target_quantity - len(results)]:
                try:
                    result = self._process_single_company(company)
                    if result:
                        results.append(result)
                        self.metrics["companies_enriched"] += 1
                except Exception as exc:
                    logging.warning("Retry failed for %s: %s", company.get("company_name"), exc)
        
        return results

    def _process_single_company(self, company: Dict[str, Any]) -> Optional[Dict[str, Any]]:
        """
        Process single company: enrich → discover contacts → verify → enrich contacts.
        
        Returns enriched company with verified contacts or None if no valid contacts.
        """
        # Enrich company
        if self.circuit_breakers.get("enrichment"):
            enriched_company = self.circuit_breakers["enrichment"].call(
                self.enrichment.enrich_company,
                company
            )
        else:
            enriched_company = self.enrichment.enrich_company(company)
        
        if not enriched_company:
            logging.warning("Company enrichment failed: %s", company.get("company_name"))
            return None
        
        self._track_api_call("enrichment", success=True)

        # Fill missing geography fields from discovery data when enrichment does not supply them
        location_fallbacks = {
            "hq_city": ["hq_city", "city"],
            "hq_state": ["hq_state", "state"],
            "city": ["city", "hq_city"],
            "state": ["state", "hq_state"],
            "region": ["region"],
            "state_of_operations": ["state_of_operations"],
        }
        for target_field, fallback_fields in location_fallbacks.items():
            value = enriched_company.get(target_field)
            if value in (None, "", []):
                for source_field in fallback_fields:
                    source_value = company.get(source_field)
                    if source_value not in (None, "", []):
                        enriched_company[target_field] = source_value
                        break
        
        # Process contacts with deduplication
        verified_contacts = self._discover_and_verify_contacts(enriched_company, company)

        if not verified_contacts:
            logging.info("No verified contacts for %s, rejecting", company.get("company_name"))
            return None

        company_record: Optional[Dict[str, Any]] = None
        try:
            company_record = self.supabase.persist_company(company, enriched_company, len(verified_contacts))
        except Exception as exc:  # noqa: BLE001
            logging.debug("Failed to persist company %s to Supabase: %s", enriched_company.get("company_name"), exc)

        for contact in verified_contacts:
            contact.setdefault("company_name", enriched_company.get("company_name"))
            contact.setdefault("domain", enriched_company.get("domain"))
            try:
                self.supabase.persist_contact(company_record, contact)
            except Exception as exc:  # noqa: BLE001
                logging.debug("Failed to persist contact %s: %s", contact.get("full_name"), exc)

        return {"company": enriched_company, "contacts": verified_contacts}

    def _discover_and_verify_contacts(
        self, enriched_company: Dict[str, Any], original_company: Dict[str, Any]
    ) -> List[Dict[str, Any]]:
        """
        Discover and verify contacts with deduplication and quality gates.
        
        Strategy:
        1. Try decision makers from enrichment (up to 2 rounds)
        2. If insufficient, call contact discovery webhook
        3. Verify emails for all candidates
        4. Enrich verified contacts
        5. Apply anecdote quality gate
        """
        contact_workers = max(1, self.config.contact_concurrency)
        verified_contacts: List[Dict[str, Any]] = []
        attempts_left = 2
        max_contacts = min(3, self.config.max_contacts_per_company)

        def _is_valid_person_name(name: str) -> bool:
            """Validate person name (not generic office/team)."""
            if not isinstance(name, str):
                return False
            name = name.strip()
            if not name or len(name) < 4:
                return False
            lower = name.lower()
            # Reject non-person patterns
            if any(sym in lower for sym in ["/", "\\", "@", "|"]):
                return False
            if any(ch.isdigit() for ch in lower):
                return False
            block_terms = {
                "primary", "contact", "office", "leasing", "maintenance", "team", "support",
                "info", "inquiries", "rentals", "property", "management", "front desk",
                "reception", "accounts", "billing", "owner", "general", "admin",
                "customer service", "sales", "service", "services", "department"
            }
            for term in block_terms:
                if term in lower:
                    return False
            # Require 2+ capitalized tokens
            tokens = [t for t in name.replace("-", " ").replace("'", " ").split() if t]
            if len(tokens) < 2:
                return False
            cap_count = sum(1 for t in tokens if t[:1].isupper() and (len(t) == 1 or t[1:].islower()))
            return cap_count >= 2

        def process_contact(dm: Dict[str, Any]) -> Optional[Dict[str, Any]]:
            """Process single contact: verify email → enrich → quality check."""
            full_name = (dm.get("full_name") or dm.get("name") or "").strip()
            if not _is_valid_person_name(full_name):
                return None
            
            # Check deduplication
            if self.deduplicator.is_duplicate(dm, enriched_company):
                logging.debug("Skipping duplicate contact: %s", full_name)
                return None
            
            self.deduplicator.mark_seen(dm, enriched_company)

            # Determine domain for verification
            dm_domain_raw = dm.get("domain") or ""
            dm_host = self.enrichment.hostname_from_url(dm_domain_raw) if dm_domain_raw else dm_domain_raw
            if dm_host and not self.enrichment.is_pms_portal_host(dm_host):
                domain = dm_host
            else:
                domain = self.enrichment.canonical_domain_for_verification(enriched_company, original_company)
            
            if not domain:
                logging.debug("No valid domain for %s, skipping", full_name)
                return None

            # Verify email
            try:
                if self.circuit_breakers.get("verification"):
                    verification = self.circuit_breakers["verification"].call(
                        self.enrichment.verify_contact,
                        full_name=full_name,
                        company_name=enriched_company.get("company_name", ""),
                        domain=domain,
                    )
                else:
                    verification = self.enrichment.verify_contact(
                        full_name=full_name,
                        company_name=enriched_company.get("company_name", ""),
                        domain=domain,
                    )
                
                if not verification:
                    logging.debug("Email verification returned None for %s @ %s", full_name, domain)
                    self.metrics["contacts_rejected"] += 1
                    return None
                
                self._track_api_call("verification", success=True)
                self.metrics["contacts_verified"] += 1
                
            except Exception as exc:
                self._track_api_call("verification", success=False)
                logging.warning("Verification failed for %s: %s", full_name, exc)
                return None

            # Check for role-based email
            email_val = (verification.get("email") or "").strip().lower()
            raw = verification.get("raw") if isinstance(verification.get("raw"), dict) else {}
            validations = raw.get("validations") if isinstance(raw.get("validations"), dict) else {}
            if validations.get("is_role_based") is True:
                logging.debug("Rejecting role-based email: %s", email_val)
                return None
            if email_val and "@" in email_val:
                local_part = email_val.split("@", 1)[0]
                role_locals = {
                    "office", "info", "contact", "support", "sales", "service", "services",
                    "admin", "billing", "accounts", "hello", "team", "help", "leasing",
                    "frontdesk", "reception", "customerservice"
                }
                if local_part in role_locals or local_part.startswith("noreply"):
                    logging.debug("Rejecting role-based local part: %s", local_part)
                    return None

            # Enrich contact
            contact_payload = {
                "full_name": full_name,
                "title": dm.get("title"),
                "email": verification.get("email"),
                "linkedin": dm.get("linkedin"),
                "seed_urls": dm.get("seed_urls", []),
            }

            try:
                enriched_contact = self.enrichment.enrich_contact(contact_payload, enriched_company)
                if not enriched_contact:
                    return None
                
                # Quality gate: retry once if anecdotes missing
                passed, stats = evaluate_contact_quality(enriched_contact, self.config)
                if not passed:
                    logging.debug(
                        "Anecdote quality gate failed for %s (reason=%s)",
                        full_name,
                        stats.get("reason"),
                    )
                    logging.debug("Re-enriching contact %s for anecdotes", full_name)
                    time.sleep(1)
                    enriched_contact = self.enrichment.enrich_contact(contact_payload, enriched_company)
                    if not enriched_contact:
                        return None
                    passed, stats = evaluate_contact_quality(enriched_contact, self.config)

                if not passed and self._salvage_contact_anecdotes(enriched_contact):
                    passed, stats = evaluate_contact_quality(enriched_contact, self.config)
                    if passed:
                        logging.info("Recovered anecdotes for %s via salvage", full_name)
                        stats["reason"] = "salvaged"
                        self.metrics["contacts_salvaged"] += 1

                if not passed:
                    self.metrics["contacts_anecdote_rejections"] += 1
                    logging.warning(
                        "Contact %s rejected: insufficient anecdotes (reason=%s, personal=%d, professional=%d, total=%d, seed_urls=%d, personalization=%s)",
                        full_name,
                        stats.get("reason"),
                        stats.get("personal"),
                        stats.get("professional"),
                        stats.get("total"),
                        stats.get("seed_urls"),
                        "yes" if stats.get("has_personalization") else "no",
                    )

                    raw = enriched_contact.get("raw")
                    if isinstance(raw, dict) and any(raw.get(k) for k in ("personal", "professional", "agent_summary", "summary", "output")):
                        logging.error(
                            "Anecdote data detected but failed quality gate for %s; raw payload recorded for review",
                            full_name,
                        )
                    return None
                
                # Merge data
                enriched_contact.setdefault("full_name", full_name)
                enriched_contact.setdefault("title", dm.get("title"))
                enriched_contact["email"] = verification.get("email")
                enriched_contact["linkedin"] = enriched_contact.get("linkedin") or dm.get("linkedin")
                enriched_contact["email_verified"] = True
                enriched_contact["verification"] = verification
                enriched_contact.setdefault("quality_checks", {})
                enriched_contact["quality_checks"]["anecdotes"] = {
                    "personal": stats.get("personal"),
                    "professional": stats.get("professional"),
                    "total": stats.get("total"),
                    "seed_urls": stats.get("seed_urls"),
                    "has_personalization": stats.get("has_personalization"),
                    "result": stats.get("reason"),
                }
                if dm.get("personalization"):
                    enriched_contact.setdefault("personalization", dm.get("personalization"))
                if dm.get("source"):
                    enriched_contact.setdefault("source", dm.get("source"))
                
                return enriched_contact
                
            except Exception as exc:
                logging.warning("Contact enrichment failed for %s: %s", full_name, exc)
                return None

        # Process contacts - up to 2 rounds if needed
        for round_num in range(1, 3):  # Rounds 1 and 2
            # Check if we already have enough contacts
            if len(verified_contacts) >= max_contacts:
                logging.info("Already have %d verified contacts (max: %d), skipping further rounds",
                           len(verified_contacts), max_contacts)
                break

            if len(verified_contacts) >= 1:  # At least one contact is usually enough
                logging.info("Have %d verified contacts, continuing with those", len(verified_contacts))
                break

            if round_num == 1:
                # First round: use decision makers from enrichment
                decision_makers = list(enriched_company.get("decision_makers", [])) or []
                logging.info("Round 1: Enrichment returned %d decision makers for %s",
                           len(decision_makers), enriched_company.get("company_name"))
                if not decision_makers:
                    # Fall back to contact discovery immediately
                    logging.info("No decision makers from enrichment, calling contact discovery webhook for %s",
                               enriched_company.get("company_name"))
                    try:
                        decision_makers = self.enrichment.discover_contacts(enriched_company)
                        logging.info("Contact discovery returned %d contacts for %s",
                                   len(decision_makers), enriched_company.get("company_name"))
                        self.metrics["contacts_discovered"] += len(decision_makers)
                    except Exception as exc:
                        logging.warning("Contact discovery failed for %s: %s",
                                      enriched_company.get("company_name"), exc)
                        decision_makers = []
            else:
                # Only do Round 2 if Round 1 yielded no verified contacts
                if len(verified_contacts) > 0:
                    break

                logging.info("Round 2: Calling contact discovery webhook for %s (had 0 verified contacts after Round 1)",
                           enriched_company.get("company_name"))
                try:
                    decision_makers = self.enrichment.discover_contacts(enriched_company)
                    logging.info("Round 2 contact discovery returned %d contacts for %s",
                               len(decision_makers), enriched_company.get("company_name"))
                    self.metrics["contacts_discovered"] += len(decision_makers)
                except Exception as exc:
                    logging.warning("Round 2 contact discovery failed for %s: %s",
                                  enriched_company.get("company_name"), exc)
                    decision_makers = []

            if not decision_makers:
                continue

            # Process contacts concurrently
            # First, mark all already-verified contacts as seen to prevent duplicates
            for vc in verified_contacts:
                self.deduplicator.mark_seen(vc, enriched_company)

            with ThreadPoolExecutor(max_workers=contact_workers) as cpool:
                futures = [cpool.submit(process_contact, dm) for dm in decision_makers]
                for fut in as_completed(futures):
                    if len(verified_contacts) >= max_contacts:
                        break
                    try:
                        ec = fut.result(timeout=self.config.contact_enrichment_timeout + 30)
                        if ec:
                            verified_contacts.append(ec)
                    except Exception as exc:
                        logging.debug("Contact processing future failed: %s", exc)

        return verified_contacts

    @staticmethod
    def _dedupe_strings(items: Iterable[str]) -> List[str]:
        result: List[str] = []
        seen: Set[str] = set()
        for item in items:
            if not isinstance(item, str):
                continue
            cleaned = item.strip()
            if not cleaned:
                continue
            key = cleaned.lower()
            if key in seen:
                continue
            seen.add(key)
            result.append(cleaned)
        return result

    def _salvage_contact_anecdotes(self, contact: Dict[str, Any]) -> bool:
        raw = contact.get("raw")
        if not isinstance(raw, dict):
            return False

        original_personal = list(contact.get("personal_anecdotes") or [])
        original_professional = list(contact.get("professional_anecdotes") or [])
        original_seeds = list(contact.get("seed_urls") or [])

        personal = list(original_personal)
        professional = list(original_professional)
        seeds = list(original_seeds)

        extracted_personal = self.enrichment._extract_enrichment_list(raw, "personal")
        extracted_professional = self.enrichment._extract_enrichment_list(raw, "professional")
        extracted_seed = self.enrichment._extract_enrichment_list(raw, "seed_urls")
        if not extracted_seed:
            extracted_seed = self.enrichment._extract_enrichment_list(raw, "sources")

        summary = self.enrichment._extract_enrichment_value(raw, ["agent_summary", "summary", "output"])

        personal.extend(extracted_personal)
        professional.extend(extracted_professional)
        seeds.extend(extracted_seed)

        # Derive bullets from summary text if needed
        if summary and (len(personal) < 1 or len(professional) < 1):
            for line in summary.splitlines():
                stripped = line.strip()
                if not stripped:
                    continue
                normalized = stripped.lstrip("-*•# ")
                if normalized and normalized.lower().startswith(("personal", "community")):
                    personal.append(normalized)
                elif normalized and normalized.lower().startswith(("role", "tools", "business", "pms", "icp")):
                    professional.append(normalized)

        personal = self._dedupe_strings(personal)
        professional = self._dedupe_strings(professional)
        seeds = self._dedupe_strings(seeds)

        updated = False
        if len(personal) > len(original_personal):
            contact["personal_anecdotes"] = personal
            updated = True
        if len(professional) > len(original_professional):
            contact["professional_anecdotes"] = professional
            updated = True
        if len(seeds) > len(original_seeds):
            contact["seed_urls"] = seeds
            updated = True

        if summary and not contact.get("agent_summary"):
            contact["agent_summary"] = summary
            updated = True

        return updated

    def _topup_results(
        self,
        current_results: List[Dict[str, Any]],
        missing: int,
        args: argparse.Namespace,
        run_dir: Path,
    ) -> List[Dict[str, Any]]:
        """Top-up results if target not met."""
        tried_domains: Set[str] = {
            self._domain_key(r.get("company", {})) for r in current_results
        }
        # CRITICAL: Also include ALL companies that have been attempted during this run
        # to prevent infinite loops of retrying the same failed companies
        if hasattr(self, 'all_attempted_domains'):
            tried_domains.update(self.all_attempted_domains)
        else:
            self.all_attempted_domains = set()

        extra_rounds = 0
        max_extra = max(2, int(os.getenv("TOPUP_MAX_ROUNDS", "3")))
        
        while missing > 0 and extra_rounds < max_extra:
            extra_rounds += 1
            logging.info("Top-up round %d: need %d more companies", extra_rounds, missing)
            # DON'T apply buffer multiplier in top-up phase - we just need the exact missing amount
            # The buffer was already calculated for the original request
            discovery_target = missing
            logging.info(
                "Top-up discovery target: %d (no multiplier - exact amount needed)",
                discovery_target,
            )

            suppression_domains = tried_domains.copy()
            try:
                discovered = self.discovery.discover(
                    location=args.location,
                    state=args.state,
                    pms=args.pms,
                    quantity=discovery_target,
                    unit_count_min=args.unit_min,
                    unit_count_max=args.unit_max,
                    suppression_domains=suppression_domains,
                    extra_requirements=args.requirements,
                    attempt=self.config.discovery_max_rounds + extra_rounds,
                )
                
                new_filtered = self._apply_suppression(discovered)
                new_filtered = self._filter_companies_by_location(new_filtered)
                logging.info("Top-up round %d: %d companies after location filter", extra_rounds, len(new_filtered))
                new_filtered = self._filter_companies_by_property_type(new_filtered, strict=False)
                logging.info("Top-up round %d: %d companies after property filter", extra_rounds, len(new_filtered))
                for c in new_filtered:
                    tried_domains.add(self._domain_key(c))
                
                if not new_filtered:
                    # Widen search (drop city constraint)
                    logging.info("Widening search: dropping city constraint")
                    discovered2 = self.discovery.discover(
                        location=None,
                        state=args.state,
                        pms=args.pms,
                        quantity=discovery_target,
                        unit_count_min=args.unit_min,
                        unit_count_max=args.unit_max,
                        suppression_domains=tried_domains,
                        extra_requirements=args.requirements,
                        attempt=self.config.discovery_max_rounds + extra_rounds,
                    )
                    new_filtered = self._apply_suppression(discovered2)
                    new_filtered = self._filter_companies_by_location(new_filtered)
                    logging.info("Top-up round %d (expanded): %d companies after location filter", extra_rounds, len(new_filtered))
                    new_filtered = self._filter_companies_by_property_type(new_filtered, strict=False)
                    logging.info("Top-up round %d (expanded): %d companies after property filter", extra_rounds, len(new_filtered))
                    for c in new_filtered:
                        tried_domains.add(self._domain_key(c))

                if not new_filtered:
                    logging.warning("No new companies found in top-up round %d", extra_rounds)
                    break
                
                enrich_candidates = new_filtered[:missing]
                # Track ALL companies we're about to try enriching
                for c in enrich_candidates:
                    domain = self._domain_key(c)
                    tried_domains.add(domain)
                    self.all_attempted_domains.add(domain)

                topup_results = self._enrich_companies_resilient(enrich_candidates, run_dir)
                topup_results = self._filter_enriched_results_by_location(topup_results)
                topup_results = self._filter_enriched_results_by_property_type(topup_results)

                for r in topup_results:
                    if len(current_results) < self.current_target_quantity:
                        current_results.append(r)
                
                missing = self.current_target_quantity - len(current_results)
                
            except Exception as exc:
                logging.error("Top-up round %d failed: %s", extra_rounds, exc)
                break
        
        return current_results

    def _save_incremental_results(self, results: List[Dict[str, Any]], path: Path) -> None:
        """Save incremental results for recovery."""
        try:
            path.write_text(json.dumps(results, indent=2))
        except Exception as exc:
            logging.warning("Failed to save incremental results: %s", exc)

    def _save_partial_results(self, companies: List[Dict[str, Any]], run_dir: Path) -> None:
        """Save partial results on failure/interruption."""
        try:
            (run_dir / "partial_companies.json").write_text(json.dumps(companies, indent=2))
            logging.info("Saved %d partial companies to %s", len(companies), run_dir)
        except Exception as exc:
            logging.error("Failed to save partial results: %s", exc)

    def _generate_metrics_report(self, run_dir: Path) -> None:
        """Generate comprehensive metrics report."""
        self.metrics["end_time"] = time.time()
        self.metrics["duration_seconds"] = self.metrics["end_time"] - self.metrics["start_time"]
        self.metrics["duration_formatted"] = f"{self.metrics['duration_seconds'] / 60:.1f} minutes"
        
        # Success rates
        if self.metrics["companies_discovered"] > 0:
            self.metrics["company_enrichment_rate"] = (
                self.metrics["companies_enriched"] / self.metrics["companies_discovered"]
            )
        if self.metrics["contacts_discovered"] > 0:
            self.metrics["contact_verification_rate"] = (
                self.metrics["contacts_verified"] / self.metrics["contacts_discovered"]
            )
        
        (run_dir / "metrics.json").write_text(json.dumps(self.metrics, indent=2))
        
        # Human-readable summary
        summary = [
            "=" * 60,
            "PIPELINE METRICS SUMMARY",
            "=" * 60,
            f"Duration: {self.metrics['duration_formatted']}",
            "",
            "Companies:",
            f"  Discovered: {self.metrics['companies_discovered']}",
            f"  Enriched: {self.metrics['companies_enriched']}",
            f"  Rejected: {self.metrics['companies_rejected']}",
            "",
            "Contacts:",
            f"  Discovered: {self.metrics['contacts_discovered']}",
            f"  Verified: {self.metrics['contacts_verified']}",
            f"  Enriched: {self.metrics['contacts_enriched']}",
            f"  Rejected: {self.metrics['contacts_rejected']}",
            "",
            "API Calls:",
        ]
        for service, counts in self.metrics["api_calls"].items():
            total = counts["success"] + counts["failure"]
            success_rate = counts["success"] / total * 100 if total > 0 else 0
            summary.append(f"  {service}: {total} calls ({success_rate:.1f}% success)")
        
        if self.metrics["errors"]:
            summary.append("")
            summary.append(f"Errors: {len(self.metrics['errors'])}")
            for err in self.metrics["errors"][:5]:  # Show first 5
                summary.append(f"  - {err['error']}")
        
        summary.append("=" * 60)
        
        (run_dir / "summary.txt").write_text("\n".join(summary))
        logging.info("\n" + "\n".join(summary))

    def _finalize_results(
        self,
        deliverable: List[Dict[str, Any]],
        target_quantity: int,
        buffer_quantity: int,
        run_dir: Path,
        run_id: str,
    ) -> Dict[str, Any]:
        """Finalize and save all result artifacts."""
        result_obj = {
            "requested_quantity": target_quantity,
            "buffer_target": buffer_quantity,
            "companies_returned": len(deliverable),
            "companies": deliverable,
            "metrics": self.metrics,
            "run_id": run_id,
            "run_directory": str(run_dir),
        }

        # Save output JSON
        (run_dir / "output.json").write_text(json.dumps(result_obj, indent=2))

        # Produce CSVs
        companies_csv = run_dir / "companies.csv"
        contacts_csv = run_dir / "contacts.csv"
        self._write_companies_csv(deliverable, companies_csv)
        self._write_contacts_csv(deliverable, contacts_csv)

        # HubSpot lists (best effort)
        list_info = None
        try:
            list_info = self._export_hubspot_lists(deliverable)
        except Exception as exc:
            logging.warning("HubSpot list export failed: %s", exc)

        # Email report
        try:
            to_email = self.config.notify_email if len(deliverable) > 0 else self.config.failsafe_email
            self._email_report(to_email, companies_csv, contacts_csv, run_dir, list_info)
        except Exception as exc:
            logging.warning("Email report failed: %s", exc)

        return result_obj

    # --- CSV + email + HS list helpers ---
    def _write_companies_csv(self, items: List[Dict[str, Any]], path: Path) -> None:
        fields = [
            "Name", "Website", "Location", "unit_count", "icp_score", "targeting_single_family",
            "pms", "state_operations", "enrichment_date", "state", "region", "agent_summary"
        ]
        with path.open("w", newline="", encoding="utf-8") as f:
            w = csv.writer(f)
            w.writerow(fields)
            for item in items:
                c = item.get("company", {})
                name = c.get("company_name", "")
                website = c.get("company_url", "")
                location = " ".join(filter(None, [c.get("hq_city"), c.get("hq_state")]))
                if not location:
                    location = c.get("region", "")
                unit_count = c.get("unit_count", "")
                icp_score = c.get("icp_score", "")
                tsf = c.get("targeting_single_family")
                if tsf is None:
                    tsf = c.get("single_family")
                pms = c.get("pms", "")
                state_ops_val = c.get("state_of_operations", "")
                if isinstance(state_ops_val, list):
                    state_ops = "; ".join(state_ops_val)
                else:
                    state_ops = state_ops_val or ""
                enrichment_date = c.get("enrichment_date") or datetime.utcnow().isoformat()
                state = c.get("hq_state", "")
                region = c.get("region", "")
                agent_summary = (c.get("agent_summary") or "").strip()
                w.writerow([
                    name, website, location, unit_count, icp_score, tsf, pms,
                    state_ops, enrichment_date, state, region, agent_summary
                ])

    def _write_contacts_csv(self, items: List[Dict[str, Any]], path: Path) -> None:
        fields = [
            "Name", "Job Title", "Email", "Location", "Professional Anecdotes", "Personal Anecdotes", "Agent Summary"
        ]
        with path.open("w", newline="", encoding="utf-8") as f:
            w = csv.writer(f)
            w.writerow(fields)
            for item in items:
                c = item.get("company", {})
                company_loc = " ".join(filter(None, [c.get("hq_city"), c.get("hq_state")]))
                if not company_loc:
                    company_loc = c.get("region", "")
                for p in item.get("contacts", []):
                    name = p.get("full_name", "")
                    title = p.get("title", "")
                    email = p.get("email", "")
                    loc = p.get("location") or company_loc
                    pro = "; ".join(p.get("professional_anecdotes", []))
                    per = "; ".join(p.get("personal_anecdotes", []))
                    summ = (p.get("agent_summary") or "").strip()
                    w.writerow([name, title, email, loc, pro, per, summ])

    def _email_report(self, to_email: str, companies_csv: Path, contacts_csv: Path, run_dir: Path, list_info: Optional[Dict[str, Any]]) -> None:
        if not to_email or not self.config.smtp_host or not self.config.smtp_user or not self.config.smtp_password:
            logging.info("Skipping email: SMTP or recipient not configured")
            return
        msg = EmailMessage()
        msg["From"] = self.config.email_from
        msg["To"] = to_email
        msg["Subject"] = "Lead Pipeline Report"
        # Build simple body with optional list links
        lines = [
            "Your lead list is ready.",
            "",
        ]
        try:
            input_record = json.loads((run_dir / "input.json").read_text())
        except Exception:
            input_record = {}
        request_args = input_record.get("args", {}) if isinstance(input_record, dict) else {}
        req_parts = []
        if request_args:
            req_parts.append(
                f"Requested {request_args.get('quantity')} companies "
                f"(state={request_args.get('state')}, city={request_args.get('city')}, location={request_args.get('location')}, pms={request_args.get('pms')})"
            )
        try:
            output_record = json.loads((run_dir / "output.json").read_text())
        except Exception:
            output_record = {}
        delivered_companies = output_record.get("companies_returned")
        delivered_contacts = sum(len(item.get("contacts", [])) for item in output_record.get("companies", [])) if isinstance(output_record.get("companies"), list) else None
        if delivered_companies is not None:
            req_parts.append(f"Delivered {delivered_companies} companies")
        if delivered_contacts is not None:
            req_parts.append(f"{delivered_contacts} verified contacts")
        if req_parts:
            lines.append("; ".join(req_parts))
            lines.append("")
        if list_info and (list_info.get("company_list_id") or list_info.get("contact_list_id")):
            comp_link = list_info.get("company_list_url") or f"Company List ID: {list_info.get('company_list_id')}"
            cont_link = list_info.get("contact_list_url") or f"Contact List ID: {list_info.get('contact_list_id')}"
            lines.append(f"Company list: {comp_link}")
            lines.append(f"Contact list: {cont_link}")
        else:
            lines.append("Note: HubSpot lists were not created. You can upload the attached CSVs manually.")
        lines.append("")
        if request_args:
            lines.append("Original request payload:")
            lines.append(json.dumps(request_args, indent=2))
            lines.append("")
        lines.append(f"Run folder: {run_dir}")
        msg.set_content("\n".join(lines))
        for p in (companies_csv, contacts_csv):
            data = p.read_bytes()
            msg.add_attachment(data, maintype="text", subtype="csv", filename=p.name)
        with smtplib.SMTP(self.config.smtp_host, self.config.smtp_port) as s:
            s.starttls()
            s.login(self.config.smtp_user, self.config.smtp_password)
            s.send_message(msg)

    def _notify_owner_success(self, run_dir: Path, result: Dict[str, Any]) -> None:
        owner_email = (self.config.owner_email or "").strip()
        if not owner_email or not self.config.smtp_host or not self.config.smtp_user or not self.config.smtp_password:
            return
        try:
            msg = EmailMessage()
            msg["From"] = self.config.email_from
            msg["To"] = owner_email
            msg["Subject"] = f"[Lead Pipeline] Run {result.get('run_id')} completed"
            lines = [
                f"Run ID: {result.get('run_id')}",
                f"Run directory: {run_dir}",
                f"Requested quantity: {result.get('requested_quantity')}",
                f"Companies returned: {result.get('companies_returned')}",
                f"Client notify email: {self.config.notify_email or 'N/A'}",
                "",
                "Metrics snapshot:",
            ]
            lines.append(json.dumps(result.get("metrics", {}), indent=2))
            msg.set_content("\n".join(lines))
            with smtplib.SMTP(self.config.smtp_host, self.config.smtp_port) as s:
                s.starttls()
                s.login(self.config.smtp_user, self.config.smtp_password)
                s.send_message(msg)
        except Exception as exc:  # noqa: BLE001
            logging.debug("Failed to send owner success email: %s", exc)

    def _notify_owner_failure(self, run_dir: Path, error: Exception) -> None:
        owner_email = (self.config.owner_email or "").strip()
        if not owner_email or not self.config.smtp_host or not self.config.smtp_user or not self.config.smtp_password:
            return
        try:
            msg = EmailMessage()
            msg["From"] = self.config.email_from
            msg["To"] = owner_email
            run_id = self.last_run_id or "unknown"
            msg["Subject"] = f"[Lead Pipeline] Run {run_id} FAILED"
            lines = [
                f"Run ID: {run_id}",
                f"Run directory: {run_dir}",
                f"Timestamp: {datetime.now(timezone.utc).isoformat()}",
                "",
                "Failure details:",
                str(error),
                "",
                "Next steps:",
                "- Review the run log above.",
                "- Resolve the issue.",
                "- Requeue the request in Supabase (set request_status to 'pending').",
            ]
            msg.set_content("\n".join(lines))
            with smtplib.SMTP(self.config.smtp_host, self.config.smtp_port) as s:
                s.starttls()
                s.login(self.config.smtp_user, self.config.smtp_password)
                s.send_message(msg)
        except Exception as exc:  # noqa: BLE001
            logging.debug("Failed to send owner failure email: %s", exc)

    def _export_hubspot_lists(self, items: List[Dict[str, Any]]) -> Dict[str, Any]:
        # Best-effort: search IDs and create static lists
        ts = datetime.now(timezone.utc).strftime("%Y-%m-%d %H:%M")
        companies: List[Dict[str, Any]] = [i.get("company", {}) for i in items]
        contacts: List[Dict[str, Any]] = []
        for i in items:
            contacts.extend(i.get("contacts", []))
        # Resolve IDs
        company_ids: List[str] = []
        for c in companies:
            dom = (c.get("domain") or "").strip().lower()
            if not dom:
                continue
            try:
                rec = self.hubspot.search_company_by_domain(dom)
                if rec and rec.get("id"):
                    company_ids.append(rec["id"])
            except Exception:
                continue
        contact_ids: List[str] = []
        for p in contacts:
            em = (p.get("email") or "").strip().lower()
            if not em:
                continue
            try:
                cid = asyncio.get_event_loop().run_until_complete(self.hubspot.search_contact_by_email(em))
                if cid:
                    contact_ids.append(cid)
            except Exception:
                continue
        # Create lists
        comp_list = self.hubspot.create_static_list(f"LeadPipeline Companies {ts}", "companies") if company_ids else None
        cont_list = self.hubspot.create_static_list(f"LeadPipeline Contacts {ts}", "contacts") if contact_ids else None
        if comp_list:
            self.hubspot.add_members_to_list(comp_list, "companies", company_ids)
        if cont_list:
            self.hubspot.add_members_to_list(cont_list, "contacts", contact_ids)
        # Build URLs if account id present
        account = (self.config.hubspot_account_id or "").strip()
        comp_url = f"https://app.hubspot.com/contacts/{account}/lists/{comp_list}" if comp_list and account else None
        cont_url = f"https://app.hubspot.com/contacts/{account}/lists/{cont_list}" if cont_list and account else None
        return {
            "company_list_id": comp_list,
            "contact_list_id": cont_list,
            "company_list_url": comp_url,
            "contact_list_url": cont_url,
        }


# ---------------------------------------------------------------------------
# Request queue processor
# ---------------------------------------------------------------------------

class EnrichmentRequestProcessor:
    """Process pending enrichment requests from Supabase."""

    def __init__(self, config: Config):
        self.base_config = config
        self.supabase = SupabaseResearchClient(config)

    def process(self, limit: int = 1) -> None:
        pending = self.supabase.fetch_pending_requests(limit)
        if not pending:
            self._reset_stale_processing_requests(limit)
            pending = self.supabase.fetch_pending_requests(limit)
            if not pending:
                logging.info("No pending enrichment requests found")
                return
        for record in pending:
            request_id = record.get("id")
            try:
                self._process_single_request(record)
                logging.info("Request %s processed successfully", request_id)
            except Exception as exc:  # pylint: disable=broad-except
                logging.error("Failed to process request %s: %s", request_id, exc, exc_info=True)

    def _process_single_request(self, record: Dict[str, Any]) -> None:
        request_id = record.get("id")
        if request_id is None:
            return
        request_payload = record.get("request") or {}
        parameters = request_payload.get("parameters") or {}

        # Debug logging to understand request structure
        logging.info("Processing request ID %s", request_id)
        logging.info("Request payload keys: %s", list(request_payload.keys()))
        logging.info("Parameters: %s", json.dumps(parameters, default=str))

        # Splitting is handled inside LeadOrchestrator discovery only (no DB queue split)

        notify_email = (
            request_payload.get("notify_email")
            or parameters.get("notify_email")
            or self.base_config.notify_email
        )

        request_clone = json.loads(json.dumps(request_payload))
        request_clone["last_attempt_at"] = datetime.utcnow().isoformat()
        self.supabase.update_request_record(request_id, status="processing", request_payload=request_clone)

        args = self._build_args_from_request(request_payload)
        logging.info("Built args - location: %s, state: %s, city: %s", args.location, args.state, args.city)
        run_config = replace(self.base_config)
        run_config.notify_email = notify_email

        orchestrator = LeadOrchestrator(run_config)
        run_result = None

        # Capture logs for this request
        with RequestLogCapture(self.supabase, request_id):
            try:
                run_result = orchestrator.run(args)
                self._mark_request_completed(request_id, request_payload, run_result)
            except Exception as exc:  # pylint: disable=broad-except
                self._mark_request_failed(request_id, request_payload, orchestrator, exc)
                raise

    def _split_and_queue_request(self, record: Dict[str, Any]) -> None:
        """Split a large request into smaller chunks and queue them."""
        try:
            # Lazy import to avoid circular dependencies
            from request_splitter import LLMRequestSplitter, RequestChunk

            request_id = record.get("id")
            request_payload = record.get("request", {})
            parameters = request_payload.get("parameters", {})

            # Initialize splitter (will use environment variable for API key)
            splitter = LLMRequestSplitter(llm_provider="anthropic")

            # Split the request
            chunks = splitter.split_request(request_id, parameters)

            logging.info(
                "Splitting request %s into %d chunks",
                request_id,
                len(chunks)
            )

            # Create sub-requests in the database
            sub_request_ids = []
            for chunk in chunks:
                sub_request_id = self._create_sub_request(record, chunk)
                if sub_request_id:
                    sub_request_ids.append(sub_request_id)

            # Update parent request with split information and mark split_attempted
            # NOTE: Avoid changing workflow_status here to bypass DB constraint (chk_workflow_status)
            request_payload["split_attempted"] = True
            run_logs = {
                "split_time": datetime.utcnow().isoformat(),
                "num_chunks": len(chunks),
                "sub_request_ids": sub_request_ids,
                "reason": f"Quantity {parameters.get('quantity')} exceeds chunk size limit of 10"
            }
            self.supabase.update_request_record(
                request_id,
                request_payload=request_payload,
                run_logs=run_logs,
            )

            if sub_request_ids:
                logging.info(
                    "Successfully split request %s into %d sub-requests",
                    request_id,
                    len(sub_request_ids)
                )
                return
            else:
                logging.warning(
                    "No sub-requests were created for %s; falling back to single-request processing",
                    request_id,
                )
                # Fall back to processing as single request
                self._process_single_request_without_split(record)
                return

        except ImportError as e:
            logging.error(f"Failed to import request splitter: {e}")
            # Fall back to processing as single large request
            logging.warning(
                "Processing request %s as single large request due to missing splitter",
                record.get("id")
            )
            # Continue with regular processing
            self._process_single_request_without_split(record)

        except Exception as e:
            logging.error(f"Failed to split request {record.get('id')}: {e}")
            # Fall back to processing as single large request
            self._process_single_request_without_split(record)

    def _create_sub_request(self, parent_record: Dict[str, Any], chunk) -> Optional[int]:
        """Create a sub-request in the database."""
        try:
            parent_id = parent_record.get("id")
            parent_payload = parent_record.get("request", {})

            # Build sub-request payload
            sub_request = {
                "workflow_status": "pending",
                "request_time": datetime.utcnow().isoformat(),
                "request": {
                    **parent_payload,  # Copy parent request data
                    "parameters": chunk.parameters,  # Use chunk-specific parameters
                    "parent_request_id": parent_id,
                    "chunk_info": {
                        "chunk_id": chunk.chunk_id,
                        "chunk_index": chunk.chunk_index,
                        "total_chunks": chunk.total_chunks,
                        "split_criteria": chunk.split_criteria
                    }
                },
                "notes": f"Sub-request {chunk.chunk_index + 1}/{chunk.total_chunks} for parent {parent_id}"
            }

            # Insert into database
            url = f"{self.supabase.base_url}/rest/v1/enrichment_requests"
            response = _http_request(
                "POST",
                url,
                headers=self.supabase.headers,
                json_body=sub_request,
                timeout=15
            )

            # Supabase may return a list of inserted rows or a single object depending on prefs
            sub_id = None
            if response:
                if isinstance(response, dict):
                    sub_id = response.get("id")
                elif isinstance(response, list) and response:
                    first = response[0]
                    if isinstance(first, dict):
                        sub_id = first.get("id")
            if sub_id:
                logging.info(
                    "Created sub-request %s (chunk %d/%d) for parent %s",
                    sub_id,
                    chunk.chunk_index + 1,
                    chunk.total_chunks,
                    parent_id
                )
                return sub_id

        except Exception as e:
            logging.error(f"Failed to create sub-request: {e}")
            return None

    def _process_single_request_without_split(self, record: Dict[str, Any]) -> None:
        """Process a request without splitting (fallback for when splitting fails)."""
        # This is the original logic without the split check
        request_id = record.get("id")
        if request_id is None:
            return
        request_payload = record.get("request") or {}
        parameters = request_payload.get("parameters") or {}

        notify_email = (
            request_payload.get("notify_email")
            or parameters.get("notify_email")
            or self.base_config.notify_email
        )

        request_clone = json.loads(json.dumps(request_payload))
        request_clone["last_attempt_at"] = datetime.utcnow().isoformat()
        self.supabase.update_request_record(request_id, status="processing", request_payload=request_clone)

        args = self._build_args_from_request(request_payload)
        run_config = replace(self.base_config)
        run_config.notify_email = notify_email

        orchestrator = LeadOrchestrator(run_config)

        with RequestLogCapture(self.supabase, request_id):
            try:
                run_result = orchestrator.run(args)
                self._mark_request_completed(request_id, request_payload, run_result)
            except Exception as exc:
                self._mark_request_failed(request_id, request_payload, orchestrator, exc)
                raise

    def _reset_stale_processing_requests(self, limit: int) -> None:
        """
        Detect requests stuck in 'processing' state beyond the stale threshold and reset them to pending.
        """
        threshold = max(60, self.base_config.request_processing_stale_seconds)
        # Fetch a small batch of candidates and inspect timestamps client-side
        candidates = self.supabase.fetch_processing_requests(limit * 3)
        now_ts = datetime.utcnow().replace(tzinfo=timezone.utc)

        for record in candidates:
            request_id = record.get("id")
            payload = record.get("request") or {}
            if not isinstance(payload, dict):
                payload = {}
            else:
                payload = dict(payload)
            last_attempt = payload.get("last_attempt_at")
            stale = False

            if not last_attempt:
                stale = True
            else:
                try:
                    normalized = last_attempt.rstrip("Z")
                    if normalized and not normalized.endswith("+00:00"):
                        normalized += "+00:00"
                    attempt_ts = datetime.fromisoformat(normalized)
                    if attempt_ts.tzinfo is None:
                        attempt_ts = attempt_ts.replace(tzinfo=timezone.utc)
                    age = (now_ts - attempt_ts).total_seconds()
                    stale = age >= threshold
                except Exception:  # noqa: BLE001
                    stale = True

            if stale and request_id is not None:
                logging.warning(
                    "Re-queueing stale processing request %s (last_attempt_at=%s)",
                    request_id,
                    last_attempt or "none",
                )
                # Remove last_attempt marker so the next run records a fresh timestamp
                payload.pop("last_attempt_at", None)
                self.supabase.update_request_record(
                    request_id,
                    status="pending",
                    request_payload=payload,
                )

    def _mark_request_completed(
        self,
        request_id: int,
        original_request: Dict[str, Any],
        result: Dict[str, Any],
    ) -> None:
        request_clone = json.loads(json.dumps(original_request))
        history = request_clone.get("history", [])
        completed_at = datetime.utcnow().isoformat()
        snapshot = {
            "status": "completed",
            "completed_at": completed_at,
            "run_id": result.get("run_id"),
            "run_directory": result.get("run_directory"),
            "companies_returned": result.get("companies_returned"),
        }
        history.append(snapshot)
        request_clone["history"] = history[-20:]
        request_clone["last_run"] = snapshot
        self.supabase.update_request_record(request_id, status="completed", request_payload=request_clone)

    def _mark_request_failed(
        self,
        request_id: int,
        original_request: Dict[str, Any],
        orchestrator: LeadOrchestrator,
        error: Exception,
    ) -> None:
        request_clone = json.loads(json.dumps(original_request))
        history = request_clone.get("history", [])
        failed_at = datetime.utcnow().isoformat()
        snapshot = {
            "status": "failed",
            "failed_at": failed_at,
            "run_id": orchestrator.last_run_id,
            "run_directory": str(orchestrator.last_run_dir) if orchestrator.last_run_dir else None,
            "error": str(error),
        }
        history.append(snapshot)
        request_clone["history"] = history[-20:]
        request_clone["last_run"] = snapshot
        self.supabase.update_request_record(request_id, status="failed", request_payload=request_clone)

    @staticmethod
    def _build_args_from_request(request: Dict[str, Any]) -> argparse.Namespace:
        parameters = request.get("parameters") or {}

        quantity = parameters.get("quantity") or request.get("quantity") or 10

        state = parameters.get("state")
        if isinstance(state, list):
            state = state[0]

        # Map priority_locations to state if not set
        if not state and parameters.get("priority_locations"):
            locations = parameters["priority_locations"]
            if isinstance(locations, list) and locations:
                first_loc = locations[0]
                # Check if it's a state abbreviation
                if len(first_loc) == 2 and first_loc.upper() in US_STATE_ABBREVIATIONS:
                    state = first_loc.upper()
        if isinstance(state, list):
            state = state[0]
        city = parameters.get("city")
        location = parameters.get("location")
        if not location and parameters.get("priority_locations"):
            location = ", ".join(parameters["priority_locations"])

        pms = parameters.get("pms")
        if not pms:
            include = parameters.get("pms_include") or []
            if isinstance(include, list) and include:
                pms = include[0]

        unit_min = parameters.get("units_min") or parameters.get("unit_min")
        unit_max = parameters.get("units_max") or parameters.get("unit_max")

        requirements = parameters.get("requirements") or parameters.get("notes") or request.get("natural_request")

        exclude = parameters.get("exclude_domains") or parameters.get("suppress_domains") or parameters.get("suppression_domains") or []
        if isinstance(exclude, str):
            exclude = [exclude]

        namespace = argparse.Namespace(
            state=state,
            city=city,
            location=location,
            pms=pms,
            quantity=int(quantity),
            unit_min=int(unit_min) if unit_min not in (None, "") else None,
            unit_max=int(unit_max) if unit_max not in (None, "") else None,
            requirements=requirements,
            exclude=exclude if isinstance(exclude, list) else [],
            max_rounds=None,
            log_level=os.getenv("LOG_LEVEL", "INFO"),
            output=None,
        )
        return namespace


# ---------------------------------------------------------------------------
# CLI
# ---------------------------------------------------------------------------

def build_arg_parser() -> argparse.ArgumentParser:
    parser = argparse.ArgumentParser(description="Lead list orchestration pipeline")
    parser.add_argument("--state", help="State abbreviation (e.g., KS)")
    parser.add_argument("--city", help="Optional city filter")
    parser.add_argument("--location", help="Free-form location text to send to discovery webhook")
    parser.add_argument("--pms", help="PMS/platform requirement")
    parser.add_argument("--quantity", type=int, help="Desired number of companies")
    parser.add_argument("--unit-min", dest="unit_min", type=int, help="Minimum unit count filter")
    parser.add_argument("--unit-max", dest="unit_max", type=int, help="Maximum unit count filter")
    parser.add_argument("--requirements", help="Additional requirements text for discovery")
    parser.add_argument(
        "--exclude",
        nargs="*",
        default=[],
        help="Additional suppression domains to exclude (space separated)",
    )
    parser.add_argument("--max-rounds", type=int, help="Override discovery max rounds")
    parser.add_argument(
        "--log-level",
        default=os.getenv("LOG_LEVEL", "INFO"),
        choices=["DEBUG", "INFO", "WARNING", "ERROR"],
        help="Logging verbosity",
    )
    parser.add_argument(
        "--output",
        help="Optional path to write JSON results (defaults to stdout only)",
    )
    parser.add_argument(
        "--process-request-queue",
        action="store_true",
        help="Process pending enrichment requests from Supabase and exit",
    )
    parser.add_argument(
        "--request-limit",
        type=int,
        default=1,
        help="Maximum number of pending requests to process when --process-request-queue is set",
    )
    return parser


def main(argv: Optional[List[str]] = None) -> int:
    _load_env_file()
    parser = build_arg_parser()
    args = parser.parse_args(argv)

    logging.basicConfig(
        level=getattr(logging, args.log_level.upper(), logging.INFO),
        format="%(asctime)s | %(levelname)s | %(message)s",
    )

    if args.process_request_queue:
        config = Config()
        processor = EnrichmentRequestProcessor(config)
        processor.process(limit=max(1, args.request_limit))
        return 0

    if args.quantity is None:
        parser.error("--quantity is required unless --process-request-queue is specified")

    try:
        config = Config()
        orchestrator = LeadOrchestrator(config)
        result = orchestrator.run(args)
    except Exception as exc:  # pylint: disable=broad-except
        logging.error("Fatal error: %s", exc)
        return 1

    output_json = json.dumps(result, indent=2)
    print(output_json)

    if args.output:
        with open(args.output, "w", encoding="utf-8") as handle:
            handle.write(output_json)
        logging.info("Wrote results to %s", args.output)

    return 0


if __name__ == "__main__":
    sys.exit(main())
