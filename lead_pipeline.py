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
from dataclasses import dataclass, field, replace
from datetime import datetime, timezone
from typing import Any, Dict, Iterable, List, Optional, Set, Tuple
from pathlib import Path
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

                content_type = resp.headers.get("Content-Type", "") or ""
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
                wait_for = _retry_after_delay(exc) or retry_backoff * attempt
                logging.warning("HTTP 429 from %s; retrying in %.1fs (attempt %d/%d)", url, wait_for, attempt, max_retries)
                time.sleep(wait_for)
                continue
            if exc.code >= 500 and attempt < max_retries:
                wait_for = retry_backoff * attempt
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
                wait_for = retry_backoff * attempt
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

        linkedin = (contact.get("linkedin") or contact.get("linkedin_url") or "").strip().lower()
        if linkedin:
            return f"linkedin:{linkedin}"

        name = (contact.get("full_name") or contact.get("name") or "").strip().lower()
        company_name = (company.get("company_name") or company.get("domain") or "").strip().lower()
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
    """
    if not path:
        return
    if not os.path.exists(path):
        return

    try:
        with open(path, "r", encoding="utf-8") as handle:
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
    except Exception as exc:  # pylint: disable=broad-except
        print(f"Warning: failed to load environment file {path}: {exc}", file=sys.stderr)


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
        default_factory=lambda: float(os.getenv("DISCOVERY_REQUEST_TIMEOUT", "1800"))  # 30 minutes
    )
    company_enrichment_timeout: float = field(
        default_factory=lambda: float(os.getenv("COMPANY_ENRICHMENT_REQUEST_TIMEOUT", "7200"))  # 2 hours
    )
    contact_enrichment_timeout: float = field(
        default_factory=lambda: float(os.getenv("CONTACT_ENRICHMENT_REQUEST_TIMEOUT", "7200"))  # 2 hours
    )
    email_verification_timeout: float = field(
        default_factory=lambda: float(os.getenv("EMAIL_VERIFICATION_REQUEST_TIMEOUT", "240"))
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
    email_verification_attempts: int = field(default_factory=lambda: int(os.getenv("EMAIL_VERIFICATION_ATTEMPTS", "3")))
    email_verification_delay: float = field(default_factory=lambda: float(os.getenv("EMAIL_VERIFICATION_DELAY", "2.5")))
    
    # Circuit breaker settings
    circuit_breaker_enabled: bool = field(default_factory=lambda: os.getenv("CIRCUIT_BREAKER_ENABLED", "true").lower() == "true")
    circuit_breaker_threshold: int = field(default_factory=lambda: int(os.getenv("CIRCUIT_BREAKER_THRESHOLD", "5")))
    circuit_breaker_timeout: float = field(default_factory=lambda: float(os.getenv("CIRCUIT_BREAKER_TIMEOUT", "300")))
    
    # Safety limits
    max_companies_per_run: int = field(default_factory=lambda: int(os.getenv("MAX_COMPANIES_PER_RUN", "500")))
    max_contacts_per_company: int = field(default_factory=lambda: int(os.getenv("MAX_CONTACTS_PER_COMPANY", "10")))
    max_enrichment_retries: int = field(default_factory=lambda: int(os.getenv("MAX_ENRICHMENT_RETRIES", "2")))

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
            filters: List[str] = ["select=*"]
            if state and self.state_column:
                filters.append(f"{self.state_column}=eq.{state}")
            if pms:
                filters.append(f"pms=ilike.*{pms}*")
            if city and self.city_column:
                filters.append(f"{self.city_column}=ilike.*{city}*")
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
        statuses = ["pending", "queued"]
        status_filter = ",".join(statuses)
        url = (
            f"{self.base_url}/rest/v1/enrichment_requests"
            f"?select=*&request_status=in.({status_filter})&order=request_time.asc&limit={limit}"
        )
        try:
            response = _http_request("GET", url, headers=self.headers, timeout=15)
            if isinstance(response, list):
                return response
        except Exception as exc:  # noqa: BLE001
            logging.warning("Failed to fetch pending requests: %s", exc)
        return []

    def update_request_record(
        self,
        request_id: int,
        *,
        status: Optional[str] = None,
        request_payload: Optional[Dict[str, Any]] = None,
    ) -> None:
        payload: Dict[str, Any] = {}
        if status is not None:
            payload["request_status"] = status
        if request_payload is not None:
            payload["request"] = request_payload
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
            "unit_count_numeric": enriched.get("unit_count"),
            "employee_count": enriched.get("employee_count"),
            "agent_summary": enriched.get("agent_summary"),
            "contact_count": contact_count,
            "contacts_found": contact_count,
            "updated_at": datetime.utcnow().isoformat(),
        }
        unit_val = payload.get("unit_count")
        if isinstance(unit_val, str):
            stripped = unit_val.replace(",", "").strip()
            if stripped.isdigit():
                payload["unit_count_numeric"] = int(stripped)
        elif isinstance(unit_val, (int, float)):
            payload["unit_count_numeric"] = int(unit_val)
        payload = {k: v for k, v in payload.items() if v not in (None, "", [], {})}
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
            "full_name": full_name or None,
            "email": email,
            "email_verified": bool(contact.get("email_verified")),
            "job_title": contact.get("title"),
            "linkedin_url": contact.get("linkedin"),
            "is_decision_maker": True,
            "research_status": "enriched",
            "outreach_status": "not_started",
            "personalization_notes": contact.get("personalization"),
            "agent_summary": contact.get("agent_summary"),
            "updated_at": datetime.utcnow().isoformat(),
        }
        payload = {k: v for k, v in payload.items() if v not in (None, "", [])}
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

        existing = self.search_company_by_domain(domain)
        if not existing:
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
            return False

        hubspot_id = existing.get("id")
        if hubspot_id and self.has_recent_activity(hubspot_id):
            logging.info("Suppressing %s (%s) due to recent activity", company.get("company_name"), domain)
            return False

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
        logging.info("Calling discovery webhook (attempt %d) for %d companies", attempt, quantity)
        try:
            result = _http_request("POST", self.url, json_body=payload, timeout=self.timeout)
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
        self.supabase = SupabaseResearchClient(config)
        self.hubspot = HubSpotClient(config)
        self.discovery = DiscoveryWebhookClient(config.discovery_webhook_url, config.discovery_request_timeout)
        self.enrichment = N8NEnrichmentClient(config)
        self.current_target_quantity = 0
        
        # Production features
        self.deduplicator = ContactDeduplicator()
        self.state_manager: Optional[StateManager] = None
        self.last_run_id: Optional[str] = None
        self.last_run_dir: Optional[Path] = None
        self.metrics = {
            "start_time": time.time(),
            "companies_discovered": 0,
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
    
    def _checkpoint_if_needed(self, state: Dict[str, Any]) -> None:
        """Save checkpoint if interval elapsed."""
        if self.state_manager and self.state_manager.should_checkpoint():
            state["metrics"] = self.metrics
            self.state_manager.save_checkpoint(state)

    def run(self, args: argparse.Namespace) -> Dict[str, Any]:
        """Execute pipeline with production resilience and recovery."""
        run_id = datetime.now(timezone.utc).strftime("%Y%m%d_%H%M%S") + "_" + uuid.uuid4().hex[:6]
        self.last_run_id = run_id
        run_dir = Path.cwd() / "runs" / run_id
        self.last_run_dir = run_dir
        run_dir.mkdir(parents=True, exist_ok=True)
        
        # Initialize state management
        self.state_manager = StateManager(run_dir)
        
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
            
            # Checkpoint after Supabase
            self._checkpoint_if_needed({
                "phase": "supabase_loaded",
                "companies_count": len(companies),
                "companies": companies[:10],  # Sample
            })
            
            # Phase 2: Discovery rounds
            logging.info("=== Phase 2: Discovery rounds (need %d more) ===", max(0, buffer_quantity - len(companies)))
            attempt = 0
            while len(companies) < buffer_quantity and attempt < max_rounds:
                attempt += 1
                remaining = buffer_quantity - len(companies)
                
                try:
                    suppression_domains = {self._domain_key(c) for c in companies}
                    suppression_domains.update(args.exclude or [])

                    if self.circuit_breakers.get("discovery"):
                        discovered = self.circuit_breakers["discovery"].call(
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
                        discovered = self.discovery.discover(
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
                    
                    self._track_api_call("discovery", success=True)
                    logging.info("Discovery round %d: %d companies found", attempt, len(discovered))
                    self.metrics["companies_discovered"] += len(discovered)
                    
                    filtered = self._apply_suppression(discovered)
                    logging.info("Discovery round %d: %d companies after suppression", attempt, len(filtered))
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
            results = self._enrich_companies_resilient(trimmed, run_dir)
            deliverable = results[:target_quantity]
            
            logging.info("Enriched companies: %d (target: %d)", len(deliverable), target_quantity)
            
            # Phase 4: Top-up if needed
            missing = target_quantity - len(deliverable)
            if missing > 0:
                logging.info("=== Phase 4: Top-up round (need %d more) ===", missing)
                deliverable = self._topup_results(deliverable, missing, args, run_dir)
            
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
        return self._apply_suppression(supabase_companies)

    def _apply_suppression(self, companies: List[Dict[str, Any]]) -> List[Dict[str, Any]]:
        if not companies:
            return []
        return self.hubspot.filter_companies(companies)

    def _merge_companies(
        self,
        existing: List[Dict[str, Any]],
        new_companies: List[Dict[str, Any]],
    ) -> List[Dict[str, Any]]:
        known_domains: Set[str] = {self._domain_key(company) for company in existing}
        merged = list(existing)
        for company in new_companies:
            domain = self._domain_key(company)
            if domain and domain in known_domains:
                continue
            known_domains.add(domain)
            merged.append(company)
        return merged

    @staticmethod
    def _domain_key(company: Dict[str, Any]) -> str:
        return (company.get("domain") or "").strip().lower()

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

        # Round 1: Decision makers from enrichment
        while attempts_left > 0 and len(verified_contacts) < max_contacts:
            attempts_left -= 1
            
            if attempts_left == 1:
                # First attempt: use decision makers from enrichment
                decision_makers = list(enriched_company.get("decision_makers", [])) or []
                logging.info("Enrichment returned %d decision makers for %s", 
                           len(decision_makers), enriched_company.get("company_name"))
                if not decision_makers:
                    # Fall back to contact discovery
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
                # Second attempt: contact discovery
                logging.info("Round 2: Calling contact discovery webhook for %s", 
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

            if not decision_makers:
                continue

            # Process contacts concurrently
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
        extra_rounds = 0
        max_extra = max(2, int(os.getenv("TOPUP_MAX_ROUNDS", "3")))
        
        while missing > 0 and extra_rounds < max_extra:
            extra_rounds += 1
            logging.info("Top-up round %d: need %d more companies", extra_rounds, missing)
            buffer_target, buffer_multiplier = self._calculate_buffer_target(missing)
            logging.info(
                "Top-up discovery target: %d (multiplier: %.2fx)",
                buffer_target,
                buffer_multiplier,
            )
            
            suppression_domains = tried_domains.copy()
            try:
                discovered = self.discovery.discover(
                    location=args.location,
                    state=args.state,
                    pms=args.pms,
                    quantity=buffer_target,
                    unit_count_min=args.unit_min,
                    unit_count_max=args.unit_max,
                    suppression_domains=suppression_domains,
                    extra_requirements=args.requirements,
                    attempt=self.config.discovery_max_rounds + extra_rounds,
                )
                
                new_filtered = self._apply_suppression(discovered)
                for c in new_filtered:
                    tried_domains.add(self._domain_key(c))
                
                if not new_filtered:
                    # Widen search (drop city constraint)
                    logging.info("Widening search: dropping city constraint")
                    discovered2 = self.discovery.discover(
                        location=None,
                        state=args.state,
                        pms=args.pms,
                        quantity=buffer_target,
                        unit_count_min=args.unit_min,
                        unit_count_max=args.unit_max,
                        suppression_domains=tried_domains,
                        extra_requirements=args.requirements,
                        attempt=self.config.discovery_max_rounds + extra_rounds,
                    )
                    new_filtered = self._apply_suppression(discovered2)
                    for c in new_filtered:
                        tried_domains.add(self._domain_key(c))

                if not new_filtered:
                    logging.warning("No new companies found in top-up round %d", extra_rounds)
                    break
                
                enrich_candidates = new_filtered[:missing]
                topup_results = self._enrich_companies_resilient(enrich_candidates, run_dir)
                
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
        run_result = None
        try:
            run_result = orchestrator.run(args)
            self._mark_request_completed(request_id, request_payload, run_result)
        except Exception as exc:  # pylint: disable=broad-except
            self._mark_request_failed(request_id, request_payload, orchestrator, exc)
            raise

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
