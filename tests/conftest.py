"""
Shared pytest fixtures and configuration for Lead Pipeline tests.

This module provides common test fixtures, mock utilities, and helper functions
used across all test files.
"""

import json
import os
import tempfile
from pathlib import Path
from typing import Dict, Any, List, Optional
from unittest.mock import MagicMock

import pytest

from lead_pipeline import (
    Config,
    CircuitBreaker,
    StateManager,
    ContactDeduplicator,
    HealthCheck,
    LeadOrchestrator,
    SupabaseResearchClient,
    HubSpotClient,
    DiscoveryWebhookClient,
    N8NEnrichmentClient,
)


# ============================================================================
# Configuration Fixtures
# ============================================================================

@pytest.fixture
def base_config() -> Config:
    """Minimal valid configuration for testing."""
    return Config(
        supabase_key="test_supabase_key",
        hubspot_token="test_hubspot_token",
        discovery_webhook_url="http://test.example.com/discovery",
        company_enrichment_webhook="http://test.example.com/company",
        contact_enrichment_webhook="http://test.example.com/contact",
        email_verification_webhook="http://test.example.com/verify",
        contact_discovery_webhook="http://test.example.com/contact-discovery",
    )


@pytest.fixture
def config_with_circuit_breakers(base_config: Config) -> Config:
    """Configuration with circuit breakers enabled."""
    base_config.circuit_breaker_enabled = True
    base_config.circuit_breaker_threshold = 3
    base_config.circuit_breaker_timeout = 60.0
    return base_config


@pytest.fixture
def config_with_quality_gates(base_config: Config) -> Config:
    """Configuration with strict quality requirements."""
    base_config.contact_min_personal_anecdotes = 3
    base_config.contact_min_professional_anecdotes = 3
    base_config.contact_min_total_anecdotes = 6
    base_config.contact_allow_personalization_fallback = False
    base_config.contact_allow_seed_url_fallback = False
    return base_config


# ============================================================================
# Temporary Directory Fixtures
# ============================================================================

@pytest.fixture
def temp_run_dir(tmp_path: Path) -> Path:
    """Create a temporary run directory for testing."""
    run_dir = tmp_path / "runs" / "test_run_123"
    run_dir.mkdir(parents=True, exist_ok=True)
    return run_dir


@pytest.fixture
def temp_state_file(temp_run_dir: Path) -> Path:
    """Create a temporary state file for testing."""
    state_file = temp_run_dir / "state.json"
    return state_file


# ============================================================================
# Client Fixtures
# ============================================================================

@pytest.fixture
def circuit_breaker() -> CircuitBreaker:
    """Create a circuit breaker for testing."""
    return CircuitBreaker(
        name="test_breaker",
        failure_threshold=3,
        recovery_timeout=60.0,
    )


@pytest.fixture
def state_manager(temp_run_dir: Path) -> StateManager:
    """Create a state manager for testing."""
    return StateManager(temp_run_dir)


@pytest.fixture
def contact_deduplicator() -> ContactDeduplicator:
    """Create a contact deduplicator for testing."""
    return ContactDeduplicator()


@pytest.fixture
def health_check(base_config: Config) -> HealthCheck:
    """Create a health check instance for testing."""
    return HealthCheck(base_config)


@pytest.fixture
def supabase_client(base_config: Config) -> SupabaseResearchClient:
    """Create a Supabase client for testing."""
    return SupabaseResearchClient(base_config)


@pytest.fixture
def hubspot_client(base_config: Config) -> HubSpotClient:
    """Create a HubSpot client for testing."""
    return HubSpotClient(base_config)


@pytest.fixture
def discovery_client(base_config: Config) -> DiscoveryWebhookClient:
    """Create a Discovery webhook client for testing."""
    return DiscoveryWebhookClient(
        base_config.discovery_webhook_url,
        base_config.discovery_request_timeout,
    )


@pytest.fixture
def enrichment_client(base_config: Config) -> N8NEnrichmentClient:
    """Create an enrichment client for testing."""
    return N8NEnrichmentClient(base_config)


@pytest.fixture
def orchestrator(base_config: Config) -> LeadOrchestrator:
    """Create a lead orchestrator for testing."""
    return LeadOrchestrator(base_config)


# ============================================================================
# Test Data Fixtures
# ============================================================================

@pytest.fixture
def sample_company() -> Dict[str, Any]:
    """Sample company data for testing."""
    return {
        "company_name": "Acme Property Management",
        "domain": "acmepm.com",
        "hq_state": "CA",
        "hq_city": "San Francisco",
        "unit_count_numeric": 250,
        "pms": "AppFolio",
        "website": "https://acmepm.com",
    }


@pytest.fixture
def sample_companies() -> List[Dict[str, Any]]:
    """Multiple sample companies for testing."""
    return [
        {
            "company_name": "Acme Property Management",
            "domain": "acmepm.com",
            "hq_state": "CA",
            "hq_city": "San Francisco",
            "unit_count_numeric": 250,
        },
        {
            "company_name": "Beta Realty Group",
            "domain": "betarealty.com",
            "hq_state": "TX",
            "hq_city": "Austin",
            "unit_count_numeric": 150,
        },
        {
            "company_name": "Gamma Property Services",
            "domain": "gammapropertyservices.com",
            "hq_state": "FL",
            "hq_city": "Miami",
            "unit_count_numeric": 300,
        },
    ]


@pytest.fixture
def sample_contact() -> Dict[str, Any]:
    """Sample contact data for testing."""
    return {
        "full_name": "John Smith",
        "first_name": "John",
        "last_name": "Smith",
        "email": "john.smith@acmepm.com",
        "job_title": "Property Manager",
        "linkedin_url": "https://linkedin.com/in/johnsmith",
        "personal_anecdotes": [
            "Volunteer at local animal shelter",
            "Marathon runner with 5 completed races",
            "Board member of neighborhood HOA",
        ],
        "professional_anecdotes": [
            "Manages portfolio of 250+ units",
            "Implemented new tenant screening process",
            "Reduced vacancy rate by 15%",
        ],
        "seed_urls": [
            "https://acmepm.com/team",
            "https://linkedin.com/in/johnsmith",
        ],
    }


@pytest.fixture
def sample_enriched_company() -> Dict[str, Any]:
    """Sample enriched company with decision makers."""
    return {
        "company_name": "Acme Property Management",
        "domain": "acmepm.com",
        "hq_state": "CA",
        "hq_city": "San Francisco",
        "unit_count_numeric": 250,
        "decision_makers": [
            {
                "full_name": "John Smith",
                "email": "john.smith@acmepm.com",
                "job_title": "Property Manager",
            },
            {
                "full_name": "Jane Doe",
                "email": "jane.doe@acmepm.com",
                "job_title": "Director of Operations",
            },
        ],
        "icp_score": 85,
        "employee_count": 25,
    }


@pytest.fixture
def sample_hubspot_response() -> Dict[str, Any]:
    """Sample HubSpot API response."""
    return {
        "results": [
            {
                "id": "12345",
                "properties": {
                    "domain": "acmepm.com",
                    "name": "Acme Property Management",
                    "hs_lastmodifieddate": "2025-10-15T12:00:00Z",
                },
            }
        ],
        "total": 1,
    }


@pytest.fixture
def sample_discovery_response() -> List[Dict[str, Any]]:
    """Sample discovery webhook response."""
    return [
        {
            "company_name": "Delta Properties LLC",
            "domain": "deltaproperties.com",
            "hq_state": "NY",
            "hq_city": "New York",
            "unit_count": 400,
        },
        {
            "company_name": "Epsilon Management Co",
            "domain": "epsilonmgmt.com",
            "hq_state": "IL",
            "hq_city": "Chicago",
            "unit_count": 175,
        },
    ]


# ============================================================================
# Mock Helpers
# ============================================================================

@pytest.fixture
def mock_http_success(monkeypatch):
    """Mock successful HTTP requests."""
    def fake_request(*args, **kwargs):
        return {"status": "success", "data": []}

    monkeypatch.setattr("lead_pipeline._http_request", fake_request)
    return fake_request


@pytest.fixture
def mock_http_failure(monkeypatch):
    """Mock failing HTTP requests."""
    from urllib.error import HTTPError

    def fake_request(*args, **kwargs):
        raise HTTPError("http://test.com", 500, "Server Error", {}, None)

    monkeypatch.setattr("lead_pipeline._http_request", fake_request)
    return fake_request


@pytest.fixture
def mock_http_timeout(monkeypatch):
    """Mock HTTP timeout."""
    from urllib.error import URLError

    def fake_request(*args, **kwargs):
        raise URLError("timeout")

    monkeypatch.setattr("lead_pipeline._http_request", fake_request)
    return fake_request


# ============================================================================
# Environment Variable Helpers
# ============================================================================

@pytest.fixture
def clean_env(monkeypatch):
    """Clean environment variables for testing."""
    # Remove all env vars that might affect tests
    env_vars = [
        "SUPABASE_URL",
        "SUPABASE_SERVICE_KEY",
        "HUBSPOT_PRIVATE_APP_TOKEN",
        "N8N_COMPANY_DISCOVERY_WEBHOOK",
        "N8N_COMPANY_ENRICHMENT_WEBHOOK",
        "N8N_CONTACT_ENRICH_WEBHOOK",
        "N8N_EMAIL_DISCOVERY_VERIFY",
        "CIRCUIT_BREAKER_ENABLED",
    ]
    for var in env_vars:
        monkeypatch.delenv(var, raising=False)


# ============================================================================
# Fixture Loaders
# ============================================================================

def load_fixture_json(filename: str) -> Any:
    """Load JSON fixture from tests/fixtures/ directory."""
    fixture_path = Path(__file__).parent / "fixtures" / filename
    with open(fixture_path, "r") as f:
        return json.load(f)


@pytest.fixture
def fixture_loader():
    """Provide fixture loading utility."""
    return load_fixture_json
