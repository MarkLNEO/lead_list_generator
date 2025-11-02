import os
import sys
import types
import pytest


@pytest.fixture(autouse=True)
def _test_env(monkeypatch):
    # Ensure project root on sys.path for imports
    root = os.path.abspath(os.path.join(os.path.dirname(__file__), os.pardir))
    if root not in sys.path:
        sys.path.insert(0, root)
    # Keep tests offline except OpenAI if explicitly enabled
    monkeypatch.setenv("SKIP_HEALTH_CHECK", "true")
    monkeypatch.setenv("CIRCUIT_BREAKER_ENABLED", "false")
    # Make discovery/enrichment timeouts small
    monkeypatch.setenv("DISCOVERY_REQUEST_TIMEOUT", "60")
    monkeypatch.setenv("COMPANY_ENRICHMENT_REQUEST_TIMEOUT", "60")
    monkeypatch.setenv("CONTACT_ENRICHMENT_REQUEST_TIMEOUT", "60")
    # Use tiny buffer limits
    monkeypatch.setenv("MAX_COMPANIES_PER_RUN", "200")
    # Default: disable QA unless explicitly enabled for a test
    if not os.getenv("QA_VALIDATOR_ENABLED"):
        monkeypatch.setenv("QA_VALIDATOR_ENABLED", "false")

    # Satisfy config validation with dummy values
    monkeypatch.setenv("SUPABASE_ANON_KEY", "test_anon_key")
    monkeypatch.setenv("HUBSPOT_PRIVATE_APP_TOKEN", "test_hubspot_token")
    monkeypatch.setenv("N8N_COMPANY_DISCOVERY_WEBHOOK", "http://localhost/discovery")
    monkeypatch.setenv("N8N_COMPANY_ENRICHMENT_WEBHOOK", "http://localhost/enrich_company")


def fake_company(i, name_prefix="Co"):
    return {
        "company_name": f"{name_prefix}{i}",
        "domain": f"example{i}.com",
        "company_url": f"https://example{i}.com",
        "hq_state": "IL",
        "hq_city": "Chicago",
        "pms": "AppFolio",
    }


def fake_enriched(company):
    return {
        "company": company,
        "contacts": [
            {
                "name": "Alex Doe",
                "email": f"alex@{company['domain']}",
                "personal_anecdotes": ["Volunteers locally"],
                "professional_anecdotes": ["10 years in PM"],
            }
        ],
    }
