import os
import sys
import types

import pytest

sys.path.insert(0, os.path.abspath(os.path.join(os.path.dirname(__file__), '..')))
from lead_pipeline import Config, SupabaseResearchClient


class DummySupabase(SupabaseResearchClient):
    def __init__(self, cfg):
        super().__init__(cfg)
        self.last_upsert = None

    def _upsert(self, table, filters, payload):  # noqa: D401
        self.last_upsert = {"table": table, "filters": filters, "payload": payload}
        return payload


@pytest.fixture
def supabase_client(monkeypatch):
    cfg = Config()
    client = DummySupabase(cfg)
    return client


def test_persist_company_sanitizes_unit_count(supabase_client):
    original = {"domain": "example.com"}
    enriched = {"domain": "example.com", "unit_count": "≈6", "company_name": "Example PM"}
    supabase_client.persist_company(original, enriched, contact_count=1)
    payload = supabase_client.last_upsert["payload"]
    assert payload["unit_count"] == 6


def test_persist_company_removes_non_numeric_units(supabase_client):
    original = {"domain": "demo.com"}
    enriched = {"domain": "demo.com", "unit_count": "N/A", "company_name": "Demo PM"}
    supabase_client.persist_company(original, enriched, contact_count=0)
    payload = supabase_client.last_upsert["payload"]
    assert "unit_count" not in payload

