import types
import time
import os
import sys
from typing import List, Dict

import pytest

sys.path.insert(0, os.path.abspath(os.path.join(os.path.dirname(__file__), '..')))
from lead_pipeline import LeadOrchestrator, Config
from tests.conftest import fake_company, fake_enriched


def test_orchestrator_happy_path(monkeypatch):
    cfg = Config()

    orch = LeadOrchestrator(cfg)

    # Fake Supabase
    monkeypatch.setattr(
        orch.supabase,
        "find_existing_companies",
        lambda **kwargs: [fake_company(i) for i in range(3)],
    )

    # No HubSpot suppression during test
    monkeypatch.setattr(orch, "_apply_suppression", lambda xs: xs)

    # Fake discovery returns a batch of companies; vary on requirements
    def fake_discover(**kwargs) -> List[Dict]:
        req = (kwargs.get("extra_requirements") or "")
        base = [fake_company(100 + i, name_prefix="Disc") for i in range(12)]
        if "Focus on" in req:
            # Simulate different area by different name prefix
            base = [fake_company(200 + i, name_prefix="Area") for i in range(12)]
        return base

    orch.discovery = types.SimpleNamespace(discover=fake_discover)

    # Fake enrichment: wrap into enriched results
    monkeypatch.setattr(
        orch,
        "_enrich_companies_resilient",
        lambda comps, run_dir: [fake_enriched(c) for c in comps],
    )

    # Build args
    args = types.SimpleNamespace(
        state="IL",
        city=None,
        location="IL",
        pms="AppFolio",
        quantity=20,
        unit_min=None,
        unit_max=None,
        requirements="",
        exclude=[],
        max_rounds=1,
        log_level="INFO",
        output=None,
    )

    result = orch.run(args)
    deliverable = result.get("companies") or result.get("results") or []
    assert len(deliverable) >= 20
    # Each enriched item has required company fields
    for item in deliverable:
        c = item.get("company", {})
        assert c.get("company_name")
        assert c.get("domain") or c.get("company_url")


def test_parallel_chunking_limit(monkeypatch):
    cfg = Config()
    orch = LeadOrchestrator(cfg)

    # Supabase returns none to force discovery
    monkeypatch.setattr(orch.supabase, "find_existing_companies", lambda **_: [])
    monkeypatch.setattr(orch, "_apply_suppression", lambda xs: xs)

    # Force a chunk plan by monkeypatching splitter
    import request_splitter as rs

    class DummyChunk:
        def __init__(self, ix):
            self.parameters = {"requirements_suffix": f"Area {ix}"}

    monkeypatch.setattr(rs, "LLMRequestSplitter", lambda llm_provider=None: types.SimpleNamespace(split_request=lambda *_: [DummyChunk(i) for i in range(5)]))

    # Track concurrency
    max_inflight = {"n": 0}
    inflight = {"n": 0}

    def fake_discover(**kwargs):
        inflight["n"] += 1
        max_inflight["n"] = max(max_inflight["n"], inflight["n"])
        time.sleep(0.05)
        inflight["n"] -= 1
        # Make domains unique per chunk by offsetting index based on requirements_suffix
        req = (kwargs.get("extra_requirements") or "")
        area_ix = 0
        if "Area" in req:
            try:
                area_ix = int(req.split("Area")[-1].strip().split()[0])
            except Exception:
                area_ix = 0
        offset = area_ix * 1000
        return [fake_company(offset + i) for i in range(10)]

    orch.discovery = types.SimpleNamespace(discover=fake_discover)
    monkeypatch.setattr(orch, "_enrich_companies_resilient", lambda comps, run_dir: [fake_enriched(c) for c in comps])

    args = types.SimpleNamespace(
        state="IL",
        city=None,
        location="IL",
        pms=None,
        quantity=20,
        unit_min=None,
        unit_max=None,
        requirements="",
        exclude=[],
        max_rounds=1,
        log_level="INFO",
        output=None,
    )

    orch.run(args)
    # Should not exceed 3 concurrent discovery calls
    assert max_inflight["n"] <= 3
