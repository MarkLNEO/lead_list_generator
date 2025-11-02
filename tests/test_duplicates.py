import types
import os
import sys
import pytest

sys.path.insert(0, os.path.abspath(os.path.join(os.path.dirname(__file__), '..')))
from lead_pipeline import LeadOrchestrator, Config


def test_merge_companies_dedupes(monkeypatch):
    cfg = Config()
    orch = LeadOrchestrator(cfg)

    existing = [
        {"company_name": "Alpha", "domain": "alpha.com"},
    ]
    new = [
        {"company_name": "Alpha LLC", "domain": "alpha.com"},  # duplicate
        {"company_name": "Beta", "domain": "beta.com"},        # unique
        {"company_name": "", "domain": "beta.com"},            # duplicate domain ignored
    ]

    merged = orch._merge_companies(existing, new)
    domains = {orch._domain_key(c) for c in merged}
    assert domains == {"alpha.com", "beta.com"}
    assert len(merged) == 2


def test_run_fails_when_only_duplicates(monkeypatch):
    cfg = Config()
    orch = LeadOrchestrator(cfg)

    # Force no Supabase candidates
    monkeypatch.setattr(orch.supabase, "find_existing_companies", lambda **_: [])
    monkeypatch.setattr(orch, "_apply_suppression", lambda xs: xs)

    # Discovery always returns the same domain, creating duplicates
    def dup_discover(**kwargs):
        return [{"company_name": f"Dup{i}", "domain": "dupe.com"} for i in range(10)]

    orch.discovery = types.SimpleNamespace(discover=dup_discover)
    monkeypatch.setattr(orch, "_enrich_companies_resilient", lambda comps, run_dir: [{"company": c, "contacts": []} for c in comps])

    args = types.SimpleNamespace(
        state="IL",
        city=None,
        location="IL",
        pms=None,
        quantity=15,  # requires 15 unique; we'll only return duplicates
        unit_min=None,
        unit_max=None,
        requirements="",
        exclude=[],
        max_rounds=1,
        log_level="INFO",
        output=None,
    )

    with pytest.raises(ValueError) as exc:
        orch.run(args)
    assert "Final gate failed" in str(exc.value)

