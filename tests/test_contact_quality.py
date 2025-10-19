"""
Tests for contact quality evaluation logic.

Tests the quality gates that determine whether a contact has sufficient
anecdotes and personalization data.
"""

import pytest

from lead_pipeline import Config, evaluate_contact_quality


def make_config(**overrides):
    base = {
        "supabase_key": "supabase",
        "hubspot_token": "hubspot",
        "discovery_webhook_url": "http://example.com/discovery",
        "company_enrichment_webhook": "http://example.com/company",
        "contact_enrichment_webhook": "http://example.com/contact",
        "email_verification_webhook": "http://example.com/verify",
    }
    base.update(overrides)
    return Config(**base)


@pytest.mark.unit
def test_quality_passes_when_thresholds_met():
    config = make_config(
        contact_min_personal_anecdotes=1,
        contact_min_professional_anecdotes=1,
        contact_min_total_anecdotes=2,
    )
    contact = {
        "personal_anecdotes": ["Personal story"],
        "professional_anecdotes": ["Professional win"],
    }

    passed, stats = evaluate_contact_quality(contact, config)

    assert passed
    assert stats["reason"] == "thresholds_met"
    assert stats["personal"] == 1
    assert stats["professional"] == 1
    assert stats["total"] == 2


@pytest.mark.unit
def test_quality_allows_min_total_when_categories_unset():
    config = make_config(
        contact_min_personal_anecdotes=0,
        contact_min_professional_anecdotes=0,
        contact_min_total_anecdotes=1,
    )
    contact = {
        "professional_anecdotes": ["Only professional data"],
    }

    passed, stats = evaluate_contact_quality(contact, config)

    assert passed
    # Either thresholds_met or total_minimum depending on normalization
    assert stats["total"] == 1
    assert stats["reason"] in {"thresholds_met", "total_minimum"}


@pytest.mark.unit
def test_quality_allows_personalization_fallback():
    config = make_config(
        contact_min_personal_anecdotes=2,
        contact_min_professional_anecdotes=2,
        contact_min_total_anecdotes=5,
        contact_allow_personalization_fallback=True,
    )
    contact = {
        "personalization": "Congrats on the new development in Austin!",
    }

    passed, stats = evaluate_contact_quality(contact, config)

    assert passed
    assert stats["reason"] == "personalization_fallback"
    assert stats["has_personalization"] is True


@pytest.mark.unit
def test_quality_allows_seed_url_dict_entries():
    config = make_config(
        contact_min_personal_anecdotes=1,
        contact_min_professional_anecdotes=1,
        contact_min_total_anecdotes=3,
    )
    contact = {
        "seed_urls": [{"url": "https://example.com/profile"}],
        "personal_anecdotes": [],
        "professional_anecdotes": [],
    }

    passed, stats = evaluate_contact_quality(contact, config)

    assert passed
    assert stats["reason"] == "seed_url_fallback"
    assert stats["seed_urls"] == 1


@pytest.mark.unit
@pytest.mark.parametrize(
    "overrides",
    [
        {},
        {"contact_allow_personalization_fallback": False},
        {"contact_allow_seed_url_fallback": False},
    ],
)
def test_quality_rejects_when_requirements_not_met(overrides):
    base_overrides = {
        "contact_allow_personalization_fallback": False,
        "contact_allow_seed_url_fallback": False,
    }
    base_overrides.update(overrides)
    config = make_config(
        contact_min_personal_anecdotes=1,
        contact_min_professional_anecdotes=1,
        contact_min_total_anecdotes=3,
        **base_overrides,
    )
    contact = {
        "personal_anecdotes": [" "],
        "professional_anecdotes": [],
    }

    passed, stats = evaluate_contact_quality(contact, config)

    assert not passed
    assert stats["reason"] == "insufficient"
    assert stats["total"] == 0
