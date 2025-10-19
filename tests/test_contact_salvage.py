from lead_pipeline import Config, LeadOrchestrator


def make_orchestrator():
    return LeadOrchestrator(
        Config(
            supabase_key="key",
            hubspot_token="token",
            discovery_webhook_url="http://example.com/discovery",
            company_enrichment_webhook="http://example.com/company",
            contact_enrichment_webhook="http://example.com/contact",
            email_verification_webhook="http://example.com/verify",
            contact_discovery_webhook="http://example.com/discover",
        )
    )


def test_salvage_contact_anecdotes_from_nested_payload():
    orchestrator = make_orchestrator()
    contact = {
        "personal_anecdotes": [],
        "professional_anecdotes": [],
        "seed_urls": [],
        "raw": {
            "message": {
                "content": {
                    "personal": [
                        "Community — Active volunteer at local shelter."
                    ],
                    "professional": [
                        {"text": "Role — Oversees leasing operations."}
                    ],
                    "seed_urls": [
                        "https://example.com/about"
                    ],
                }
            }
        },
    }

    salvaged = orchestrator._salvage_contact_anecdotes(contact)

    assert salvaged is True
    assert contact["personal_anecdotes"] == ["Community — Active volunteer at local shelter."]
    assert contact["professional_anecdotes"] == ["Role — Oversees leasing operations."]
    assert contact["seed_urls"] == ["https://example.com/about"]


def test_salvage_uses_summary_sections():
    orchestrator = make_orchestrator()
    contact = {
        "personal_anecdotes": [],
        "professional_anecdotes": [],
        "seed_urls": [],
        "raw": {
            "agent_summary": "- Personal: Enjoys marathons\n- Role: Director of Property Management"
        },
    }

    salvaged = orchestrator._salvage_contact_anecdotes(contact)

    assert salvaged is True
    assert contact["personal_anecdotes"][0].startswith("Personal")
    assert contact["professional_anecdotes"][0].startswith("Role")
