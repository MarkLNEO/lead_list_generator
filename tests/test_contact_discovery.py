import pytest

from lead_pipeline import Config, N8NEnrichmentClient, _http_request  # noqa: F401


def make_config():
    return Config(
        supabase_key="supabase",
        hubspot_token="hubspot",
        discovery_webhook_url="http://example.com/discovery",
        company_enrichment_webhook="http://example.com/company",
        contact_enrichment_webhook="http://example.com/contact",
        email_verification_webhook="http://example.com/verify",
        contact_discovery_webhook="http://example.com/discover",
    )


def test_extract_contacts_from_message_response():
    payload = {
        "index": 0,
        "message": {
            "role": "assistant",
            "content": {
                "contacts": [
                    {
                        "name": "Ryan Smith",
                        "job_title": "Managing Partner",
                        "company": "At Home Wichita Property Management",
                        "domain": "athomewichita.rentals",
                        "linkedin_url": "https://linkedin.com/in/ryan",
                    }
                ],
                "total_found": 1,
            },
        },
    }

    contacts = N8NEnrichmentClient._extract_contacts_from_response(payload)

    assert len(contacts) == 1
    assert contacts[0]["name"] == "Ryan Smith"


def test_extract_contacts_from_list_of_messages():
    payload = [
        {
            "index": 0,
            "message": {
                "role": "assistant",
                "content": {
                    "contacts": [
                        {"name": "Armin Herteux", "job_title": "Owner", "domain": "blackdogmhk.com"},
                        {"name": "Shawn Kruse", "job_title": "Owner", "domain": "blackdogmhk.com"},
                    ]
                },
            },
        }
    ]

    contacts = N8NEnrichmentClient._extract_contacts_from_response(payload)

    assert {c["name"] for c in contacts} == {"Armin Herteux", "Shawn Kruse"}


def test_extract_contacts_from_plain_list():
    payload = [
        {"name": "A", "job_title": "CEO"},
        {"full_name": "B", "role": "COO"},
    ]

    contacts = N8NEnrichmentClient._extract_contacts_from_response(payload)

    assert len(contacts) == 2


def test_discover_contacts_deduplicates(monkeypatch):
    config = make_config()
    client = N8NEnrichmentClient(config)

    sample_response = {
        "message": {
            "role": "assistant",
            "content": {
                "contacts": [
                    {"name": "Ryan Smith", "job_title": "Managing Partner", "domain": "example.com"},
                    {"name": "Ryan Smith", "job_title": "Managing Partner", "domain": "example.com"},
                ]
            },
        }
    }

    def fake_http_request(*args, **kwargs):
        return sample_response

    monkeypatch.setattr("lead_pipeline._http_request", fake_http_request)

    contacts = client.discover_contacts({"company_name": "Example", "domain": "example.com"})

    assert len(contacts) == 1
    assert contacts[0]["full_name"] == "Ryan Smith"
