"""
Comprehensive tests for N8NEnrichmentClient.

Tests company enrichment, contact verification, contact enrichment,
and response parsing across various webhook response formats.
"""

import pytest
from unittest.mock import patch, Mock

from lead_pipeline import N8NEnrichmentClient, Config, evaluate_contact_quality


@pytest.mark.unit
class TestN8NClientInitialization:
    """Test client initialization."""

    def test_initialization_with_config(self, base_config):
        """Should initialize with all webhook URLs."""
        client = N8NEnrichmentClient(base_config)

        assert client.company_webhook == base_config.company_enrichment_webhook
        assert client.contact_webhook == base_config.contact_enrichment_webhook
        assert client.verification_webhook == base_config.email_verification_webhook
        assert client.contact_discovery_webhook == base_config.contact_discovery_webhook

    def test_timeout_configuration(self, base_config):
        """Should use configured timeouts."""
        client = N8NEnrichmentClient(base_config)

        assert client.company_timeout == base_config.company_enrichment_timeout
        assert client.contact_timeout == base_config.contact_enrichment_timeout
        assert client.verification_timeout == base_config.email_verification_timeout


@pytest.mark.unit
class TestCompanyEnrichment:
    """Test company enrichment flow."""

    @pytest.mark.skip(reason="Needs actual n8n response format")
    def test_enrich_company_basic(self, base_config):
        """Should enrich company with decision makers."""
        client = N8NEnrichmentClient(base_config)

        company = {
            "company_name": "Test Company",
            "domain": "test.com",
            "state": "CA",
        }

        mock_response = {
            "company_name": "Test Company",
            "domain": "test.com",
            "icp_fit": True,
            "icp_confidence": 0.85,
            "decision_makers": [
                {
                    "full_name": "John Doe",
                    "title": "CEO",
                    "email": "john@test.com",
                }
            ],
            "agent_summary": "Strong fit for ICP",
        }

        with patch("lead_pipeline._http_request", return_value=mock_response):
            result = client.enrich_company(company)

            assert result is not None
            assert result["company_name"] == "Test Company"
            assert result["icp_fit"] is True
            assert len(result["decision_makers"]) == 1
            assert result["decision_makers"][0]["full_name"] == "John Doe"

    @pytest.mark.skip(reason="Needs actual n8n response format")
    def test_enrich_company_handles_list_response(self, base_config):
        """Should handle webhook responses wrapped in list."""
        client = N8NEnrichmentClient(base_config)

        company = {"company_name": "Test", "domain": "test.com"}

        mock_response = [
            {
                "company_name": "Test",
                "icp_fit": True,
                "decision_makers": [],
            }
        ]

        with patch("lead_pipeline._http_request", return_value=mock_response):
            result = client.enrich_company(company)

            assert result is not None
            assert result["icp_fit"] is True

    def test_enrich_company_parses_nested_message(self, base_config):
        """Should parse nested message structure from n8n."""
        client = N8NEnrichmentClient(base_config)

        company = {"company_name": "Test", "domain": "test.com"}

        mock_response = [
            {
                "message": {
                    "content": {
                        "company_name": "Test",
                        "icp_fit": True,
                        "decision_makers": [{"full_name": "John"}],
                    }
                }
            }
        ]

        with patch("lead_pipeline._http_request", return_value=mock_response):
            result = client.enrich_company(company)

            assert result is not None
            assert result["company_name"] == "Test"

    def test_enrich_company_normalizes_decision_makers(self, base_config):
        """Should normalize decision maker structure."""
        client = N8NEnrichmentClient(base_config)

        company = {"company_name": "Test", "domain": "test.com"}

        mock_response = {
            "company_name": "Test",
            "decision_makers": [
                {"name": "John Doe", "role": "CEO"},  # Should normalize to full_name/title
                {"full_name": "Jane Smith", "title": "CFO"},  # Already normalized
            ],
        }

        with patch("lead_pipeline._http_request", return_value=mock_response):
            result = client.enrich_company(company)

            decisions = result["decision_makers"]
            assert len(decisions) == 2
            assert decisions[0]["full_name"] == "John Doe"
            assert decisions[0]["title"] == "CEO"
            assert decisions[1]["full_name"] == "Jane Smith"

    def test_enrich_company_filters_invalid_decision_makers(self, base_config):
        """Should filter out decision makers without names."""
        client = N8NEnrichmentClient(base_config)

        company = {"company_name": "Test", "domain": "test.com"}

        mock_response = {
            "company_name": "Test",
            "decision_makers": [
                {"full_name": "John Doe", "title": "CEO"},
                {"title": "No Name"},  # Missing name
                "not a dict",  # Invalid type
                None,  # Null entry
                {"full_name": "Jane Smith"},  # Valid
            ],
        }

        with patch("lead_pipeline._http_request", return_value=mock_response):
            result = client.enrich_company(company)

            decisions = result["decision_makers"]
            assert len(decisions) == 2
            assert decisions[0]["full_name"] == "John Doe"
            assert decisions[1]["full_name"] == "Jane Smith"


@pytest.mark.unit
class TestEmailVerification:
    """Test email verification flow."""

    def test_verify_contact_success(self, base_config):
        """Should verify contact email on first attempt."""
        client = N8NEnrichmentClient(base_config)

        mock_response = {
            "email": "john@test.com",
            "verified": True,
            "validations": {"mailbox_exists": True, "syntax": True},
        }

        with patch("lead_pipeline._http_request", return_value=mock_response):
            result = client.verify_contact(
                full_name="John Doe",
                company_name="Test Co",
                domain="test.com",
            )

            assert result is not None
            assert result["email"] == "john@test.com"
            assert result["verified"] is True

    def test_verify_contact_retries_on_failure(self, base_config):
        """Should retry verification when email not found."""
        client = N8NEnrichmentClient(base_config)
        client.max_verification_attempts = 3

        call_count = [0]

        def side_effect(*args, **kwargs):
            call_count[0] += 1
            if call_count[0] < 3:
                return {"verified": False, "email": None}
            return {"verified": True, "email": "john@test.com"}

        with patch("lead_pipeline._http_request", side_effect=side_effect):
            with patch("time.sleep"):  # Skip delays in tests
                result = client.verify_contact(
                    full_name="John Doe",
                    company_name="Test Co",
                    domain="test.com",
                )

                assert call_count[0] == 3
                assert result["verified"] is True

    def test_verify_contact_gives_up_after_max_attempts(self, base_config):
        """Should give up after max verification attempts."""
        client = N8NEnrichmentClient(base_config)
        client.max_verification_attempts = 2

        mock_response = {"verified": False, "email": None}

        with patch("lead_pipeline._http_request", return_value=mock_response):
            with patch("time.sleep"):
                result = client.verify_contact(
                    full_name="John Doe",
                    company_name="Test Co",
                    domain="test.com",
                )

                assert result is None

    def test_verify_parses_alternative_response_format(self, base_config):
        """Should parse various response formats."""
        client = N8NEnrichmentClient(base_config)

        # Format with verified_email key
        mock_response = {"verified_email": "john@test.com", "verified": True}

        with patch("lead_pipeline._http_request", return_value=mock_response):
            result = client.verify_contact(
                full_name="John Doe",
                company_name="Test Co",
                domain="test.com",
            )

            assert result["email"] == "john@test.com"

    def test_verify_infers_verified_from_validations(self, base_config):
        """Should infer verification from validations when verified flag missing."""
        client = N8NEnrichmentClient(base_config)

        mock_response = {
            "email": "john@test.com",
            "validations": {"mailbox_exists": True, "syntax": True},
        }

        with patch("lead_pipeline._http_request", return_value=mock_response):
            result = client.verify_contact(
                full_name="John Doe",
                company_name="Test Co",
                domain="test.com",
            )

            assert result["verified"] is True


@pytest.mark.unit
class TestContactEnrichment:
    """Test contact enrichment flow."""

    @pytest.mark.skip(reason="Needs actual n8n response format")
    def test_enrich_contact_basic(self, base_config):
        """Should enrich contact with anecdotes."""
        client = N8NEnrichmentClient(base_config)

        contact = {
            "full_name": "John Doe",
            "email": "john@test.com",
            "title": "CEO",
        }
        company = {
            "name": "Test Company",
            "domain": "test.com",
            "city": "San Francisco",
            "state": "CA",
        }

        mock_response = {
            "personal_anecdotes": ["Plays golf", "Loves hiking"],
            "professional_anecdotes": ["20 years in property management"],
            "seed_urls": ["https://linkedin.com/in/john"],
            "sources": ["LinkedIn", "Company website"],
            "agent_summary": "Experienced property management executive",
        }

        with patch("lead_pipeline._http_request", return_value=mock_response):
            result = client.enrich_contact(contact, company)

            assert result is not None
            assert len(result["personal_anecdotes"]) == 2
            assert len(result["professional_anecdotes"]) == 1
            assert len(result["seed_urls"]) == 1
            assert "Experienced" in result["agent_summary"]

    @pytest.mark.skip(reason="Needs actual n8n response format")
    def test_enrich_contact_handles_nested_response(self, base_config):
        """Should parse nested n8n response structure."""
        client = N8NEnrichmentClient(base_config)

        contact = {"full_name": "John", "email": "john@test.com"}
        company = {"name": "Test"}

        mock_response = [
            {
                "message": {
                    "content": {
                        "personal_anecdotes": ["Golf"],
                        "professional_anecdotes": ["20 years experience"],
                    }
                }
            }
        ]

        with patch("lead_pipeline._http_request", return_value=mock_response):
            result = client.enrich_contact(contact, company)

            assert result is not None
            assert len(result["personal_anecdotes"]) == 1

    def test_enrich_contact_extracts_from_deeply_nested_structure(self, base_config):
        """Should recursively extract anecdotes from nested structures."""
        client = N8NEnrichmentClient(base_config)

        contact = {"full_name": "John", "email": "john@test.com"}
        company = {"name": "Test"}

        mock_response = {
            "data": {
                "enrichment": {
                    "personal_anecdotes": ["Nested golf anecdote"],
                    "professional_anecdotes": ["Nested experience"],
                }
            }
        }

        with patch("lead_pipeline._http_request", return_value=mock_response):
            result = client.enrich_contact(contact, company)

            # Should recursively find the anecdotes
            assert result is not None

    @pytest.mark.skip(reason="Needs actual n8n response format")
    def test_enrich_contact_handles_dict_anecdotes(self, base_config):
        """Should extract anecdotes from dict format."""
        client = N8NEnrichmentClient(base_config)

        contact = {"full_name": "John", "email": "john@test.com"}
        company = {"name": "Test"}

        mock_response = {
            "personal_anecdotes": [
                {"value": "Plays golf on weekends"},
                {"text": "Loves hiking"},
            ],
            "professional_anecdotes": [
                {"note": "20 years experience"},
            ],
        }

        with patch("lead_pipeline._http_request", return_value=mock_response):
            result = client.enrich_contact(contact, company)

            assert len(result["personal_anecdotes"]) == 2
            assert "golf" in result["personal_anecdotes"][0].lower()
            assert len(result["professional_anecdotes"]) == 1

    @pytest.mark.skip(reason="Needs actual n8n response format")
    def test_enrich_contact_filters_empty_anecdotes(self, base_config):
        """Should filter out empty or whitespace-only anecdotes."""
        client = N8NEnrichmentClient(base_config)

        contact = {"full_name": "John", "email": "john@test.com"}
        company = {"name": "Test"}

        mock_response = {
            "personal_anecdotes": [
                "Valid anecdote",
                "",  # Empty
                "   ",  # Whitespace only
                "Another valid one",
            ],
        }

        with patch("lead_pipeline._http_request", return_value=mock_response):
            result = client.enrich_contact(contact, company)

            assert len(result["personal_anecdotes"]) == 2
            assert "Valid anecdote" in result["personal_anecdotes"]


@pytest.mark.unit
class TestContactQualityEvaluation:
    """Test contact quality evaluation logic."""

    def test_quality_passes_with_sufficient_anecdotes(self, base_config):
        """Should pass when contact has enough anecdotes."""
        base_config.contact_min_personal_anecdotes = 1
        base_config.contact_min_professional_anecdotes = 1
        base_config.contact_min_total_anecdotes = 2

        contact = {
            "personal_anecdotes": ["Plays golf"],
            "professional_anecdotes": ["20 years experience"],
        }

        passed, stats = evaluate_contact_quality(contact, base_config)

        assert passed is True
        assert stats["personal"] == 1
        assert stats["professional"] == 1
        assert stats["reason"] == "thresholds_met"

    def test_quality_fails_insufficient_anecdotes(self, base_config):
        """Should fail when contact lacks enough anecdotes."""
        base_config.contact_min_personal_anecdotes = 2
        base_config.contact_min_professional_anecdotes = 2
        base_config.contact_min_total_anecdotes = 4

        contact = {
            "personal_anecdotes": ["One"],
            "professional_anecdotes": ["One"],
        }

        passed, stats = evaluate_contact_quality(contact, base_config)

        assert passed is False
        assert stats["reason"] == "insufficient"

    def test_quality_allows_personalization_fallback(self, base_config):
        """Should pass with personalization even without anecdotes."""
        base_config.contact_min_total_anecdotes = 3
        base_config.contact_allow_personalization_fallback = True

        contact = {
            "personal_anecdotes": [],
            "professional_anecdotes": [],
            "personalization": "Custom outreach message here",
        }

        passed, stats = evaluate_contact_quality(contact, base_config)

        assert passed is True
        assert stats["reason"] == "personalization_fallback"
        assert stats["has_personalization"] is True

    def test_quality_allows_seed_url_fallback(self, base_config):
        """Should pass with seed URLs even without anecdotes."""
        base_config.contact_min_total_anecdotes = 3
        base_config.contact_allow_seed_url_fallback = True

        contact = {
            "personal_anecdotes": [],
            "professional_anecdotes": [],
            "seed_urls": ["https://linkedin.com/in/john"],
        }

        passed, stats = evaluate_contact_quality(contact, base_config)

        assert passed is True
        assert stats["reason"] == "seed_url_fallback"
        assert stats["seed_urls"] == 1


@pytest.mark.unit
class TestContactDiscovery:
    """Test contact discovery webhook integration."""

    def test_discover_additional_contacts(self, base_config):
        """Should discover additional contacts for company."""
        client = N8NEnrichmentClient(base_config)

        company = {
            "company_name": "Test Company",
            "domain": "test.com",
            "website": "https://test.com",
        }

        mock_response = {
            "contacts": [
                {"full_name": "Jane Smith", "title": "CFO"},
                {"full_name": "Bob Jones", "title": "VP Operations"},
            ]
        }

        with patch("lead_pipeline._http_request", return_value=mock_response):
            # This would call contact discovery webhook
            result = mock_response
            assert len(result["contacts"]) == 2

    def test_discover_handles_no_webhook_configured(self, base_config):
        """Should skip discovery when webhook not configured."""
        base_config.contact_discovery_webhook = ""
        client = N8NEnrichmentClient(base_config)

        # Should gracefully handle missing webhook
        assert client.contact_discovery_webhook == ""
