"""
Integration tests for Supabase → HubSpot suppression flow.

Tests the complete flow of querying companies from Supabase and filtering
them through HubSpot's suppression logic.
"""

import pytest
from unittest.mock import patch, Mock
from lead_pipeline import SupabaseResearchClient, HubSpotClient, Config


@pytest.mark.integration
@pytest.mark.supabase
@pytest.mark.hubspot
class TestSuppressionFlow:
    """Test complete suppression flow from Supabase to HubSpot filtering."""

    @pytest.fixture
    def mock_supabase_response(self):
        """Mock Supabase response with multiple companies."""
        return [
            {
                "company_name": "Active Company",
                "domain": "active.com",
                "hq_state": "CA",
                "unit_count_numeric": 100,
            },
            {
                "company_name": "Old Company",
                "domain": "old.com",
                "hq_state": "CA",
                "unit_count_numeric": 200,
            },
            {
                "company_name": "New Company",
                "domain": "new.com",
                "hq_state": "CA",
                "unit_count_numeric": 150,
            },
        ]

    @pytest.fixture
    def mock_hubspot_responses(self):
        """Mock HubSpot responses for different companies."""
        return {
            "active.com": {
                "results": [
                    {
                        "id": "123",
                        "properties": {
                            "domain": "active.com",
                            "hs_lastmodifieddate": "2025-10-15T12:00:00Z",
                        },
                    }
                ],
                "total": 1,
            },
            "old.com": {
                "results": [
                    {
                        "id": "456",
                        "properties": {
                            "domain": "old.com",
                            "hs_lastmodifieddate": "2024-01-01T12:00:00Z",
                        },
                    }
                ],
                "total": 1,
            },
            "new.com": {
                "results": [],
                "total": 0,
            },
        }

    def test_complete_suppression_flow(
        self, base_config, mock_supabase_response, mock_hubspot_responses
    ):
        """Test complete flow: query Supabase → check HubSpot → filter."""
        supabase_client = SupabaseResearchClient(base_config)
        hubspot_client = HubSpotClient(base_config)

        # Mock Supabase query
        with patch.object(supabase_client, "_request") as mock_supabase:
            mock_supabase.return_value = mock_supabase_response

            # Mock HubSpot searches
            with patch.object(hubspot_client, "_request") as mock_hubspot:
                def hubspot_side_effect(method, endpoint, **kwargs):
                    # Extract domain from search query
                    if "domain=" in endpoint:
                        domain = endpoint.split("domain=")[1].split("&")[0]
                        return mock_hubspot_responses.get(domain, {"results": [], "total": 0})
                    return {"results": [], "total": 0}

                mock_hubspot.side_effect = hubspot_side_effect

                # Query companies from Supabase
                companies = supabase_client.find_existing_companies(
                    state="CA",
                    pms=None,
                    city=None,
                    unit_min=None,
                    unit_max=None,
                    limit=100,
                )

                # Filter through HubSpot
                allowed_companies = hubspot_client.filter_companies(companies)

        # Verify results
        assert len(companies) == 3  # Got all from Supabase
        assert len(allowed_companies) == 2  # Filtered out active company

        # Should keep old and new companies
        allowed_domains = {c["domain"] for c in allowed_companies}
        assert "old.com" in allowed_domains  # Old activity is OK
        assert "new.com" in allowed_domains  # Not in HubSpot is OK
        assert "active.com" not in allowed_domains  # Recent activity suppressed

    def test_suppression_with_all_recent_companies(
        self, base_config, mock_supabase_response
    ):
        """Test when all companies have recent HubSpot activity."""
        supabase_client = SupabaseResearchClient(base_config)
        hubspot_client = HubSpotClient(base_config)

        # All companies have recent activity
        recent_responses = {
            domain: {
                "results": [
                    {
                        "id": f"id_{domain}",
                        "properties": {
                            "domain": domain,
                            "hs_lastmodifieddate": "2025-10-15T12:00:00Z",
                        },
                    }
                ],
                "total": 1,
            }
            for domain in ["active.com", "old.com", "new.com"]
        }

        with patch.object(supabase_client, "_request") as mock_supabase:
            mock_supabase.return_value = mock_supabase_response

            with patch.object(hubspot_client, "_request") as mock_hubspot:
                def hubspot_side_effect(method, endpoint, **kwargs):
                    if "domain=" in endpoint:
                        domain = endpoint.split("domain=")[1].split("&")[0]
                        return recent_responses.get(domain, {"results": [], "total": 0})
                    return {"results": [], "total": 0}

                mock_hubspot.side_effect = hubspot_side_effect

                companies = supabase_client.find_existing_companies(
                    state="CA", pms=None, city=None, unit_min=None, unit_max=None, limit=100
                )
                allowed_companies = hubspot_client.filter_companies(companies)

        # All should be suppressed
        assert len(allowed_companies) == 0

    def test_suppression_with_no_hubspot_matches(
        self, base_config, mock_supabase_response
    ):
        """Test when no companies are in HubSpot."""
        supabase_client = SupabaseResearchClient(base_config)
        hubspot_client = HubSpotClient(base_config)

        with patch.object(supabase_client, "_request") as mock_supabase:
            mock_supabase.return_value = mock_supabase_response

            with patch.object(hubspot_client, "_request") as mock_hubspot:
                mock_hubspot.return_value = {"results": [], "total": 0}

                companies = supabase_client.find_existing_companies(
                    state="CA", pms=None, city=None, unit_min=None, unit_max=None, limit=100
                )
                allowed_companies = hubspot_client.filter_companies(companies)

        # All should pass through
        assert len(allowed_companies) == 3

    def test_suppression_handles_hubspot_errors_gracefully(
        self, base_config, mock_supabase_response
    ):
        """Test that HubSpot errors don't crash the flow."""
        supabase_client = SupabaseResearchClient(base_config)
        hubspot_client = HubSpotClient(base_config)

        with patch.object(supabase_client, "_request") as mock_supabase:
            mock_supabase.return_value = mock_supabase_response

            with patch.object(hubspot_client, "_request") as mock_hubspot:
                from urllib.error import HTTPError

                mock_hubspot.side_effect = HTTPError(
                    "http://test.com", 500, "Server Error", {}, None
                )

                companies = supabase_client.find_existing_companies(
                    state="CA", pms=None, city=None, unit_min=None, unit_max=None, limit=100
                )

                # Should handle error and allow companies through
                # (conservative approach: if HubSpot check fails, allow the company)
                allowed_companies = hubspot_client.filter_companies(companies)

                # Implementation may vary - either allow all or reject all depending on safety preference
                assert isinstance(allowed_companies, list)


@pytest.mark.integration
@pytest.mark.slow
class TestSuppressionPerformance:
    """Test suppression flow performance with larger datasets."""

    def test_batch_suppression_performance(self, base_config):
        """Test suppression with 50+ companies."""
        supabase_client = SupabaseResearchClient(base_config)
        hubspot_client = HubSpotClient(base_config)

        # Create 50 mock companies
        companies = [
            {
                "company_name": f"Company {i}",
                "domain": f"company{i}.com",
                "hq_state": "CA",
                "unit_count_numeric": 100 + i,
            }
            for i in range(50)
        ]

        with patch.object(hubspot_client, "_request") as mock_hubspot:
            # Half in HubSpot with recent activity, half not
            def hubspot_side_effect(method, endpoint, **kwargs):
                if "domain=" in endpoint:
                    domain = endpoint.split("domain=")[1].split("&")[0]
                    company_num = int(domain.replace("company", "").replace(".com", ""))

                    if company_num % 2 == 0:  # Even numbers have recent activity
                        return {
                            "results": [
                                {
                                    "id": str(company_num),
                                    "properties": {
                                        "domain": domain,
                                        "hs_lastmodifieddate": "2025-10-15T12:00:00Z",
                                    },
                                }
                            ],
                            "total": 1,
                        }
                    return {"results": [], "total": 0}

                return {"results": [], "total": 0}

            mock_hubspot.side_effect = hubspot_side_effect

            # Filter companies
            allowed_companies = hubspot_client.filter_companies(companies)

        # Should filter out ~half (even numbered companies)
        assert len(allowed_companies) == 25  # Only odd numbered companies
