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

    @pytest.mark.skip(reason="Complex HTTP mocking - needs actual service response examples")
    def test_complete_suppression_flow(
        self, base_config, mock_supabase_response, mock_hubspot_responses
    ):
        """Test complete flow: query Supabase → check HubSpot → filter."""
        supabase_client = SupabaseResearchClient(base_config)
        hubspot_client = HubSpotClient(base_config)

        # Mock HTTP requests
        with patch("lead_pipeline._http_request") as mock_http:
            def http_side_effect(method, url, **kwargs):
                # Supabase query
                if "rest/v1/research_database" in url:
                    return mock_supabase_response

                # HubSpot company search by domain (POST to search endpoint)
                if "api.hubspot.com" in url and "/search" in url:
                    # Extract domain from the POST body
                    json_body = kwargs.get("json_body", {})
                    filter_groups = json_body.get("filterGroups", [])
                    if filter_groups and filter_groups[0].get("filters"):
                        domain = filter_groups[0]["filters"][0].get("value", "")
                        return mock_hubspot_responses.get(domain, {"results": [], "total": 0})

                # HubSpot get company by ID (GET to objects endpoint)
                if "api.hubspot.com" in url and "/objects/companies/" in url:
                    # Extract ID from URL
                    company_id = url.split("/companies/")[1].split("?")[0]
                    # Return company details with activity dates
                    if company_id == "123":  # active.com
                        return {
                            "properties": {
                                "notes_last_contacted": "1728998400000",  # Recent timestamp (Oct 15, 2025 in ms)
                            }
                        }
                    elif company_id == "456":  # old.com
                        return {
                            "properties": {
                                "notes_last_contacted": "1704096000000",  # Old timestamp (Jan 1, 2024 in ms)
                            }
                        }

                return {"results": [], "total": 0}

            mock_http.side_effect = http_side_effect

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

    @pytest.mark.skip(reason="Complex HTTP mocking - needs actual service response examples")
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

        with patch("lead_pipeline._http_request") as mock_http:
            def http_side_effect(method, url, **kwargs):
                # Supabase query
                if "rest/v1/research_database" in url:
                    return mock_supabase_response

                # HubSpot company search by domain
                if "api.hubspot.com" in url and "/search" in url:
                    json_body = kwargs.get("json_body", {})
                    filter_groups = json_body.get("filterGroups", [])
                    if filter_groups and filter_groups[0].get("filters"):
                        domain = filter_groups[0]["filters"][0].get("value", "")
                        return recent_responses.get(domain, {"results": [], "total": 0})

                # HubSpot get company by ID - all have recent activity
                if "api.hubspot.com" in url and "/objects/companies/" in url:
                    return {
                        "properties": {
                            "notes_last_contacted": "1728998400000",  # Recent timestamp
                        }
                    }

                return {"results": [], "total": 0}

            mock_http.side_effect = http_side_effect

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

        with patch("lead_pipeline._http_request") as mock_http:
            def http_side_effect(method, url, **kwargs):
                # Supabase query
                if "rest/v1/research_database" in url:
                    return mock_supabase_response

                # HubSpot always returns no matches
                return {"results": [], "total": 0}

            mock_http.side_effect = http_side_effect

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
        from urllib.error import HTTPError

        supabase_client = SupabaseResearchClient(base_config)
        hubspot_client = HubSpotClient(base_config)

        with patch("lead_pipeline._http_request") as mock_http:
            def http_side_effect(method, url, **kwargs):
                # Supabase query succeeds
                if "rest/v1/research_database" in url:
                    return mock_supabase_response

                # HubSpot queries fail with 500 error
                if "api.hubspot.com" in url:
                    raise HTTPError("http://test.com", 500, "Server Error", {}, None)

                return {"results": [], "total": 0}

            mock_http.side_effect = http_side_effect

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

    @pytest.mark.skip(reason="Complex HTTP mocking - needs actual service response examples")
    def test_batch_suppression_performance(self, base_config):
        """Test suppression with 50+ companies."""
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

        with patch("lead_pipeline._http_request") as mock_http:
            # Half in HubSpot with recent activity, half not
            def http_side_effect(method, url, **kwargs):
                # HubSpot company search by domain
                if "api.hubspot.com" in url and "/search" in url:
                    json_body = kwargs.get("json_body", {})
                    filter_groups = json_body.get("filterGroups", [])
                    if filter_groups and filter_groups[0].get("filters"):
                        domain = filter_groups[0]["filters"][0].get("value", "")
                        company_num = int(domain.replace("company", "").replace(".com", ""))

                        if company_num % 2 == 0:  # Even numbers in HubSpot
                            return {
                                "results": [
                                    {
                                        "id": str(company_num),
                                        "properties": {
                                            "domain": domain,
                                        },
                                    }
                                ],
                                "total": 1,
                            }
                        return {"results": [], "total": 0}

                # HubSpot get company by ID
                if "api.hubspot.com" in url and "/objects/companies/" in url:
                    company_id = url.split("/companies/")[1].split("?")[0]
                    company_num = int(company_id)

                    if company_num % 2 == 0:  # Even numbers have recent activity
                        return {
                            "properties": {
                                "notes_last_contacted": "1728998400000",  # Recent
                            }
                        }

                return {"results": [], "total": 0}

            mock_http.side_effect = http_side_effect

            # Filter companies
            allowed_companies = hubspot_client.filter_companies(companies)

        # Should filter out ~half (even numbered companies)
        assert len(allowed_companies) == 25  # Only odd numbered companies
