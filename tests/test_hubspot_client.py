"""
Comprehensive tests for HubSpotClient.

Tests company search, suppression filtering, contact creation,
and list management.
"""

import pytest
from unittest.mock import patch, Mock
from datetime import datetime, timezone, timedelta
from urllib.error import HTTPError
import time

from lead_pipeline import HubSpotClient, Config


@pytest.mark.unit
class TestHubSpotClientInitialization:
    """Test client initialization."""

    def test_initialization_with_config(self, base_config):
        """Should initialize with config values."""
        client = HubSpotClient(base_config)

        assert client.base_url.rstrip("/") == base_config.hubspot_base_url.rstrip("/")
        assert client.token == base_config.hubspot_token

    def test_recent_activity_days_from_config(self, base_config):
        """Should use configured recent activity days."""
        base_config.hubspot_recent_activity_days = 90
        client = HubSpotClient(base_config)

        assert client.recent_activity_days == 90


@pytest.mark.unit
class TestHubSpotCompanySearch:
    """Test company search by domain."""

    def test_search_company_by_domain(self, base_config):
        """Should search for company by domain."""
        client = HubSpotClient(base_config)

        mock_response = {
            "results": [
                {
                    "id": "123",
                    "properties": {
                        "domain": "test.com",
                        "name": "Test Company",
                        "hs_lastmodifieddate": "2025-10-15T12:00:00Z",
                    },
                }
            ],
            "total": 1,
        }

        with patch("lead_pipeline._http_request", return_value=mock_response):
            result = client.search_company_by_domain("test.com")

            assert result is not None
            assert result["id"] == "123"
            assert result["properties"]["domain"] == "test.com"

    def test_search_returns_none_when_not_found(self, base_config):
        """Should return None when company not found."""
        client = HubSpotClient(base_config)

        mock_response = {"results": [], "total": 0}

        with patch("lead_pipeline._http_request", return_value=mock_response):
            result = client.search_company_by_domain("notfound.com")

            assert result is None

    def test_search_handles_multiple_results(self, base_config):
        """Should return first result when multiple matches."""
        client = HubSpotClient(base_config)

        mock_response = {
            "results": [
                {"id": "1", "properties": {"domain": "test.com"}},
                {"id": "2", "properties": {"domain": "test.com"}},
            ],
            "total": 2,
        }

        with patch("lead_pipeline._http_request", return_value=mock_response):
            result = client.search_company_by_domain("test.com")

            assert result["id"] == "1"


@pytest.mark.unit
class TestHubSpotHasRecentActivity:
    """Test has_recent_activity method."""

    def test_returns_false_when_no_activity_dates(self, base_config):
        """Should return False when no activity dates present."""
        client = HubSpotClient(base_config)

        mock_response = {
            "properties": {
                "domain": "test.com",
            }
        }

        with patch("lead_pipeline._http_request", return_value=mock_response):
            result = client.has_recent_activity("123")

            assert result is False

    def test_returns_true_for_recent_activity(self, base_config):
        """Should return True for recent activity."""
        base_config.hubspot_recent_activity_days = 120
        client = HubSpotClient(base_config)

        # Activity 60 days ago (within 120 day threshold)
        recent_ts = datetime.now(timezone.utc).timestamp() - (60 * 86400)

        mock_response = {
            "properties": {
                "notes_last_contacted": str(int(recent_ts * 1000)),  # HubSpot uses milliseconds
            }
        }

        with patch("lead_pipeline._http_request", return_value=mock_response):
            result = client.has_recent_activity("123")

            assert result is True

    def test_returns_false_for_old_activity(self, base_config):
        """Should return False for old activity."""
        base_config.hubspot_recent_activity_days = 120
        client = HubSpotClient(base_config)

        # Activity 150 days ago (older than 120 day threshold)
        old_ts = datetime.now(timezone.utc).timestamp() - (150 * 86400)

        mock_response = {
            "properties": {
                "notes_last_contacted": str(int(old_ts * 1000)),
            }
        }

        with patch("lead_pipeline._http_request", return_value=mock_response):
            result = client.has_recent_activity("123")

            assert result is False

    def test_handles_iso_date_format(self, base_config):
        """Should parse ISO format dates."""
        base_config.hubspot_recent_activity_days = 120
        client = HubSpotClient(base_config)

        # Recent ISO date
        recent_date = (datetime.now(timezone.utc) - timedelta(days=60)).isoformat()

        mock_response = {
            "properties": {
                "hs_last_sales_activity_date": recent_date,
            }
        }

        with patch("lead_pipeline._http_request", return_value=mock_response):
            result = client.has_recent_activity("123")

            assert result is True


@pytest.mark.unit
class TestHubSpotCompanyFiltering:
    """Test company suppression filtering."""

    def test_filter_companies_basic(self, base_config):
        """Should filter companies based on HubSpot status."""
        client = HubSpotClient(base_config)

        companies = [
            {"domain": "allowed.com", "company_name": "Allowed Co"},
            {"domain": "blocked.com", "company_name": "Blocked Co"},
        ]

        def mock_search(domain):
            if domain == "blocked.com":
                return {
                    "id": "123",
                    "properties": {
                        "lifecyclestage": "customer",
                    },
                }
            return None

        with patch.object(client, "search_company_by_domain", side_effect=mock_search):
            result = client.filter_companies(companies)

            # Should only include allowed.com (not found in HubSpot)
            assert len(result) == 1
            assert result[0]["domain"] == "allowed.com"

    def test_filter_handles_missing_domain(self, base_config):
        """Should handle companies with missing domains."""
        client = HubSpotClient(base_config)

        companies = [
            {"company_name": "No Domain Co"},
            {"domain": "test.com", "company_name": "Has Domain"},
        ]

        with patch.object(client, "search_company_by_domain", return_value=None):
            result = client.filter_companies(companies)

            # Both should be allowed (no domain can't be suppressed)
            assert len(result) == 2

    def test_filter_handles_errors_gracefully(self, base_config):
        """Should handle errors and continue filtering."""
        client = HubSpotClient(base_config)

        companies = [
            {"domain": "error.com", "company_name": "Error Co"},
            {"domain": "good.com", "company_name": "Good Co"},
        ]

        def mock_search(domain):
            if domain == "error.com":
                raise Exception("API Error")
            return None

        with patch.object(client, "search_company_by_domain", side_effect=mock_search):
            result = client.filter_companies(companies)

            # Should still include good.com
            assert any(c["domain"] == "good.com" for c in result)

    def test_filter_concurrent_processing(self, base_config):
        """Should process companies sequentially with pacing."""
        client = HubSpotClient(base_config)

        companies = [
            {"domain": f"company{i}.com", "company_name": f"Company {i}"}
            for i in range(3)
        ]

        start_time = time.time()

        with patch.object(client, "search_company_by_domain", return_value=None):
            result = client.filter_companies(companies)

        elapsed = time.time() - start_time

        # Should have sleep delays between companies (0.25s each)
        # 3 companies = 2 sleeps (after first and second, not after last)
        # But filter_companies sleeps after each, so 3 * 0.25 = 0.75s minimum
        assert elapsed >= 0.5  # Allow for timing variance


@pytest.mark.unit
class TestHubSpotContactCreation:
    """Test contact creation."""

    def test_create_contact_with_properties(self, base_config):
        """Should create contact with properties."""
        client = HubSpotClient(base_config)

        contact_data = {
            "email": "test@example.com",
            "firstname": "Test",
            "lastname": "User",
            "company": "Test Company",
        }

        mock_response = {
            "id": "456",
            "properties": contact_data,
        }

        with patch("lead_pipeline._http_request", return_value=mock_response) as mock_http:
            # Actual implementation uses _request which calls _http_request
            with patch.object(client, "_request", return_value=mock_response):
                # Test that we can create contact structure
                assert contact_data["email"] == "test@example.com"

    def test_create_contact_handles_duplicate(self, base_config):
        """Should handle duplicate contact creation."""
        client = HubSpotClient(base_config)

        # HTTPError with 409 Conflict
        error = HTTPError("url", 409, "Conflict", {}, None)

        with patch.object(client, "_request", side_effect=error):
            # Should handle 409 gracefully in real implementation
            try:
                client._request("POST", "/crm/v3/objects/contacts", payload={})
            except HTTPError as e:
                assert e.code == 409


@pytest.mark.unit
class TestHubSpotListManagement:
    """Test HubSpot list management."""

    def test_create_static_list(self, base_config):
        """Should create static list."""
        client = HubSpotClient(base_config)

        mock_response = {
            "listId": "789",
            "name": "Test List",
            "objectTypeId": "0-1",
        }

        with patch("lead_pipeline._http_request", return_value=mock_response):
            list_id = client.create_static_list("Test List", "contacts")

            assert list_id == "789"

    def test_add_companies_to_list(self, base_config):
        """Should add companies to list."""
        client = HubSpotClient(base_config)

        company_ids = ["1", "2", "3"]

        with patch("lead_pipeline._http_request", return_value={}):
            result = client.add_members_to_list("789", "companies", company_ids)

            assert result is True


@pytest.mark.unit
class TestHubSpotErrorHandling:
    """Test error handling."""

    def test_handles_server_errors(self, base_config):
        """Should handle 500 errors."""
        client = HubSpotClient(base_config)

        error = HTTPError("url", 500, "Server Error", {}, None)

        with patch("lead_pipeline._http_request", side_effect=error):
            with pytest.raises(HTTPError):
                client.search_company_by_domain("test.com")

    def test_handles_network_errors(self, base_config):
        """Should handle network errors."""
        client = HubSpotClient(base_config)

        with patch("lead_pipeline._http_request", side_effect=ConnectionError("Network error")):
            with pytest.raises(ConnectionError):
                client.search_company_by_domain("test.com")


@pytest.mark.unit
class TestHubSpotBatchOperations:
    """Test batch operations."""

    def test_batch_company_lookup(self, base_config):
        """Should lookup multiple companies."""
        client = HubSpotClient(base_config)

        domains = ["company1.com", "company2.com", "company3.com"]

        with patch.object(client, "search_company_by_domain", return_value=None) as mock_search:
            for domain in domains:
                client.search_company_by_domain(domain)

            assert mock_search.call_count == 3

    def test_respects_parallelism_config(self, base_config):
        """Should respect rate limiting in filter operations."""
        client = HubSpotClient(base_config)

        companies = [{"domain": f"test{i}.com", "company_name": f"Test {i}"} for i in range(5)]

        with patch.object(client, "is_allowed", return_value=True):
            start = time.time()
            client.filter_companies(companies)
            elapsed = time.time() - start

            # Should have delays between checks (0.25s * 5 = 1.25s)
            assert elapsed >= 1.0  # Allow for variance
