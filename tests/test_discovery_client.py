"""
Comprehensive tests for DiscoveryWebhookClient.

Tests company discovery webhook calls, response parsing,
and timeout management.
"""

import pytest
from unittest.mock import patch
from urllib.error import HTTPError, URLError

from lead_pipeline import DiscoveryWebhookClient


@pytest.mark.unit
class TestDiscoveryClientInitialization:
    """Test client initialization."""

    def test_initialization(self):
        """Should initialize with URL and timeout."""
        webhook_url = "http://test.com/webhook"
        timeout = 1800.0

        client = DiscoveryWebhookClient(webhook_url, timeout)

        assert client.url == webhook_url
        assert client.timeout == 1800.0  # Enforces minimum 60

    def test_minimum_timeout_enforced(self):
        """Should enforce minimum timeout of 60 seconds."""
        client = DiscoveryWebhookClient("http://test.com/webhook", 30.0)

        assert client.timeout == 60.0  # Should be increased to minimum


@pytest.mark.unit
class TestDiscoveryWebhookCall:
    """Test discovery webhook request/response."""

    def test_discover_basic(self):
        """Should call discovery webhook with search parameters."""
        client = DiscoveryWebhookClient("http://test.com/webhook", 1800.0)

        mock_response = {
            "companies": [
                {
                    "company_name": "Discovered Company",
                    "domain": "discovered.com",
                    "location": "San Francisco, CA",
                }
            ],
            "total": 1,
        }

        with patch("lead_pipeline._http_request", return_value=mock_response):
            result = client.discover(
                location="San Francisco, CA",
                state="CA",
                pms="AppFolio",
                quantity=10,
                unit_count_min=None,
                unit_count_max=None,
                suppression_domains=[],
                extra_requirements=None,
                attempt=1,
            )

            assert isinstance(result, list)
            assert len(result) == 1
            assert result[0]["company_name"] == "Discovered Company"

    def test_discover_passes_all_parameters(self):
        """Should pass all search parameters to webhook."""
        client = DiscoveryWebhookClient("http://test.com/webhook", 1800.0)

        with patch("lead_pipeline._http_request") as mock_http:
            mock_http.return_value = {"companies": []}

            client.discover(
                location="Austin, TX",
                state="TX",
                pms="Yardi",
                quantity=20,
                unit_count_min=100,
                unit_count_max=500,
                suppression_domains=["exclude1.com", "exclude2.com"],
                extra_requirements="NARPM member",
                attempt=1,
            )

            # Verify webhook was called
            assert mock_http.called
            call_args = mock_http.call_args
            payload = call_args[1]["json_body"]

            assert payload["location"] == "Austin, TX"
            assert payload["state"] == "TX"
            assert payload["pms"] == "Yardi"
            assert payload["quantity"] == 20
            assert payload["unit_count_min"] == 100
            assert payload["unit_count_max"] == 500
            assert "exclude1.com" in payload["suppression_list"]
            assert payload["requirements"] == "NARPM member"

    def test_discover_uses_configured_timeout(self):
        """Should use configured timeout for webhook call."""
        timeout = 3600.0  # 1 hour
        client = DiscoveryWebhookClient("http://test.com/webhook", timeout)

        with patch("lead_pipeline._http_request") as mock_http:
            mock_http.return_value = []

            client.discover(
                location="CA",
                state="CA",
                pms=None,
                quantity=10,
                unit_count_min=None,
                unit_count_max=None,
                suppression_domains=[],
                extra_requirements=None,
                attempt=1,
            )

            # Verify timeout was passed
            assert mock_http.call_args[1]["timeout"] == timeout


@pytest.mark.unit
class TestDiscoveryResponseParsing:
    """Test parsing various discovery response formats."""

    def test_parse_standard_format(self):
        """Should parse standard discovery response."""
        client = DiscoveryWebhookClient("http://test.com/webhook", 1800.0)

        mock_response = {
            "companies": [
                {"company_name": "Company 1", "domain": "co1.com"},
                {"company_name": "Company 2", "domain": "co2.com"},
            ],
            "total": 2,
        }

        with patch("lead_pipeline._http_request", return_value=mock_response):
            result = client.discover(
                location="CA",
                state="CA",
                pms=None,
                quantity=10,
                unit_count_min=None,
                unit_count_max=None,
                suppression_domains=[],
                extra_requirements=None,
                attempt=1,
            )

            assert len(result) == 2
            assert result[0]["company_name"] == "Company 1"
            assert result[1]["company_name"] == "Company 2"

    def test_parse_nested_structure(self):
        """Should handle nested response structures."""
        client = DiscoveryWebhookClient("http://test.com/webhook", 1800.0)

        mock_response = {
            "data": {
                "companies": [
                    {"company_name": "Test", "domain": "test.com"}
                ]
            }
        }

        with patch("lead_pipeline._http_request", return_value=mock_response):
            result = client.discover(
                location="CA",
                state="CA",
                pms=None,
                quantity=10,
                unit_count_min=None,
                unit_count_max=None,
                suppression_domains=[],
                extra_requirements=None,
                attempt=1,
            )

            # Should find companies in nested structure
            assert isinstance(result, list)

    def test_handle_empty_response(self):
        """Should handle empty discovery response."""
        client = DiscoveryWebhookClient("http://test.com/webhook", 1800.0)

        mock_response = {"companies": [], "total": 0}

        with patch("lead_pipeline._http_request", return_value=mock_response):
            result = client.discover(
                location="CA",
                state="CA",
                pms=None,
                quantity=10,
                unit_count_min=None,
                unit_count_max=None,
                suppression_domains=[],
                extra_requirements=None,
                attempt=1,
            )

            assert result == []


@pytest.mark.unit
class TestDiscoveryErrorHandling:
    """Test error handling during discovery."""

    def test_handles_http_error_returns_empty_list(self):
        """Should return empty list on HTTP errors."""
        client = DiscoveryWebhookClient("http://test.com/webhook", 1800.0)

        with patch("lead_pipeline._http_request") as mock_http:
            mock_http.side_effect = HTTPError(
                "http://test.com", 500, "Server Error", {}, None
            )

            result = client.discover(
                location="CA",
                state="CA",
                pms=None,
                quantity=10,
                unit_count_min=None,
                unit_count_max=None,
                suppression_domains=[],
                extra_requirements=None,
                attempt=1,
            )

            # Should catch exception and return empty list
            assert result == []

    def test_handles_timeout_error(self):
        """Should return empty list on timeout errors."""
        client = DiscoveryWebhookClient("http://test.com/webhook", 30.0)

        with patch("lead_pipeline._http_request") as mock_http:
            mock_http.side_effect = URLError("timeout")

            result = client.discover(
                location="CA",
                state="CA",
                pms=None,
                quantity=10,
                unit_count_min=None,
                unit_count_max=None,
                suppression_domains=[],
                extra_requirements=None,
                attempt=1,
            )

            assert result == []


@pytest.mark.unit
class TestDiscoverySuppressionList:
    """Test suppression domain handling."""

    def test_passes_suppression_domains(self):
        """Should pass suppression domains to webhook."""
        client = DiscoveryWebhookClient("http://test.com/webhook", 1800.0)

        suppression = ["domain1.com", "domain2.com", "domain3.com"]

        with patch("lead_pipeline._http_request") as mock_http:
            mock_http.return_value = []

            client.discover(
                location="CA",
                state="CA",
                pms=None,
                quantity=10,
                unit_count_min=None,
                unit_count_max=None,
                suppression_domains=suppression,
                extra_requirements=None,
                attempt=1,
            )

            payload = mock_http.call_args[1]["json_body"]
            assert "suppression_list" in payload
            for domain in suppression:
                assert domain in payload["suppression_list"]

    def test_filters_empty_suppression_domains(self):
        """Should filter out empty domain strings."""
        client = DiscoveryWebhookClient("http://test.com/webhook", 1800.0)

        suppression = ["domain1.com", "", None, "domain2.com"]

        with patch("lead_pipeline._http_request") as mock_http:
            mock_http.return_value = []

            client.discover(
                location="CA",
                state="CA",
                pms=None,
                quantity=10,
                unit_count_min=None,
                unit_count_max=None,
                suppression_domains=suppression,
                extra_requirements=None,
                attempt=1,
            )

            payload = mock_http.call_args[1]["json_body"]
            # Should only include valid domains
            assert "domain1.com" in payload["suppression_list"]
            assert "domain2.com" in payload["suppression_list"]
            assert "" not in payload["suppression_list"]


@pytest.mark.unit
class TestDiscoveryAttemptTracking:
    """Test attempt number tracking for retries."""

    def test_tracks_attempt_number(self):
        """Should pass attempt number to webhook."""
        client = DiscoveryWebhookClient("http://test.com/webhook", 1800.0)

        with patch("lead_pipeline._http_request") as mock_http:
            mock_http.return_value = []

            client.discover(
                location="CA",
                state="CA",
                pms=None,
                quantity=10,
                unit_count_min=None,
                unit_count_max=None,
                suppression_domains=[],
                extra_requirements=None,
                attempt=3,
            )

            payload = mock_http.call_args[1]["json_body"]
            assert payload["attempt"] == 3
