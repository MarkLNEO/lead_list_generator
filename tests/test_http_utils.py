"""
Tests for HTTP utility functions (_http_request, _retry_after_delay).

These utilities handle HTTP communication with retry logic and error handling.
"""

import json
from unittest.mock import Mock, patch
from urllib.error import HTTPError, URLError
import pytest
from lead_pipeline import _http_request, _retry_after_delay


@pytest.mark.unit
@pytest.mark.http
class TestHTTPRequestBasics:
    """Test basic HTTP request functionality."""

    @patch("lead_pipeline.urllib.request.urlopen")
    def test_successful_get_request(self, mock_urlopen):
        """Should handle successful GET request."""
        mock_response = Mock()
        mock_response.read.return_value = b'{"status": "ok"}'
        mock_response.headers.get.return_value = "application/json"
        mock_urlopen.return_value.__enter__.return_value = mock_response

        result = _http_request("GET", "http://example.com/api")

        assert result == {"status": "ok"}
        mock_urlopen.assert_called_once()

    @patch("lead_pipeline.urllib.request.urlopen")
    def test_successful_post_with_json(self, mock_urlopen):
        """Should handle POST request with JSON body."""
        mock_response = Mock()
        mock_response.read.return_value = b'{"created": true}'
        mock_response.headers.get.return_value = "application/json"
        mock_urlopen.return_value.__enter__.return_value = mock_response

        payload = {"name": "Test", "value": 123}
        result = _http_request(
            "POST",
            "http://example.com/api",
            json_body=payload,
        )

        assert result == {"created": True}

    @patch("lead_pipeline.urllib.request.urlopen")
    def test_request_with_query_params(self, mock_urlopen):
        """Should append query parameters to URL."""
        mock_response = Mock()
        mock_response.read.return_value = b'[]'
        mock_response.headers.get.return_value = "application/json"
        mock_urlopen.return_value.__enter__.return_value = mock_response

        _http_request(
            "GET",
            "http://example.com/api",
            params={"state": "CA", "limit": 10},
        )

        # Check that URL includes encoded params
        call_args = mock_urlopen.call_args
        request = call_args[0][0]
        assert "state=CA" in request.full_url
        assert "limit=10" in request.full_url

    @patch("lead_pipeline.urllib.request.urlopen")
    def test_request_with_custom_headers(self, mock_urlopen):
        """Should include custom headers in request."""
        mock_response = Mock()
        mock_response.read.return_value = b'{}'
        mock_response.headers.get.return_value = "application/json"
        mock_urlopen.return_value.__enter__.return_value = mock_response

        headers = {
            "Authorization": "Bearer token123",
            "X-Custom-Header": "value",
        }

        _http_request("GET", "http://example.com/api", headers=headers)

        call_args = mock_urlopen.call_args
        request = call_args[0][0]
        assert request.headers.get("Authorization") == "Bearer token123"
        assert request.headers.get("X-custom-header") == "value"


@pytest.mark.unit
@pytest.mark.http
class TestHTTPRequestRetry:
    """Test HTTP request retry logic."""

    @patch("lead_pipeline.urllib.request.urlopen")
    @patch("lead_pipeline.time.sleep")
    def test_retries_on_500_error(self, mock_sleep, mock_urlopen):
        """Should retry on 5xx server errors."""
        # Fail twice, then succeed
        error = HTTPError("http://test.com", 500, "Server Error", {}, None)
        mock_urlopen.side_effect = [
            error,
            error,
            self._create_success_response(),
        ]

        result = _http_request(
            "GET",
            "http://example.com/api",
            max_retries=3,
        )

        assert result == {"status": "ok"}
        assert mock_urlopen.call_count == 3
        assert mock_sleep.call_count == 2  # Slept between retries

    @patch("lead_pipeline.urllib.request.urlopen")
    @patch("lead_pipeline.time.sleep")
    def test_retries_on_429_rate_limit(self, mock_sleep, mock_urlopen):
        """Should retry on 429 rate limit errors."""
        error = HTTPError("http://test.com", 429, "Too Many Requests", {}, None)
        mock_urlopen.side_effect = [
            error,
            self._create_success_response(),
        ]

        result = _http_request(
            "GET",
            "http://example.com/api",
            max_retries=3,
        )

        assert result == {"status": "ok"}
        assert mock_urlopen.call_count == 2

    @patch("lead_pipeline.urllib.request.urlopen")
    @patch("lead_pipeline.time.sleep")
    def test_exponential_backoff(self, mock_sleep, mock_urlopen):
        """Should use exponential backoff between retries."""
        error = HTTPError("http://test.com", 500, "Server Error", {}, None)
        mock_urlopen.side_effect = [error, error, error, error]

        with pytest.raises(HTTPError):
            _http_request(
                "GET",
                "http://example.com/api",
                max_retries=4,
                retry_backoff=2.0,
            )

        # Check backoff progression: 2s, 4s, 6s
        sleep_calls = [call[0][0] for call in mock_sleep.call_args_list]
        assert sleep_calls == [2.0, 4.0, 6.0]

    @patch("lead_pipeline.urllib.request.urlopen")
    @patch("lead_pipeline.time.sleep")
    def test_respects_retry_after_header(self, mock_sleep, mock_urlopen):
        """Should respect Retry-After header if present."""
        error = HTTPError("http://test.com", 429, "Rate Limited", {"Retry-After": "5"}, None)
        mock_urlopen.side_effect = [
            error,
            self._create_success_response(),
        ]

        _http_request("GET", "http://example.com/api")

        # Should sleep for Retry-After duration
        mock_sleep.assert_called_once_with(5.0)

    @patch("lead_pipeline.urllib.request.urlopen")
    @patch("lead_pipeline.time.sleep")
    def test_retries_on_network_error(self, mock_sleep, mock_urlopen):
        """Should retry on network errors."""
        error = URLError("Connection timeout")
        mock_urlopen.side_effect = [
            error,
            self._create_success_response(),
        ]

        result = _http_request(
            "GET",
            "http://example.com/api",
            max_retries=3,
        )

        assert result == {"status": "ok"}
        assert mock_urlopen.call_count == 2

    @patch("lead_pipeline.urllib.request.urlopen")
    def test_gives_up_after_max_retries(self, mock_urlopen):
        """Should raise error after exceeding max retries."""
        error = HTTPError("http://test.com", 500, "Server Error", {}, None)
        mock_urlopen.side_effect = error

        with pytest.raises(HTTPError):
            _http_request(
                "GET",
                "http://example.com/api",
                max_retries=3,
            )

        assert mock_urlopen.call_count == 3

    @patch("lead_pipeline.urllib.request.urlopen")
    def test_does_not_retry_4xx_client_errors(self, mock_urlopen):
        """Should not retry on 4xx client errors (except 429)."""
        error = HTTPError("http://test.com", 404, "Not Found", {}, None)
        mock_urlopen.side_effect = error

        with pytest.raises(HTTPError):
            _http_request(
                "GET",
                "http://example.com/api",
                max_retries=3,
            )

        # Should fail immediately without retries
        assert mock_urlopen.call_count == 1

    @staticmethod
    def _create_success_response():
        """Helper to create a successful mock response."""
        mock_response = Mock()
        mock_response.read.return_value = b'{"status": "ok"}'
        mock_response.headers.get.return_value = "application/json"
        mock_response.__enter__ = Mock(return_value=mock_response)
        mock_response.__exit__ = Mock(return_value=False)
        return mock_response


@pytest.mark.unit
@pytest.mark.http
class TestHTTPResponseParsing:
    """Test HTTP response parsing."""

    @patch("lead_pipeline.urllib.request.urlopen")
    def test_parses_json_response(self, mock_urlopen):
        """Should parse JSON response."""
        mock_response = Mock()
        mock_response.read.return_value = b'{"key": "value", "number": 42}'
        mock_response.headers.get.return_value = "application/json"
        mock_urlopen.return_value.__enter__.return_value = mock_response

        result = _http_request("GET", "http://example.com/api")

        assert result == {"key": "value", "number": 42}

    @patch("lead_pipeline.urllib.request.urlopen")
    def test_handles_empty_response(self, mock_urlopen):
        """Should handle empty response body."""
        mock_response = Mock()
        mock_response.read.return_value = b''
        mock_response.headers.get.return_value = "application/json"
        mock_urlopen.return_value.__enter__.return_value = mock_response

        result = _http_request("GET", "http://example.com/api")

        assert result == {}

    @patch("lead_pipeline.urllib.request.urlopen")
    def test_falls_back_to_text_on_invalid_json(self, mock_urlopen):
        """Should return text if JSON parsing fails."""
        mock_response = Mock()
        mock_response.read.return_value = b'Not valid JSON'
        mock_response.headers.get.return_value = "text/plain"
        mock_urlopen.return_value.__enter__.return_value = mock_response

        result = _http_request("GET", "http://example.com/api")

        assert result == "Not valid JSON"

    @patch("lead_pipeline.urllib.request.urlopen")
    def test_parses_json_without_content_type_header(self, mock_urlopen):
        """Should attempt JSON parsing even without content-type header."""
        mock_response = Mock()
        mock_response.read.return_value = b'{"success": true}'
        mock_response.headers.get.return_value = None
        mock_urlopen.return_value.__enter__.return_value = mock_response

        result = _http_request("GET", "http://example.com/api")

        assert result == {"success": True}


@pytest.mark.unit
@pytest.mark.http
class TestRetryAfterDelay:
    """Test _retry_after_delay helper function."""

    def test_parses_numeric_retry_after(self):
        """Should parse numeric Retry-After header."""
        error = Mock(spec=HTTPError)
        error.headers = {"Retry-After": "30"}

        delay = _retry_after_delay(error)

        assert delay == 30.0

    def test_returns_none_if_no_header(self):
        """Should return None if no Retry-After header."""
        error = Mock(spec=HTTPError)
        error.headers = {}

        delay = _retry_after_delay(error)

        assert delay is None

    def test_handles_invalid_retry_after(self):
        """Should return None for invalid Retry-After value."""
        error = Mock(spec=HTTPError)
        error.headers = {"Retry-After": "invalid"}

        delay = _retry_after_delay(error)

        assert delay is None

    def test_handles_error_without_headers(self):
        """Should handle error object without headers attribute."""
        error = Mock(spec=HTTPError)
        delattr(error, "headers")

        delay = _retry_after_delay(error)

        assert delay is None
