"""
Integration tests for circuit breaker with real retry scenarios.

Tests circuit breaker behavior integrated with HTTP utilities and
actual retry logic.
"""

import pytest
import time
from unittest.mock import patch, Mock
from urllib.error import HTTPError, URLError
from lead_pipeline import CircuitBreaker, _http_request


@pytest.mark.integration
@pytest.mark.circuit_breaker
@pytest.mark.http
class TestCircuitBreakerWithRetries:
    """Test circuit breaker integrated with HTTP retry logic."""

    def test_circuit_breaker_prevents_wasted_retries(self):
        """Test that circuit breaker stops retries when open."""
        breaker = CircuitBreaker(
            name="test_service",
            failure_threshold=3,
            recovery_timeout=60.0,
        )

        call_count = 0

        def failing_request():
            nonlocal call_count
            call_count += 1
            raise HTTPError("http://test.com", 500, "Server Error", {}, None)

        # Trip the circuit breaker
        for _ in range(3):
            with pytest.raises(HTTPError):
                breaker.call(failing_request)

        # Circuit should be open
        assert breaker.state.value == "open"
        assert call_count == 3

        # Next call should be rejected immediately without calling function
        with pytest.raises(RuntimeError, match="Circuit breaker .* is OPEN"):
            breaker.call(failing_request)

        # Call count should not increase
        assert call_count == 3

    def test_circuit_breaker_with_http_request_integration(self):
        """Test circuit breaker protecting HTTP requests."""
        breaker = CircuitBreaker(
            name="api_service",
            failure_threshold=2,
            recovery_timeout=0.1,
        )

        call_count = 0

        with patch("lead_pipeline.urllib.request.urlopen") as mock_urlopen:
            def failing_urlopen(*args, **kwargs):
                nonlocal call_count
                call_count += 1
                raise HTTPError("http://api.com", 503, "Service Unavailable", {}, None)

            mock_urlopen.side_effect = failing_urlopen

            # First request - fails, circuit still closed
            with pytest.raises(HTTPError):
                breaker.call(
                    _http_request,
                    "GET",
                    "http://api.com/endpoint",
                    max_retries=1,
                )

            # Second request - fails, circuit opens
            with pytest.raises(HTTPError):
                breaker.call(
                    _http_request,
                    "GET",
                    "http://api.com/endpoint",
                    max_retries=1,
                )

            assert breaker.state.value == "open"

            # Third request - rejected by circuit breaker
            with pytest.raises(RuntimeError, match="Circuit breaker .* is OPEN"):
                breaker.call(
                    _http_request,
                    "GET",
                    "http://api.com/endpoint",
                    max_retries=1,
                )

            # Should have only made 2 actual HTTP calls (before circuit opened)
            # max_retries=1 means "try once total, no retries"
            assert call_count == 2  # 2 requests * 1 attempt each

    def test_circuit_breaker_recovery_with_successful_request(self):
        """Test that circuit breaker recovers after timeout with successful request."""
        breaker = CircuitBreaker(
            name="recoverable_service",
            failure_threshold=2,
            recovery_timeout=0.1,  # 100ms for fast test
        )

        with patch("lead_pipeline.urllib.request.urlopen") as mock_urlopen:
            # Fail twice to open circuit
            error = HTTPError("http://test.com", 500, "Error", {}, None)
            mock_urlopen.side_effect = [error, error]

            for _ in range(2):
                with pytest.raises(HTTPError):
                    breaker.call(_http_request, "GET", "http://test.com", max_retries=1)

            assert breaker.state.value == "open"

            # Wait for recovery timeout
            time.sleep(0.15)

            # Next request succeeds
            success_response = Mock()
            success_response.read.return_value = b'{"status": "ok"}'
            success_response.headers.get.return_value = "application/json"
            mock_urlopen.side_effect = None
            mock_urlopen.return_value.__enter__.return_value = success_response

            # Should enter half-open, then close on success
            result = breaker.call(_http_request, "GET", "http://test.com")

            assert result == {"status": "ok"}
            assert breaker.state.value == "closed"
            assert breaker.failure_count == 0


@pytest.mark.integration
@pytest.mark.circuit_breaker
@pytest.mark.slow
class TestCircuitBreakerWithBackoff:
    """Test circuit breaker with exponential backoff."""

    def test_circuit_breaker_with_exponential_backoff(self):
        """Test that circuit breaker works with retry backoff."""
        breaker = CircuitBreaker(
            name="backoff_service",
            failure_threshold=3,
        )

        call_times = []

        with patch("lead_pipeline.urllib.request.urlopen") as mock_urlopen:
            with patch("lead_pipeline.time.sleep") as mock_sleep:
                error = HTTPError("http://test.com", 500, "Error", {}, None)
                mock_urlopen.side_effect = error

                def record_sleep(duration):
                    call_times.append(time.time())

                mock_sleep.side_effect = record_sleep

                # First request - should retry with backoff
                with pytest.raises(HTTPError):
                    breaker.call(
                        _http_request,
                        "GET",
                        "http://test.com",
                        max_retries=4,
                        retry_backoff=1.0,
                    )

                # Should have exponential backoff: 1s, 2s, 3s
                assert mock_sleep.call_count == 3

                # Verify backoff progression
                sleep_durations = [call[0][0] for call in mock_sleep.call_args_list]
                assert sleep_durations == [1.0, 2.0, 3.0]

                assert breaker.failure_count == 1
                assert breaker.state.value == "closed"  # Not reached threshold yet

    def test_circuit_opens_before_retries_exhaust(self):
        """Test that circuit can open during retry attempts."""
        breaker = CircuitBreaker(
            name="fast_fail_service",
            failure_threshold=2,
        )

        with patch("lead_pipeline.urllib.request.urlopen") as mock_urlopen:
            error = HTTPError("http://test.com", 500, "Error", {}, None)
            mock_urlopen.side_effect = error

            # First request fails (1 failure)
            with pytest.raises(HTTPError):
                breaker.call(_http_request, "GET", "http://test.com", max_retries=1)

            assert breaker.failure_count == 1

            # Second request fails (2 failures - should open circuit)
            with pytest.raises(HTTPError):
                breaker.call(_http_request, "GET", "http://test.com", max_retries=1)

            assert breaker.failure_count == 2
            assert breaker.state.value == "open"


@pytest.mark.integration
@pytest.mark.circuit_breaker
class TestCircuitBreakerMultipleServices:
    """Test multiple circuit breakers for different services."""

    def test_independent_circuit_breakers(self):
        """Test that different services have independent circuit breakers."""
        breaker_a = CircuitBreaker("service_a", failure_threshold=2)
        breaker_b = CircuitBreaker("service_b", failure_threshold=2)

        def failing_func():
            raise Exception("Service failed")

        def success_func():
            return "success"

        # Fail service A twice
        for _ in range(2):
            with pytest.raises(Exception):
                breaker_a.call(failing_func)

        # Service A circuit opens
        assert breaker_a.state.value == "open"

        # Service B circuit still closed
        assert breaker_b.state.value == "closed"

        # Service B still works
        result = breaker_b.call(success_func)
        assert result == "success"

        # Service A rejects calls
        with pytest.raises(RuntimeError, match="Circuit breaker .* is OPEN"):
            breaker_a.call(success_func)

    def test_circuit_breaker_metrics_tracking(self):
        """Test tracking failures across multiple calls."""
        breaker = CircuitBreaker(
            name="metrics_service",
            failure_threshold=5,
        )

        def intermittent_func(should_fail):
            if should_fail:
                raise Exception("Failed")
            return "success"

        # Mix of success and failure
        results = []
        for i in range(10):
            try:
                result = breaker.call(intermittent_func, should_fail=(i % 3 == 0))
                results.append("success")
            except Exception:
                results.append("failure")

        # Should have mixed results
        assert "success" in results
        assert "failure" in results

        # Circuit should not be open (failures interspersed with successes)
        assert breaker.state.value == "closed"
        assert breaker.failure_count < breaker.failure_threshold


@pytest.mark.integration
@pytest.mark.circuit_breaker
class TestCircuitBreakerNetworkErrors:
    """Test circuit breaker with various network error types."""

    def test_circuit_breaker_with_url_errors(self):
        """Test circuit breaker handles URLError (network issues)."""
        breaker = CircuitBreaker(
            name="network_service",
            failure_threshold=2,
        )

        with patch("lead_pipeline.urllib.request.urlopen") as mock_urlopen:
            mock_urlopen.side_effect = URLError("Connection timeout")

            # Should fail and increment circuit breaker
            with pytest.raises(URLError):
                breaker.call(_http_request, "GET", "http://test.com", max_retries=1)

            assert breaker.failure_count == 1

    def test_circuit_breaker_differentiates_error_types(self):
        """Test that circuit breaker tracks specific exception types."""
        # Circuit breaker only for ValueError
        breaker = CircuitBreaker(
            name="typed_service",
            failure_threshold=2,
            expected_exception=ValueError,
        )

        # ValueError increments count
        with pytest.raises(ValueError):
            breaker.call(lambda: (_ for _ in ()).throw(ValueError("test")))

        assert breaker.failure_count == 1

        # TypeError doesn't increment (not expected exception)
        with pytest.raises(TypeError):
            breaker.call(lambda: (_ for _ in ()).throw(TypeError("test")))

        # Failure count shouldn't increase for unexpected exceptions
        assert breaker.failure_count == 1
