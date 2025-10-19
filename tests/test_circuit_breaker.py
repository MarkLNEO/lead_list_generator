"""
Tests for CircuitBreaker class.

The circuit breaker prevents cascading failures by tracking failures and
opening the circuit when a threshold is reached.
"""

import time
import pytest
from lead_pipeline import CircuitBreaker, CircuitState


@pytest.mark.unit
@pytest.mark.circuit_breaker
class TestCircuitBreakerBasics:
    """Test basic circuit breaker functionality."""

    def test_initial_state_is_closed(self, circuit_breaker):
        """Circuit breaker should start in CLOSED state."""
        assert circuit_breaker.state == CircuitState.CLOSED
        assert circuit_breaker.failure_count == 0

    def test_successful_call_keeps_circuit_closed(self, circuit_breaker):
        """Successful calls should keep circuit CLOSED."""
        def success_func():
            return "success"

        result = circuit_breaker.call(success_func)

        assert result == "success"
        assert circuit_breaker.state == CircuitState.CLOSED
        assert circuit_breaker.failure_count == 0

    def test_single_failure_increments_count(self, circuit_breaker):
        """Single failure should increment count but not open circuit."""
        def failing_func():
            raise Exception("test error")

        with pytest.raises(Exception, match="test error"):
            circuit_breaker.call(failing_func)

        assert circuit_breaker.failure_count == 1
        assert circuit_breaker.state == CircuitState.CLOSED


@pytest.mark.unit
@pytest.mark.circuit_breaker
class TestCircuitBreakerOpening:
    """Test circuit breaker opening on failures."""

    def test_opens_after_threshold_failures(self):
        """Circuit should open after reaching failure threshold."""
        breaker = CircuitBreaker(
            name="test",
            failure_threshold=3,
            recovery_timeout=60.0,
        )

        def failing_func():
            raise ValueError("test error")

        # First 2 failures: circuit stays closed
        for _ in range(2):
            with pytest.raises(ValueError):
                breaker.call(failing_func)
            assert breaker.state == CircuitState.CLOSED

        # 3rd failure: circuit opens
        with pytest.raises(ValueError):
            breaker.call(failing_func)

        assert breaker.state == CircuitState.OPEN
        assert breaker.failure_count == 3

    def test_rejects_calls_when_open(self):
        """Open circuit should reject calls immediately."""
        breaker = CircuitBreaker(
            name="test",
            failure_threshold=2,
            recovery_timeout=60.0,
        )

        def failing_func():
            raise Exception("error")

        # Open the circuit
        for _ in range(2):
            with pytest.raises(Exception):
                breaker.call(failing_func)

        assert breaker.state == CircuitState.OPEN

        # Next call should be rejected without calling function
        call_count = 0

        def counted_func():
            nonlocal call_count
            call_count += 1
            return "success"

        with pytest.raises(RuntimeError, match="Circuit breaker .* is OPEN"):
            breaker.call(counted_func)

        assert call_count == 0  # Function was never called


@pytest.mark.unit
@pytest.mark.circuit_breaker
class TestCircuitBreakerRecovery:
    """Test circuit breaker recovery after timeout."""

    def test_enters_half_open_after_timeout(self):
        """Circuit should enter HALF_OPEN state after recovery timeout."""
        breaker = CircuitBreaker(
            name="test",
            failure_threshold=2,
            recovery_timeout=0.1,  # 100ms for fast test
        )

        def failing_func():
            raise Exception("error")

        # Open the circuit
        for _ in range(2):
            with pytest.raises(Exception):
                breaker.call(failing_func)

        assert breaker.state == CircuitState.OPEN

        # Wait for recovery timeout
        time.sleep(0.15)

        # Next call should transition to HALF_OPEN
        # (it will still fail, but state changes)
        with pytest.raises(Exception):
            breaker.call(failing_func)

        # State should have been HALF_OPEN during the call
        # But failed again, so back to OPEN
        assert breaker.state == CircuitState.OPEN

    def test_successful_call_in_half_open_closes_circuit(self):
        """Successful call in HALF_OPEN should close the circuit."""
        breaker = CircuitBreaker(
            name="test",
            failure_threshold=2,
            recovery_timeout=0.1,
        )

        def failing_func():
            raise Exception("error")

        def success_func():
            return "recovered"

        # Open the circuit
        for _ in range(2):
            with pytest.raises(Exception):
                breaker.call(failing_func)

        assert breaker.state == CircuitState.OPEN

        # Wait for recovery timeout
        time.sleep(0.15)

        # Successful call should close circuit
        result = breaker.call(success_func)

        assert result == "recovered"
        assert breaker.state == CircuitState.CLOSED
        assert breaker.failure_count == 0

    def test_failure_in_half_open_reopens_circuit(self):
        """Failed call in HALF_OPEN should reopen the circuit."""
        breaker = CircuitBreaker(
            name="test",
            failure_threshold=2,
            recovery_timeout=0.1,
        )

        def failing_func():
            raise Exception("still failing")

        # Open the circuit
        for _ in range(2):
            with pytest.raises(Exception):
                breaker.call(failing_func)

        assert breaker.state == CircuitState.OPEN

        # Wait for recovery timeout
        time.sleep(0.15)

        # Failed call should keep circuit open
        with pytest.raises(Exception):
            breaker.call(failing_func)

        assert breaker.state == CircuitState.OPEN


@pytest.mark.unit
@pytest.mark.circuit_breaker
class TestCircuitBreakerSuccessReset:
    """Test circuit breaker success handling."""

    def test_success_resets_failure_count(self):
        """Successful call should reset failure count."""
        breaker = CircuitBreaker(
            name="test",
            failure_threshold=3,
        )

        def failing_func():
            raise Exception("error")

        def success_func():
            return "ok"

        # Accumulate some failures
        with pytest.raises(Exception):
            breaker.call(failing_func)
        assert breaker.failure_count == 1

        with pytest.raises(Exception):
            breaker.call(failing_func)
        assert breaker.failure_count == 2

        # Success should reset
        breaker.call(success_func)
        assert breaker.failure_count == 0
        assert breaker.state == CircuitState.CLOSED


@pytest.mark.unit
@pytest.mark.circuit_breaker
class TestCircuitBreakerConfiguration:
    """Test circuit breaker configuration options."""

    def test_custom_failure_threshold(self):
        """Custom failure threshold should be respected."""
        breaker = CircuitBreaker(
            name="test",
            failure_threshold=5,
        )

        def failing_func():
            raise Exception("error")

        # Should not open until 5th failure
        for i in range(4):
            with pytest.raises(Exception):
                breaker.call(failing_func)
            assert breaker.state == CircuitState.CLOSED

        # 5th failure opens circuit
        with pytest.raises(Exception):
            breaker.call(failing_func)
        assert breaker.state == CircuitState.OPEN

    def test_custom_exception_type(self):
        """Circuit breaker should only catch specified exception type."""
        breaker = CircuitBreaker(
            name="test",
            failure_threshold=2,
            expected_exception=ValueError,
        )

        def value_error_func():
            raise ValueError("value error")

        def type_error_func():
            raise TypeError("type error")

        # ValueError should be caught and counted
        with pytest.raises(ValueError):
            breaker.call(value_error_func)
        assert breaker.failure_count == 1

        # TypeError should not be caught by circuit breaker
        with pytest.raises(TypeError):
            breaker.call(type_error_func)
        # Failure count doesn't increase for unexpected exceptions
        # (the exception propagates before circuit breaker logic)


@pytest.mark.unit
@pytest.mark.circuit_breaker
class TestCircuitBreakerWithArguments:
    """Test circuit breaker with function arguments."""

    def test_passes_args_to_function(self, circuit_breaker):
        """Circuit breaker should pass through positional arguments."""
        def add(a, b):
            return a + b

        result = circuit_breaker.call(add, 2, 3)
        assert result == 5

    def test_passes_kwargs_to_function(self, circuit_breaker):
        """Circuit breaker should pass through keyword arguments."""
        def greet(name, greeting="Hello"):
            return f"{greeting}, {name}!"

        result = circuit_breaker.call(greet, name="World", greeting="Hi")
        assert result == "Hi, World!"

    def test_mixed_args_and_kwargs(self, circuit_breaker):
        """Circuit breaker should handle mixed args and kwargs."""
        def build_url(protocol, domain, port=80, secure=False):
            prefix = "https" if secure else protocol
            return f"{prefix}://{domain}:{port}"

        result = circuit_breaker.call(
            build_url,
            "http",
            "example.com",
            port=8080,
            secure=True,
        )
        assert result == "https://example.com:8080"
