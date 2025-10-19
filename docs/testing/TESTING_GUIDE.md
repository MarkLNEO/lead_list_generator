# Testing Guide

**Lead Pipeline Orchestrator - Comprehensive Testing Guide**

---

## Quick Start

```bash
# Install test dependencies
pip install -r requirements-test.txt

# Run all tests
pytest

# Run with coverage report
pytest --cov=lead_pipeline --cov-report=html

# Run specific test types
pytest -m unit              # Unit tests only
pytest -m integration       # Integration tests only
pytest -m "not e2e"         # Everything except e2e tests

# Run tests in parallel
pytest -n auto
```

---

## Test Structure

```
tests/
├── conftest.py                 # Shared fixtures and configuration
├── fixtures/                   # Test data files
│   ├── companies.json
│   ├── contacts.json
│   ├── enriched_companies.json
│   ├── hubspot_responses.json
│   └── webhook_responses.json
├── test_buffer_strategy.py    # Buffer calculation tests
├── test_circuit_breaker.py    # Circuit breaker tests
├── test_contact_discovery.py  # Contact discovery tests
├── test_contact_quality.py    # Quality gate tests
├── test_contact_salvage.py    # Contact salvage tests
├── test_deduplicator.py        # Deduplication tests
├── test_health_check.py        # Health check tests
├── test_http_utils.py          # HTTP utility tests
├── test_state_manager.py       # State persistence tests
├── integration/                # Integration tests
│   └── (coming soon)
└── e2e/                        # End-to-end tests
    └── (coming soon)
```

---

## Test Markers

Tests are organized using pytest markers:

| Marker | Description | Example |
|--------|-------------|---------|
| `unit` | Fast, isolated unit tests | `pytest -m unit` |
| `integration` | Tests with external dependencies | `pytest -m integration` |
| `e2e` | Full pipeline end-to-end tests | `pytest -m e2e` |
| `slow` | Tests that take > 1 second | `pytest -m "not slow"` |
| `circuit_breaker` | Circuit breaker logic tests | `pytest -m circuit_breaker` |
| `state_manager` | State persistence tests | `pytest -m state_manager` |
| `http` | HTTP utilities tests | `pytest -m http` |
| `supabase` | Requires Supabase mock | `pytest -m supabase` |
| `hubspot` | Requires HubSpot mock | `pytest -m hubspot` |
| `webhook` | Requires webhook mock | `pytest -m webhook` |
| `smoke` | Quick smoke tests for CI | `pytest -m smoke` |

---

## Running Tests

### Basic Usage

```bash
# Run all tests
pytest

# Run with verbose output
pytest -v

# Run specific test file
pytest tests/test_circuit_breaker.py

# Run specific test class
pytest tests/test_circuit_breaker.py::TestCircuitBreakerBasics

# Run specific test function
pytest tests/test_circuit_breaker.py::TestCircuitBreakerBasics::test_initial_state_is_closed

# Run tests matching pattern
pytest -k "circuit_breaker"
pytest -k "test_state"
```

### Coverage Reports

```bash
# Terminal coverage report
pytest --cov=lead_pipeline --cov-report=term-missing

# HTML coverage report
pytest --cov=lead_pipeline --cov-report=html
open htmlcov/index.html

# XML coverage report (for CI)
pytest --cov=lead_pipeline --cov-report=xml

# Fail if coverage below threshold
pytest --cov-fail-under=80
```

### Performance

```bash
# Run tests in parallel (4 workers)
pytest -n 4

# Run tests in parallel (auto-detect CPUs)
pytest -n auto

# Show slowest 10 tests
pytest --durations=10
```

### Debugging

```bash
# Stop on first failure
pytest -x

# Show local variables in traceback
pytest --showlocals

# Drop into debugger on failure
pytest --pdb

# Run last failed tests only
pytest --lf

# Run failed tests first, then others
pytest --ff
```

---

## Writing Tests

### Basic Test Structure

```python
import pytest
from lead_pipeline import Config, CircuitBreaker


@pytest.mark.unit
@pytest.mark.circuit_breaker
class TestCircuitBreakerBasics:
    """Test basic circuit breaker functionality."""

    def test_initial_state_is_closed(self, circuit_breaker):
        """Circuit breaker should start in CLOSED state."""
        assert circuit_breaker.state == CircuitState.CLOSED
        assert circuit_breaker.failure_count == 0
```

### Using Fixtures

```python
def test_with_config_fixture(base_config):
    """Use predefined config fixture."""
    assert base_config.supabase_key == "test_supabase_key"


def test_with_custom_config(base_config):
    """Modify fixture for specific test."""
    base_config.enrichment_concurrency = 10
    assert base_config.enrichment_concurrency == 10


def test_with_temp_dir(temp_run_dir):
    """Use temporary directory."""
    assert temp_run_dir.exists()
    test_file = temp_run_dir / "test.txt"
    test_file.write_text("test data")
    assert test_file.exists()
```

### Mocking HTTP Requests

```python
@patch("lead_pipeline.urllib.request.urlopen")
def test_successful_request(mock_urlopen):
    """Mock successful HTTP request."""
    mock_response = Mock()
    mock_response.read.return_value = b'{"status": "ok"}'
    mock_response.headers.get.return_value = "application/json"
    mock_urlopen.return_value.__enter__.return_value = mock_response

    result = _http_request("GET", "http://example.com/api")

    assert result == {"status": "ok"}
```

### Using Test Data Fixtures

```python
def test_with_fixture_data(fixture_loader):
    """Load test data from JSON fixtures."""
    companies = fixture_loader("companies.json")

    assert len(companies) == 5
    assert companies[0]["company_name"] == "Acme Property Management"
```

### Parametrized Tests

```python
@pytest.mark.parametrize("threshold,expected_state", [
    (1, CircuitState.OPEN),
    (3, CircuitState.CLOSED),
    (5, CircuitState.CLOSED),
])
def test_failure_thresholds(threshold, expected_state):
    """Test various failure thresholds."""
    breaker = CircuitBreaker("test", failure_threshold=threshold)
    # ... test logic
```

---

## CI/CD Integration

### GitHub Actions

Tests run automatically on:
- Push to `main` or `test-suite` branches
- Pull requests to `main`

Workflow runs:
1. **Unit Tests**: On Python 3.9, 3.10, 3.11, 3.12
2. **Integration Tests**: On Python 3.11
3. **Linting**: flake8 and black
4. **Type Checking**: mypy

### Local Pre-Commit Checks

```bash
# Run what CI runs
pytest tests/ -m "unit or not (integration or e2e)"
flake8 lead_pipeline.py tests/
black --check lead_pipeline.py tests/
mypy lead_pipeline.py --ignore-missing-imports
```

---

## Test Coverage Goals

| Component | Target | Current |
|-----------|--------|---------|
| **Overall** | 80% | ~60% |
| CircuitBreaker | 100% | 100% |
| StateManager | 100% | 100% |
| HTTP Utils | 95% | 95% |
| ContactDeduplicator | 100% | 100% |
| HealthCheck | 90% | 90% |
| Supabase Client | 80% | 40% |
| HubSpot Client | 80% | 40% |
| Enrichment Client | 75% | 50% |
| LeadOrchestrator | 70% | 30% |

---

## Common Testing Patterns

### Testing Error Handling

```python
def test_handles_http_error():
    """Test error handling for HTTP failures."""
    with pytest.raises(HTTPError, match="500 Server Error"):
        _http_request("GET", "http://failing-server.com")
```

### Testing Async Behavior

```python
def test_concurrent_enrichment(monkeypatch):
    """Test concurrent processing."""
    call_count = 0

    def fake_enrich(*args, **kwargs):
        nonlocal call_count
        call_count += 1
        return {"enriched": True}

    monkeypatch.setattr("lead_pipeline.enrich_company", fake_enrich)

    # Test parallel enrichment
    result = enrich_companies_concurrently(companies, concurrency=3)

    assert call_count == len(companies)
```

### Testing State Persistence

```python
def test_state_recovery(state_manager, sample_companies):
    """Test state save and recovery."""
    # Save state
    state = {"companies": sample_companies, "phase": "enrichment"}
    state_manager.save_checkpoint(state)

    # Load state
    recovered = state_manager.load_checkpoint()

    assert recovered["phase"] == "enrichment"
    assert len(recovered["companies"]) == len(sample_companies)
```

---

## Troubleshooting

### Tests Failing Locally

```bash
# Clear pytest cache
pytest --cache-clear

# Reinstall test dependencies
pip install -r requirements-test.txt --force-reinstall

# Check for import errors
python -c "import lead_pipeline; print('OK')"

# Run single failing test with verbose output
pytest tests/test_xyz.py::test_abc -vv --showlocals
```

### Slow Tests

```bash
# Identify slow tests
pytest --durations=20

# Run only fast tests
pytest -m "not slow"

# Use test parallelization
pytest -n auto
```

### Coverage Not Updating

```bash
# Remove old coverage data
rm -rf .coverage htmlcov/

# Run with fresh coverage
pytest --cov=lead_pipeline --cov-report=html

# Check if source is being collected
pytest --cov=lead_pipeline --cov-report=term-missing -v
```

---

## Best Practices

### DO:
- ✅ Use descriptive test names
- ✅ Add docstrings to test functions
- ✅ Use pytest markers for organization
- ✅ Use fixtures for common setup
- ✅ Mock external dependencies
- ✅ Test both success and failure paths
- ✅ Test edge cases and boundary conditions
- ✅ Keep tests independent and isolated

### DON'T:
- ❌ Test implementation details
- ❌ Use hardcoded paths or credentials
- ❌ Share state between tests
- ❌ Make tests depend on execution order
- ❌ Write tests that require manual intervention
- ❌ Skip writing tests for "simple" functions

---

## Resources

- [pytest Documentation](https://docs.pytest.org/)
- [pytest-cov Documentation](https://pytest-cov.readthedocs.io/)
- [unittest.mock Documentation](https://docs.python.org/3/library/unittest.mock.html)
- [Testing Best Practices](https://docs.python-guide.org/writing/tests/)

---

## Getting Help

- Check test output with `-vv` flag for detailed errors
- Use `--pdb` to drop into debugger on failure
- Review `conftest.py` for available fixtures
- Ask for code review on test-related PRs

---

**Happy Testing!** 🧪
