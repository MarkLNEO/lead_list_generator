# Integration Tests

Integration tests verify that multiple components work together correctly with realistic interactions.

## Test Files

### test_suppression_flow.py
Tests the complete Supabase → HubSpot suppression workflow:
- Query companies from Supabase
- Check each against HubSpot for recent activity
- Filter out companies with recent activity
- Performance tests with 50+ companies

**Run:** `pytest tests/integration/test_suppression_flow.py -v`

### test_state_recovery.py
Tests state persistence and recovery workflows:
- Save and load pipeline checkpoints
- Recovery after interruptions
- Incremental checkpoint updates
- Large state handling
- Corruption recovery

**Run:** `pytest tests/integration/test_state_recovery.py -v`

### test_circuit_breaker_integration.py
Tests circuit breaker integrated with HTTP retries:
- Circuit breaker preventing wasted retries
- Recovery after timeout
- Exponential backoff integration
- Multiple independent circuit breakers
- Network error handling

**Run:** `pytest tests/integration/test_circuit_breaker_integration.py -v`

## Running Integration Tests

```bash
# Run all integration tests
pytest -m integration

# Run specific integration test file
pytest tests/integration/test_suppression_flow.py

# Run with verbose output
pytest -m integration -v

# Run integration tests in parallel
pytest -m integration -n auto

# Skip slow integration tests
pytest -m "integration and not slow"
```

## Test Markers

Integration tests use these markers:
- `@pytest.mark.integration` - All integration tests
- `@pytest.mark.supabase` - Tests requiring Supabase mock
- `@pytest.mark.hubspot` - Tests requiring HubSpot mock
- `@pytest.mark.state_manager` - State persistence tests
- `@pytest.mark.circuit_breaker` - Circuit breaker tests
- `@pytest.mark.slow` - Tests taking > 1 second

## Coverage

Integration tests focus on:
- ✅ Component interactions with mocked HTTP
- ✅ Data flow between services
- ✅ Error handling across components
- ✅ State management workflows
- ✅ Circuit breaker resilience

Integration tests do NOT:
- ❌ Call real external APIs
- ❌ Require live credentials
- ❌ Test implementation details of single functions
- ❌ Duplicate unit test coverage

## Next: End-to-End Tests

See `tests/e2e/` for full pipeline tests (Phase 4).
