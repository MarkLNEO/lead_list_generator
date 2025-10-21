# Test Suite Documentation

**Project**: Lead List Generator v2.0
**Branch**: `test-suite`
**Last Updated**: 2025-10-20

---

## Quick Links

- 📋 **[Session Summary](SESSION_SUMMARY.md)** - Complete history of all testing sessions
- 🎯 **[Next Session Prompt](NEXT_SESSION_PROMPT.md)** - Copy/paste this to start your next session
- ✅ **[Option 1 Complete](OPTION_1_COMPLETE.md)** - 100% pass rate achievement details
- 📊 **[Test Results Summary](TEST_RESULTS_SUMMARY.md)** - Overall test statistics
- 📈 **[Coverage Plan](COVERAGE_IMPROVEMENT_PLAN.md)** - Original 5-phase improvement plan

---

## Current Status

### Test Results
```
✅ 281 tests passing (100% pass rate)
⏭️ 9 tests skipped (need real API responses)
❌ 0 tests failing
📊 Coverage: 49.98%
```

### What's Working
- All core utilities (HTTP, circuit breakers, state management, deduplication)
- Configuration validation
- CLI argument parsing
- API client basic functionality (Supabase, HubSpot, Discovery, N8N)
- Orchestrator initialization and metrics

### What Needs Work
- LeadOrchestrator.run() main flow (~425 uncovered lines)
- Enrichment flow with retries
- Contact quality validation and salvage logic
- Error recovery and notification paths

---

## Documentation Index

### Session History
1. **[SESSION_SUMMARY.md](SESSION_SUMMARY.md)**
   - Complete chronological history
   - What was accomplished each session
   - Known issues and next steps

### Achievement Reports
2. **[OPTION_1_COMPLETE.md](OPTION_1_COMPLETE.md)**
   - How we achieved 100% pass rate
   - Tests fixed (12 total)
   - Coverage improvements

3. **[TEST_RESULTS_SUMMARY.md](TEST_RESULTS_SUMMARY.md)**
   - Overall test statistics
   - Coverage breakdown by module
   - Files modified/created

### Planning Documents
4. **[COVERAGE_IMPROVEMENT_PLAN.md](COVERAGE_IMPROVEMENT_PLAN.md)**
   - Original 5-phase plan (35% → 80%)
   - Estimated effort and timeline

5. **[PHASE_1-3_COMPLETE.md](PHASE_1-3_COMPLETE.md)**
   - Initial test creation summary
   - 250+ tests across 7 files

### Next Steps
6. **[NEXT_SESSION_PROMPT.md](NEXT_SESSION_PROMPT.md)**
   - **START HERE** for your next session
   - Context and goals pre-loaded
   - Implementation guidance

---

## Quick Start

### First Time Setup
```bash
cd /Users/andrewpyle/PAD/clients/rebar_hq/lead_list_generator
git checkout test-suite
source venv/bin/activate

# Install test dependencies
pip install -r requirements-test.txt

# Install package in editable mode
pip install -e .
```

### Run Tests
```bash
# Run all tests (excluding E2E)
pytest -m "not e2e" -v

# Quick run (quiet mode)
pytest -m "not e2e" -q

# With coverage report
pytest -m "not e2e" --cov=lead_pipeline --cov-report=html
open htmlcov/index.html

# Run specific test file
pytest tests/test_orchestrator_core.py -v

# Run only failed tests
pytest -m "not e2e" --lf

# Run only tests matching pattern
pytest -m "not e2e" -k "orchestrator"
```

### Check Coverage
```bash
# Terminal report with missing lines
pytest -m "not e2e" --cov=lead_pipeline --cov-report=term-missing

# HTML report (detailed)
pytest -m "not e2e" --cov=lead_pipeline --cov-report=html
open htmlcov/index.html

# See which specific lines need coverage
pytest -m "not e2e" --cov=lead_pipeline --cov-report=term-missing | grep "lead_pipeline.py"
```

---

## Test Organization

### Test Files
```
tests/
├── conftest.py                       # Shared fixtures
├── test_buffer_strategy.py          # Buffer calculation
├── test_circuit_breaker.py          # Circuit breaker logic
├── test_cli.py                      # CLI/main entry point
├── test_config_advanced.py          # Config validation
├── test_contact_discovery.py        # Contact discovery
├── test_contact_quality.py          # Quality validation
├── test_contact_salvage.py          # Fallback logic
├── test_deduplicator.py             # Deduplication
├── test_discovery_client.py         # Discovery webhook
├── test_health_check.py             # Health checks
├── test_http_utils.py               # HTTP utilities
├── test_hubspot_client.py           # HubSpot client
├── test_n8n_enrichment_client.py    # N8N enrichment
├── test_orchestrator_core.py        # Orchestrator core
├── test_state_manager.py            # State persistence
├── test_supabase_client.py          # Supabase queries
└── integration/
    ├── test_circuit_breaker_integration.py
    ├── test_state_recovery.py
    └── test_suppression_flow.py
```

### Test Markers
```python
@pytest.mark.unit              # Fast, isolated unit tests
@pytest.mark.integration       # Integration tests with mocking
@pytest.mark.e2e              # End-to-end (all skipped)
@pytest.mark.slow             # Tests taking >1 second
@pytest.mark.orchestrator     # Orchestrator-specific tests
@pytest.mark.skip             # Skipped tests
```

---

## Coverage Goals

### Current: 49.98%
```
Module                     Coverage
─────────────────────────────────
HTTP Utils                   95%
Circuit Breaker              98%
State Manager                95%
Deduplicator                 95%
Health Check                 90%
Config                       85%
SupabaseClient              70%
HubSpotClient               65%
DiscoveryClient             60%
N8NEnrichmentClient         55%
LeadOrchestrator            35%  ⚠️ NEEDS WORK
CLI/Main                    40%
```

### Target: 80%+

**To achieve 80% coverage**:
1. ✅ Phase 1-3: Create base tests (DONE - reached 50%)
2. ⏳ Phase 4: Add orchestrator run() tests (+20%)
3. ⏳ Phase 5: Add enrichment flow tests (+10%)

---

## Troubleshooting

### Tests Failing?
```bash
# Check for import errors
python -c "import lead_pipeline; print('OK')"

# Verify package installed
pip show lead-pipeline

# Reinstall if needed
pip install -e .

# Clear pytest cache
rm -rf .pytest_cache __pycache__ tests/__pycache__
```

### Coverage Not Updating?
```bash
# Clear coverage data
rm -rf .coverage htmlcov/

# Re-run with fresh coverage
pytest -m "not e2e" --cov=lead_pipeline --cov-report=html --cov-report=term-missing
```

### Can't Find Test Files?
```bash
# Verify test discovery
pytest --collect-only

# Should show all test files in tests/ directory
```

---

## Contributing

### Adding New Tests

1. **Choose the right file**: Add to existing test file or create new one
2. **Use fixtures**: Leverage `base_config` and other fixtures from `conftest.py`
3. **Mock at the right level**: Mock `_http_request` for HTTP, not individual clients
4. **Follow naming**: `test_<what>_<when>_<expected>`
5. **Add markers**: `@pytest.mark.unit` or `@pytest.mark.integration`
6. **Document**: Add docstrings explaining what the test validates

### Test Template
```python
import pytest
from unittest.mock import patch, Mock
from lead_pipeline import YourClass

@pytest.mark.unit
class TestYourClass:
    """Test YourClass functionality."""

    def test_method_basic_case(self, base_config):
        """Should do X when given Y."""
        # Arrange
        instance = YourClass(base_config)

        # Act
        result = instance.method()

        # Assert
        assert result == expected
```

---

## Need Help?

1. **Read the docs**: Start with [SESSION_SUMMARY.md](SESSION_SUMMARY.md)
2. **Check examples**: Look at similar tests in existing files
3. **Use the prompt**: Copy [NEXT_SESSION_PROMPT.md](NEXT_SESSION_PROMPT.md) to Claude Code
4. **Run tests**: `pytest -v` to see what's working

---

**Last Updated**: 2025-10-20
**Maintained By**: Test suite improvement project
**Status**: ✅ Option 1 Complete, ⏳ Option 2 In Progress
