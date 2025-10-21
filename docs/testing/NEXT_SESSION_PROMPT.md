# Context Prompt for Next Session

Copy and paste this to Claude Code in your next session:

---

## Project Context

I'm continuing work on the **Lead List Generator** test suite improvement. We're on the `test-suite` branch working to increase test coverage from 50% to 80%+.

### Current Status (as of 2025-10-20)
- ✅ **Option 1 COMPLETE**: Achieved 100% test pass rate
  - **281 tests passing**, 0 failing, 9 skipped
  - **Coverage: 49.98%** (up from 35%)
  - All test failures fixed or properly skipped

- ⏳ **Option 2 IN PROGRESS**: Push to 80% coverage
  - **Gap**: Need +30% coverage (50% → 80%)
  - **Primary target**: LeadOrchestrator.run() method (~425 uncovered lines)

### Quick Environment Setup

```bash
cd /Users/andrewpyle/PAD/clients/rebar_hq/lead_list_generator
git checkout test-suite
source venv/bin/activate

# Verify current state
pytest -m "not e2e" -q
# Should show: 281 passed, 9 skipped, 7 deselected

# Check coverage
pytest -m "not e2e" --cov=lead_pipeline --cov-report=html
open htmlcov/index.html
```

## What I Need You to Do

### Goal
Add comprehensive tests for **LeadOrchestrator.run()** to increase coverage from 50% to at least 60% (stretch goal: 70-80%).

### Approach
The `run()` method (lines 2119-2544 in `lead_pipeline.py`) has 4 main phases that need testing:

1. **Phase 1**: Load companies from Supabase
2. **Phase 2**: Discovery rounds (with max_rounds logic, circuit breakers, suppression)
3. **Phase 3**: Company enrichment (concurrent, with retries)
4. **Phase 4**: Top-up logic when results < target quantity

### Test Structure to Create

```python
# In tests/test_orchestrator_run.py (new file)

@pytest.mark.integration
@pytest.mark.orchestrator
class TestOrchestratorRunMethod:
    """Test LeadOrchestrator.run() orchestration flow."""

    def test_run_phase1_supabase_loading(self):
        """Test Phase 1: Load companies from Supabase."""
        # Mock Supabase to return companies
        # Verify companies are loaded and suppression applied

    def test_run_phase2_discovery_single_round(self):
        """Test Phase 2: Single discovery round."""
        # Mock Supabase returns some companies
        # Mock discovery webhook to return more
        # Verify buffer target calculation and suppression

    def test_run_phase2_discovery_multiple_rounds(self):
        """Test Phase 2: Multiple discovery rounds until buffer filled."""
        # Mock discovery to return incrementally
        # Verify max_rounds is respected

    def test_run_phase3_enrichment(self):
        """Test Phase 3: Company enrichment."""
        # Mock enrichment webhook responses
        # Verify concurrent processing

    def test_run_phase4_topup(self):
        """Test Phase 4: Top-up when results < target."""
        # Mock insufficient results after enrichment
        # Verify top-up discovery triggered

    def test_run_saves_input_and_output_files(self):
        """Test run directory creation and file persistence."""

    def test_run_checkpoints_state(self):
        """Test state checkpointing at each phase."""

    def test_run_handles_discovery_errors(self):
        """Test error handling when discovery fails."""

    def test_run_handles_keyboard_interrupt(self):
        """Test partial results saved on interruption."""
```

### Key Implementation Details

**Mocking Strategy**:
- Use `patch("lead_pipeline._http_request")` for HTTP calls
- Mock file I/O with `patch("builtins.open")`
- Mock StateManager, CircuitBreaker as needed

**Critical Methods in run() Flow**:
```python
# These are called by run() and need mocking
_load_supabase_candidates()     # Calls SupabaseClient
_apply_suppression()            # Calls HubSpotClient
discovery.discover()            # Discovery webhook
_enrich_companies_resilient()   # Enrichment flow
_checkpoint_if_needed()         # State persistence
_finalize_results()             # Save outputs
```

### Files to Reference

**Main implementation**: `lead_pipeline.py` lines 2119-2544
**Existing orchestrator tests**: `tests/test_orchestrator_core.py` (for examples)
**Session history**: `docs/testing/SESSION_SUMMARY.md`
**Test fixtures**: `tests/conftest.py`

### Success Criteria

- ✅ Add 20-30 new tests for run() method
- ✅ Achieve at least 60% coverage (stretch: 70-80%)
- ✅ All tests pass (maintain 100% pass rate)
- ✅ Follow existing test patterns and mocking strategies

### Important Notes

1. **Don't modify** `lead_pipeline.py` - only add tests
2. **Use existing fixtures** from `conftest.py` (`base_config`, etc.)
3. **Follow naming conventions**: `test_run_*` for run() method tests
4. **Mark appropriately**: Use `@pytest.mark.integration` and `@pytest.mark.orchestrator`
5. **Keep mocking consistent**: Mock at the `_http_request` level, not individual clients

## Background (Optional Detail)

### What We've Already Done
- Created 250+ tests across 7 new test files
- Fixed all test failures (12 total)
- Improved coverage from 35% → 50%
- Rewrote DiscoveryWebhookClient and HubSpotClient tests to match actual API

### What's Already Well-Tested (Skip These)
- ✅ Buffer calculation logic
- ✅ Metrics tracking
- ✅ Circuit breakers
- ✅ State management
- ✅ Deduplication
- ✅ Config validation
- ✅ CLI argument parsing

### What Needs Testing (Focus Here)
- ❌ LeadOrchestrator.run() main flow
- ❌ Enrichment flow with retries
- ❌ Contact quality validation
- ❌ Error recovery paths

---

**Read the full session history**: `docs/testing/SESSION_SUMMARY.md`

**Questions?** Ask me to clarify any implementation details before starting.
