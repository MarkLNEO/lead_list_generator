# Context Prompt for Next Session

Copy and paste this to Claude Code in your next session:

---

## Project Context

I'm continuing work on the **Lead List Generator** test suite improvement. We're on the `test-suite` branch working to increase test coverage from 66% to 80%+.

### Current Status (as of 2025-10-21)
- ✅ **340 tests passing**, 0 failing, 9 skipped
- ✅ **Coverage: 66.38%** (up from 49.98% at session start)
- 🎯 **Goal**: Push to 80% coverage (+13.6% needed)

### Latest Progress (Session 2025-10-21)
Added **59 new tests** covering:
- ✅ `_enrich_companies_resilient()` - error handling, retries, metrics (6 tests)
- ✅ `evaluate_contact_quality()` - validation logic (5 tests)
- ✅ `_salvage_contact_anecdotes()` - data extraction (5 tests)
- ✅ `_process_single_company()` - company processing (6 tests)
- ✅ `_discover_and_verify_contacts()` - contact discovery flow (9 tests)
- ✅ `LeadOrchestrator.run()` phases - main orchestration (28 tests)

All tests in: `tests/test_orchestrator_run.py`

### Quick Environment Setup

```bash
cd /Users/andrewpyle/PAD/clients/rebar_hq/lead_list_generator
git checkout test-suite
source venv/bin/activate

# Verify current state
pytest -m "not e2e" -q
# Should show: 340 passed, 9 skipped, 7 deselected

# Check coverage
pytest -m "not e2e" --cov=lead_pipeline --cov-report=html
open htmlcov/index.html
```

## What I Need You to Do

### Goal
Add tests for remaining uncovered areas to reach 80% coverage (+13.6% needed).

### Priority 1: _enrich_companies() Method (Lines 2358-2544)
**Estimated Impact**: +8-10% coverage

This is the OLD version of company enrichment (before resilient version was added). It contains ~185 lines of complex nested logic:

**Key Areas to Test**:
1. Person name validation (`_is_valid_person_name` nested function)
   - Reject generic names (office, team, info, etc.)
   - Reject single-token names
   - Reject names with digits or special symbols
   - Require 2+ capitalized tokens

2. Contact processing with quality gates
   - Email verification and validation
   - Role-based email rejection
   - Contact enrichment with retry on quality failure
   - Anecdote quality evaluation (personal, professional, total)

3. Multi-round contact discovery
   - Round 1: Use decision makers from enrichment
   - Round 2: Call contact discovery webhook if needed
   - Stop when max contacts reached

4. Concurrent processing
   - ThreadPoolExecutor for company-level concurrency
   - Nested ThreadPoolExecutor for contact-level concurrency
   - Exception handling for futures

**Test Structure**:
```python
# In tests/test_orchestrator_run.py (or new file)

@pytest.mark.integration
@pytest.mark.orchestrator
class TestEnrichCompaniesLegacy:
    """Test _enrich_companies() legacy method."""

    def test_enrich_validates_person_names(self):
        """Should reject invalid person names."""
        # Test all rejection patterns

    def test_enrich_concurrent_company_processing(self):
        """Should process multiple companies concurrently."""

    def test_enrich_contact_rounds(self):
        """Should attempt 2 rounds of contact discovery."""

    def test_enrich_contact_quality_retry(self):
        """Should re-enrich contacts on quality failure."""

    def test_enrich_respects_max_contacts(self):
        """Should limit to max_contacts_per_company."""
```

### Priority 2: Edge Cases & Error Paths
**Estimated Impact**: +3-5% coverage

**Areas to Cover**:
1. Circuit breaker integration paths (lines with `circuit_breakers.get()`)
2. Notification email sending (`_notify_owner_success`, `_notify_owner_failure`)
3. Metrics report generation (`_generate_metrics_report`)
4. Additional health check scenarios
5. Config edge cases (negative values, extreme limits)

### Files to Reference

**Main implementation**: `lead_pipeline.py` lines 2358-2544
**Existing test patterns**: `tests/test_orchestrator_run.py` (lines 1460-1930)
**Session history**: `docs/testing/SESSION_SUMMARY.md`
**Test fixtures**: `tests/conftest.py`

### Success Criteria

- ✅ Add 20-30 new tests for uncovered areas
- ✅ Achieve at least 75% coverage (stretch: 80%)
- ✅ All tests pass (maintain 100% pass rate)
- ✅ Follow existing test patterns and mocking strategies

### Important Notes

1. **Don't modify** `lead_pipeline.py` - only add tests
2. **Use existing fixtures** from `conftest.py` (`base_config`, `skip_health_check`, etc.)
3. **Follow naming conventions**: `test_<method>_<scenario>_<expected>`
4. **Mark appropriately**: Use `@pytest.mark.integration` and `@pytest.mark.orchestrator`
5. **Keep mocking consistent**: Mock at the appropriate abstraction level
6. **Reference existing tests**: Look at `test_orchestrator_run.py` for patterns

### Coverage Gap Analysis

Run this to see exact missing lines:
```bash
pytest -m "not e2e" --cov=lead_pipeline --cov-report=term-missing | grep "lead_pipeline.py"
```

**Known gaps** (from coverage report):
- Lines 2360-2544: `_enrich_companies()` method
- Lines 863-891, 910-924, 927-932: Email notification methods
- Lines 972-998: Config edge cases
- Lines 3107-3157: Metrics reporting

## Background (Optional Detail)

### What We've Already Done
- ✅ Created 340 tests across 18 test files
- ✅ Fixed all test failures (100% pass rate)
- ✅ Improved coverage from 35% → 66.38% (+31.4%)
- ✅ Added comprehensive orchestrator.run() tests
- ✅ Added enrichment flow tests (_enrich_companies_resilient)
- ✅ Added contact quality validation tests
- ✅ Added contact salvage tests
- ✅ Added contact discovery/verification tests

### What's Already Well-Tested (Skip These)
- ✅ Buffer calculation logic
- ✅ Metrics tracking (basic)
- ✅ Circuit breakers (basic)
- ✅ State management
- ✅ Deduplication
- ✅ Config validation (basic)
- ✅ CLI argument parsing
- ✅ LeadOrchestrator.run() main phases
- ✅ _enrich_companies_resilient() with retries
- ✅ _process_single_company() flow
- ✅ _discover_and_verify_contacts() flow
- ✅ Contact quality validation
- ✅ Contact salvage logic

### What Needs Testing (Focus Here)
- ❌ _enrich_companies() legacy method (lines 2358-2544)
- ❌ Email notifications (_notify_owner_success, _notify_owner_failure)
- ❌ Metrics report generation
- ❌ Circuit breaker integration paths
- ❌ Config edge cases

---

**Read the full session history**: `docs/testing/SESSION_SUMMARY.md`

**Questions?** Ask me to clarify any implementation details before starting.
