# Test Coverage Analysis & Improvement Plan

**Date**: 2025-10-19
**Branch**: test-suite
**Current Coverage**: ~15% (15 tests / 86 methods)

---

## Current State

### Existing Tests (15 total)

| File | Tests | Coverage Area |
|------|-------|---------------|
| `test_buffer_strategy.py` | 4 | Buffer calculation logic |
| `test_contact_discovery.py` | 4 | Contact extraction & deduplication |
| `test_contact_quality.py` | 5 | Quality gates & validation |
| `test_contact_salvage.py` | 2 | Anecdote salvage logic |

### Test Infrastructure Issues

❌ **Missing Infrastructure:**
- No `pytest` in requirements
- No `pytest.ini` or configuration
- No CI/CD configuration
- No test fixtures or mocking utilities
- No coverage reporting
- No integration/e2e test structure

---

## Coverage Gaps

### Critical Gaps (High Priority)

**1. Circuit Breaker (CircuitBreaker class)**
- ✅ State transitions (CLOSED → OPEN → HALF_OPEN)
- ✅ Failure threshold triggering
- ✅ Recovery timeout behavior
- ✅ Success resets

**2. State Management (StateManager class)**
- ✅ Checkpoint saving/loading
- ✅ Recovery from interruption
- ✅ Atomic file writes
- ✅ Checkpoint interval logic

**3. HTTP Utilities (_http_request)**
- ✅ Retry logic with exponential backoff
- ✅ 429 rate limit handling
- ✅ 5xx server error retries
- ✅ Timeout handling
- ✅ JSON vs text response parsing

**4. Health Checks (HealthCheck class)**
- ✅ Configuration validation
- ✅ Connectivity checks
- ✅ Credential validation

**5. Contact Deduplication (ContactDeduplicator class)**
- ✅ Email-based deduplication
- ✅ LinkedIn fallback
- ✅ Name+Company fallback
- ✅ Attempt counting

### Important Gaps (Medium Priority)

**6. Supabase Client (SupabaseResearchClient)**
- ✅ Company queries with filters
- ✅ Request queue operations
- ✅ Company/contact persistence
- ✅ Error handling

**7. HubSpot Client (HubSpotClient)**
- ✅ Domain search
- ✅ Suppression logic (recent activity check)
- ✅ Batch filtering
- ✅ Static list creation
- ✅ List membership management

**8. Discovery Webhook (DiscoveryWebhookClient)**
- ✅ Company discovery requests
- ✅ Response parsing
- ✅ Timeout handling
- ✅ Domain suppression

**9. Enrichment Client (N8NEnrichmentClient)**
- ✅ Company enrichment
- ✅ Contact enrichment
- ✅ Email verification
- ✅ Response parsing edge cases
- ✅ Concurrent enrichment

**10. Pipeline Orchestration (LeadOrchestrator)**
- ✅ Phase 1: Supabase loading
- ✅ Phase 2: Discovery rounds
- ✅ Phase 3: Company enrichment
- ✅ Phase 4: Contact enrichment
- ✅ Top-up logic
- ✅ Error recovery
- ✅ Metrics tracking

### Nice-to-Have (Lower Priority)

**11. Output Generation**
- ✅ CSV generation
- ✅ JSON output formatting
- ✅ Metrics reporting
- ✅ Summary generation

**12. Notification System**
- ✅ Email delivery
- ✅ Success notifications
- ✅ Failure notifications

**13. Configuration (Config class)**
- ✅ Environment variable parsing
- ✅ Validation logic
- ✅ Default values

**14. Validation Utilities**
- ✅ `evaluate_contact_quality()`
- ✅ Person name validation
- ✅ Email validation
- ✅ Domain validation

---

## Test Types Needed

### 1. Unit Tests (Expand Coverage)

**Target: 80%+ coverage**

- All classes with isolated mocking
- Edge cases for each method
- Error handling paths
- Validation logic

### 2. Integration Tests (New)

**Test real component interactions:**

- Supabase → HubSpot suppression flow
- Discovery → Enrichment pipeline
- Contact verification → Enrichment flow
- State persistence → Recovery
- Circuit breaker integration with real retries

### 3. End-to-End Tests (New)

**Test complete pipeline flows:**

- Small batch (5 companies) end-to-end
- Error recovery scenarios
- Interrupt & resume
- Various filter combinations
- Quality gate rejections

### 4. Performance Tests (New)

- Concurrent enrichment stress test
- Large batch processing (100+ companies)
- Memory usage monitoring
- API rate limit handling

---

## Implementation Plan

### Phase 1: Test Infrastructure (Days 1-2)

1. Create `requirements-test.txt` with pytest, pytest-cov, pytest-mock
2. Create `pytest.ini` with configuration
3. Create `tests/conftest.py` with shared fixtures
4. Add `tests/fixtures/` for test data
5. Set up GitHub Actions CI/CD workflow
6. Add coverage reporting

### Phase 2: Expand Unit Tests (Days 3-5)

1. **CircuitBreaker tests** (test_circuit_breaker.py)
2. **StateManager tests** (test_state_manager.py)
3. **HTTP utilities tests** (test_http_utils.py)
4. **ContactDeduplicator tests** (test_deduplicator.py)
5. **HealthCheck tests** (test_health_check.py)
6. **Supabase client tests** (test_supabase_client.py)
7. **HubSpot client tests** (test_hubspot_client.py)
8. **Enrichment client tests** (expand test_contact_discovery.py)
9. **Validation utilities tests** (expand test_contact_quality.py)

### Phase 3: Integration Tests (Days 6-7)

1. Create `tests/integration/` directory
2. **test_suppression_flow.py** - Supabase → HubSpot flow
3. **test_discovery_enrichment.py** - Discovery → Enrichment pipeline
4. **test_state_recovery.py** - Checkpoint → Resume flow
5. **test_circuit_breaker_integration.py** - Circuit breaker with real retries

### Phase 4: End-to-End Tests (Days 8-9)

1. Create `tests/e2e/` directory
2. **test_small_batch.py** - 5-company end-to-end with mocked webhooks
3. **test_error_scenarios.py** - Error handling and recovery
4. **test_quality_gates.py** - Contact quality rejection flows
5. **test_interrupt_resume.py** - Interrupt and resume scenarios

### Phase 5: Documentation & CI/CD (Day 10)

1. Update `TEST_ANALYSIS.md` with new coverage
2. Create `TESTING_GUIDE.md` for developers
3. Set up GitHub Actions workflow
4. Configure code coverage reporting
5. Add test badges to README

---

## Success Metrics

- ✅ **Unit Test Coverage**: 80%+ line coverage
- ✅ **Integration Tests**: 10+ integration test scenarios
- ✅ **E2E Tests**: 5+ complete pipeline tests
- ✅ **CI/CD**: Automated test runs on all PRs
- ✅ **Documentation**: Comprehensive testing guide
- ✅ **Speed**: Full test suite runs in < 5 minutes

---

## Risks & Mitigations

| Risk | Mitigation |
|------|------------|
| External API mocking complexity | Use pytest-mock and httpretty for HTTP mocking |
| Long-running tests slow CI | Separate unit/integration/e2e with pytest marks |
| State persistence test flakiness | Use temporary directories and atomic operations |
| Webhook mocking realism | Create realistic fixture data from production runs |

---

## Next Steps

1. **Approve this plan** with any modifications
2. **Start Phase 1**: Test infrastructure setup
3. **Iteratively implement** Phases 2-5
4. **Review and adjust** based on findings

---

**Estimated Completion**: 10 days (assumes 4-6 hours/day)
