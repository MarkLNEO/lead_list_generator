# Test Coverage Improvement Plan
**Target: 35% → 80%+**
**Date**: 2025-10-20

---

## Current State

- **Coverage**: 35% (725/2067 lines)
- **Tests Passing**: 136/143 (95%)
- **E2E Tests**: 7 skipped (expected)

---

## Coverage Gap Analysis

### 1. LeadOrchestrator Main Flow (~500 lines, 0% covered)

**Missing Tests:**
- `LeadOrchestrator.run()` - main pipeline execution
- Discovery rounds and top-up logic
- Buffer calculation and company queuing
- Enrichment orchestration (concurrent processing)
- Contact verification flow
- Contact enrichment with quality gates
- Metrics collection and reporting
- State checkpointing during execution
- Recovery from interruptions

**Priority**: 🔴 CRITICAL (Core business logic)

---

### 2. API Client Methods (~400 lines, ~30% covered)

#### SupabaseResearchClient
**Covered**: Basic query building
**Missing**:
- Error handling for missing columns
- Pagination and limit handling
- Contact insertion/updates
- Request record management
- Column fallback logic

#### HubSpotClient
**Covered**: None
**Missing**:
- Company search by domain
- Activity date filtering
- Batch suppression logic
- Contact creation
- List management
- Error recovery

#### DiscoveryWebhookClient
**Covered**: None
**Missing**:
- Webhook request/response handling
- Final results parsing
- Portal URL parsing
- Timeout handling
- Retry logic

#### N8NEnrichmentClient
**Covered**: Response parsing basics
**Missing**:
- Company enrichment flow
- Decision maker parsing
- Contact discovery integration
- Email verification retries
- Contact enrichment with anecdote validation
- Seed URL and source extraction

**Priority**: 🟡 HIGH (Critical paths)

---

### 3. Configuration & Environment (~100 lines, ~60% covered)

**Missing**:
- `.env.local` file loading edge cases
- Config validation error messages
- Environment variable precedence
- Default value handling
- Malformed config handling

**Priority**: 🟢 MEDIUM

---

### 4. CLI Argument Parsing (~50 lines, 0% covered)

**Missing**:
- Argument parsing (build_arg_parser)
- main() function flow
- Request queue processing mode
- Output file writing
- Exit codes

**Priority**: 🟢 MEDIUM

---

### 5. Email Notifications (~150 lines, 0% covered)

**Missing**:
- Email formatting and sending
- Summary generation
- Attachment handling
- SMTP connection handling
- Failsafe email routing

**Priority**: 🟡 HIGH (Production feature)

---

### 6. Error Recovery Scenarios (~200 lines, ~20% covered)

**Missing**:
- Circuit breaker integration with full pipeline
- State recovery with real orchestrator
- Partial results handling
- Graceful shutdown
- Signal handling

**Priority**: 🟡 HIGH (Reliability)

---

## Implementation Plan

### Phase 1: Core Orchestration (Target: +25% coverage)
**Estimated Time**: 3-4 hours

```
tests/test_orchestrator_core.py
- test_orchestrator_run_basic_flow
- test_orchestrator_buffer_calculation
- test_orchestrator_discovery_rounds
- test_orchestrator_enrichment_flow
- test_orchestrator_metrics_collection
- test_orchestrator_state_checkpointing
```

### Phase 2: API Clients (Target: +20% coverage)
**Estimated Time**: 2-3 hours

```
tests/test_supabase_client.py
- test_find_companies_with_filters
- test_column_fallback_handling
- test_insert_contacts
- test_update_request_records

tests/test_hubspot_client.py
- test_search_company_by_domain
- test_filter_companies_batch
- test_activity_date_parsing
- test_create_contacts

tests/test_discovery_client.py
- test_discovery_webhook_call
- test_parse_final_results
- test_timeout_handling

tests/test_n8n_client.py
- test_enrich_company_full_flow
- test_verify_contact_retries
- test_enrich_contact_quality_gates
```

### Phase 3: CLI & Configuration (Target: +10% coverage)
**Estimated Time**: 1-2 hours

```
tests/test_cli.py
- test_argument_parsing
- test_main_function_flow
- test_request_queue_mode
- test_output_file_writing

tests/test_config_advanced.py
- test_env_file_loading
- test_config_validation_errors
- test_default_values
```

### Phase 4: Email & Recovery (Target: +10% coverage)
**Estimated Time**: 2 hours

```
tests/test_email_notifications.py
- test_send_summary_email
- test_email_formatting
- test_smtp_error_handling

tests/test_error_recovery_advanced.py
- test_circuit_breaker_with_pipeline
- test_state_recovery_full_flow
- test_graceful_shutdown
```

### Phase 5: Edge Cases & Polish (Target: +5% coverage)
**Estimated Time**: 1 hour

```
- Missing error branches
- Rare edge cases
- Logging statements
- Defensive code paths
```

---

## Total Estimate

- **Time**: 9-12 hours
- **New Tests**: ~80-100 test methods
- **Target Coverage**: 80%+
- **Files Created**: 8-10 new test files

---

## Success Criteria

✅ Coverage ≥ 80%
✅ All critical paths tested
✅ Main orchestration flow fully covered
✅ API clients comprehensively tested
✅ Error recovery scenarios validated
✅ All tests passing (>95% pass rate)

---

## Next Steps

1. ✅ Create this plan
2. ⏳ Implement Phase 1 (Orchestrator core tests)
3. ⏳ Implement Phase 2 (API client tests)
4. ⏳ Implement Phase 3 (CLI & config tests)
5. ⏳ Implement Phase 4 (Email & recovery tests)
6. ⏳ Implement Phase 5 (Edge cases)
7. ⏳ Verify 80%+ coverage achieved
8. ⏳ Document any remaining gaps
