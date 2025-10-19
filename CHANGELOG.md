# Changelog

All notable changes to the Lead Pipeline Orchestrator.

## [2.0.1] - 2025-10-18 - Queue Automation & Discovery Tuning

### ✨ Added
- Supabase queue processor (`--process-request-queue`) now runs pending `enrichment_requests` automatically, updates history, and emails owners on completion or failure.
- Owner notification emails fire on every successful run; failures alert the owner only, keeping clients unaware until the rerun succeeds.

### 🔄 Changed
- Default `DISCOVERY_REQUEST_TIMEOUT` lowered to **1800 seconds (30 minutes)** for faster failure detection; documentation updated with guidance on when to raise it.
- Documentation and production guide refreshed to reflect automated persistence back to Supabase and the new owner notifications.

### 🧪 Testing
- `python -m pytest`
- `./test_production.sh`

---

## [2.0.0] - 2025-10-17 - Production Hardening Release

### 🎯 Major Changes

Complete rebuild with enterprise-grade resilience, observability, and error handling.

### ✨ Added

#### Resilience Features
- **Circuit Breakers**: Prevent cascading failures from external service issues
  - Independent breakers for each service (Supabase, HubSpot, Discovery, Enrichment, Verification)
  - Configurable failure threshold and recovery timeout
  - Auto-recovery with half-open state testing

- **State Persistence**: Auto-checkpoint every 5 minutes
  - Atomic file writes prevent corruption
  - Resume from interruption with zero data loss
  - Incremental results saved every 5 companies

- **Exponential Backoff**: Progressive retry delays
  - 5s → 10s → 20s → 40s progression
  - Prevents hammering failing services
  - Respects Retry-After headers

#### Quality & Validation
- **Contact Deduplication**: Prevent duplicate processing
  - Email-based primary key
  - LinkedIn fallback
  - Name+Company tertiary key
  - Tracks attempts per contact

- **Multi-Level Quality Gates**:
  - Person name validation (reject generic/role names)
  - Email verification with role-based filtering
  - Anecdote quality requirements (3+ personal, 3+ professional)
  - Company domain validation (reject PMS portals)

- **Health Checks**: Pre-flight validation
  - Configuration completeness check
  - Configuration validity (ranges, limits)
  - Network connectivity tests
  - Credential validation

#### Observability
- **Comprehensive Metrics**: Track everything
  - Companies: discovered, enriched, rejected
  - Contacts: discovered, verified, enriched, rejected
  - API calls: per-service success/failure counts
  - Errors: detailed log with context and stack traces
  - Timing: total duration, per-phase breakdown

- **Multiple Output Formats**:
  - `metrics.json`: Machine-readable metrics
  - `summary.txt`: Human-readable dashboard
  - `run.log`: Detailed execution log
  - `state.json`: Recovery checkpoint

#### Configuration
- **Safety Limits**:
  - `MAX_COMPANIES_PER_RUN=500`: Cap per-run costs
  - `MAX_CONTACTS_PER_COMPANY=10`: Limit per-company processing
  - `MAX_ENRICHMENT_RETRIES=2`: Control retry attempts

- **Circuit Breaker Controls**:
  - `CIRCUIT_BREAKER_ENABLED=true`: Enable/disable breakers
  - `CIRCUIT_BREAKER_THRESHOLD=5`: Failures before opening
  - `CIRCUIT_BREAKER_TIMEOUT=300`: Seconds before recovery test

#### Error Handling
- **Graceful Degradation**: Continue on non-fatal errors
  - Discovery fails → Use Supabase + top-up
  - HubSpot down → Skip suppression (with warning)
  - Verification down → Skip contact (don't crash)

- **Partial Results**: Always save progress
  - `partial_companies.json`: Companies discovered before crash
  - `incremental_results.json`: Enriched companies so far
  - Merge and continue on retry

#### Documentation
- **PRODUCTION_GUIDE.md**: 2000+ line operations manual
  - Configuration reference
  - Deployment procedures
  - Monitoring guidelines
  - Troubleshooting runbook
  - Performance tuning
  - Security considerations

- **QUICKSTART.md**: 5-minute setup guide
  - Common use cases
  - Quick troubleshooting
  - Essential tips

- **PRODUCTION_READINESS.md**: Readiness report
  - Feature inventory
  - Risk assessment
  - Testing strategy
  - Deployment checklist
  - Maintenance schedule

- **test_production.sh**: Automated testing script
  - 7 automated tests
  - Configuration validation
  - Connectivity checks
  - Dry run execution

### 🔄 Changed

#### Core Architecture
- Refactored `LeadOrchestrator` with production features
  - Added circuit breaker integration
  - Added state manager integration
  - Added metrics tracking
  - Added error context tracking

- Enhanced `run()` method with phases:
  1. Health checks
  2. Supabase loading
  3. Discovery rounds (with checkpointing)
  4. Enrichment (with retry logic)
  5. Top-up (if needed)
  6. Finalization

#### Enrichment Flow
- Split `_enrich_companies()` into resilient version:
  - `_enrich_companies_resilient()`: Handles errors, checkpoints
  - `_process_single_company()`: Isolated company processing
  - `_discover_and_verify_contacts()`: Multi-round contact discovery

- Added contact processing with deduplication:
  - Check seen contacts before processing
  - Mark contacts as seen to prevent duplicates
  - Track attempt counts

#### Configuration
- Enhanced `Config` class with validation:
  - Strict required field checking
  - Range validation for timeouts
  - Concurrency limit validation
  - Detailed error messages

- Added safety limit fields:
  - `max_companies_per_run`
  - `max_contacts_per_company`
  - `max_enrichment_retries`

#### Logging
- Structured logging throughout
- Error context preservation
- Progress indicators
- Phase-based logging sections

### 🐛 Fixed

#### Critical Issues
- **Contact Duplication**: Same contact enriched multiple times
  - Root cause: No deduplication across rounds
  - Fix: ContactDeduplicator class with unique key generation

- **Attempt Counter Display**: Showed "attempt 9" when max was 8
  - Root cause: Top-up rounds used MAX_ROUNDS + extra
  - Fix: Accurate counter labeling (still cosmetic)

- **Network Connectivity**: Discovery POST stuck without reaching n8n
  - Root cause: No timeout handling
  - Fix: Comprehensive timeout configuration + circuit breakers

- **Supabase Column Errors**: Missing column crashes pipeline
  - Root cause: No validation of table schema
  - Fix: Progressive fallback on missing columns

#### Quality Issues
- **Empty Anecdotes Accepted**: Contacts with null anecdotes passed
  - Root cause: No validation of anecdote quality
  - Fix: Hard gate requiring 3+ non-empty per category + retry logic

- **Role-Based Emails**: Generic emails (office@, info@) passed
  - Root cause: Insufficient email validation
  - Fix: Comprehensive role-based filtering + local part checking

- **Non-Person Names**: Generic terms ("Office Manager") passed
  - Root cause: No name pattern validation
  - Fix: Multi-level name validation (capitalization, tokens, blocklist)

#### Operational Issues
- **Data Loss on Crash**: No recovery mechanism
  - Fix: Auto-checkpointing every 5 minutes

- **No Progress Visibility**: Unknown pipeline state during execution
  - Fix: Real-time metrics + incremental result saves

- **Poor Error Context**: Generic error messages
  - Fix: Detailed error tracking with context and stack traces

### 🗑️ Removed
- None (all existing functionality preserved)

### ⚠️ Breaking Changes
- None (fully backward compatible)

### 📊 Performance
- **Memory**: Remains low (<2GB for 100 companies)
- **Throughput**: Unchanged (bottleneck is external services)
- **Reliability**: Dramatically improved (graceful degradation)

### 🔒 Security
- Added credential validation in health checks
- Enhanced error logging (no credential leakage)
- Documented credential rotation procedures

---

## [1.0.0] - 2025-10-16 - Initial Implementation

### Added
- Single-script architecture
- Supabase integration
- HubSpot suppression
- n8n webhook integration
- Company enrichment
- Contact discovery and enrichment
- Email verification
- CSV export
- Email notifications
- Basic retry logic

### Known Issues
- No circuit breakers
- No state persistence
- Contact duplication
- Poor error handling
- Limited observability
- No health checks
- Brittle failure modes

---

## Migration Guide: 1.0 → 2.0

### Configuration Changes

**New Optional Variables** (with defaults):
```bash
# Circuit breakers (optional, default: enabled)
CIRCUIT_BREAKER_ENABLED=true
CIRCUIT_BREAKER_THRESHOLD=5
CIRCUIT_BREAKER_TIMEOUT=300

# Safety limits (optional, default: shown)
MAX_COMPANIES_PER_RUN=500
MAX_CONTACTS_PER_COMPANY=10
MAX_ENRICHMENT_RETRIES=2
```

**Timeout Recommendations** (existing variables, new recommended values):
```bash
# Old: 480s (8 min)
# New: 1800s (30 minutes) default; increase only if the discovery workflow is progressing but slow
DISCOVERY_REQUEST_TIMEOUT=1800

# Old: 600s (10 min)
# New: 7200s (2 hours)
COMPANY_ENRICHMENT_REQUEST_TIMEOUT=7200
CONTACT_ENRICHMENT_REQUEST_TIMEOUT=7200
```

**Why?** Discovery and enrichment legitimately take 30-60 minutes per batch. Old timeouts caused premature failures.

### Behavior Changes

**Contact Processing**:
- Old: Same contact could be enriched multiple times
- New: Deduplication prevents redundant processing

**Failure Handling**:
- Old: Fail-fast on any error
- New: Graceful degradation, partial results saved

**Progress Tracking**:
- Old: Logs only
- New: Logs + metrics.json + summary.txt + state.json

### Upgrade Steps

1. **Backup existing configuration**:
   ```bash
   cp .env.local .env.local.backup
   ```

2. **Update timeouts** (recommended starting point):
   ```bash
   # Add to .env.local
   DISCOVERY_REQUEST_TIMEOUT=1800
   COMPANY_ENRICHMENT_REQUEST_TIMEOUT=7200
   CONTACT_ENRICHMENT_REQUEST_TIMEOUT=7200
   ```

3. **Test new version**:
   ```bash
   ./test_production.sh
   ```

4. **Run side-by-side comparison** (optional):
   ```bash
   # Old version
   git checkout v1.0.0
   python lead_pipeline.py --state CA --quantity 10 --output results_v1.json

   # New version
   git checkout v2.0.0
   python lead_pipeline.py --state CA --quantity 10 --output results_v2.json

   # Compare
   diff <(jq . results_v1.json) <(jq . results_v2.json)
   ```

5. **Deploy to production**:
   ```bash
   # Pull latest
   git checkout v2.0.0

   # Run production
   python lead_pipeline.py --state TX --quantity 50
   ```

### Rollback Plan

If issues encountered:

```bash
# Rollback to v1.0
git checkout v1.0.0

# Restore old config
cp .env.local.backup .env.local

# Resume operations
python lead_pipeline.py ...
```

---

## Future Roadmap

### Planned Features (v2.1)
- [ ] Async I/O for improved performance
- [ ] Redis caching for HubSpot lookups
- [ ] Webhook retry queues
- [ ] Real-time progress API
- [ ] Slack/Discord notifications

### Under Consideration (v3.0)
- [ ] Distributed execution (Celery/Ray)
- [ ] ML-based quality prediction
- [ ] Dynamic timeout adjustment
- [ ] A/B testing framework
- [ ] GraphQL API

### Community Requests
- [ ] Docker containerization
- [ ] Kubernetes deployment
- [ ] Terraform modules
- [ ] CloudWatch integration
- [ ] Datadog dashboards

---

**Semantic Versioning**: This project follows [SemVer](https://semver.org/)

**Changelog Format**: Based on [Keep a Changelog](https://keepachangelog.com/)
