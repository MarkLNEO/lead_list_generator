# Lead List Orchestrator

A production-ready lead generation pipeline that discovers, enriches, and validates property management companies and their decision-makers.

## Overview

The Lead List Orchestrator is a robust, fault-tolerant system that:
- Discovers property management companies based on location and criteria
- Enriches company data with decision-maker contacts
- Verifies email addresses and enriches contact information
- Applies intelligent filtering for location and property management relevance
- Provides comprehensive logging and error handling

## Features

- 🔄 **Resilient Pipeline**: Automatic retries, circuit breakers, and graceful failure handling
- 📍 **Location Filtering**: Accurate city/state matching with multiple fallback strategies
- 🏢 **Property Management Validation**: Intelligent filtering to ensure relevance
- 📧 **Contact Quality Assurance**: Email verification and contact enrichment
- 📊 **Comprehensive Logging**: All processing logs stored in Supabase for debugging
- ✅ **Zero-Results Protection**: Prevents marking requests as complete with no results
- 🧪 **Nano QA Gate (optional)**: A lightweight AI check (gpt-5-nano) that samples outputs and suggests a small refinement, retrying once if results look off
- 🧪 **Bulletproof Testing**: Comprehensive test suite for pre-deployment validation

## Quick Start

### Prerequisites

1. Python 3.8+
2. Environment variables configured (see Configuration section)
3. Access to required services (Supabase, N8N webhooks, HubSpot)

### Installation

```bash
# Clone the repository
git clone <repository-url>
cd lead_list_orchestrator

# Create and configure environment file
cp .env.local.example .env.local
# Edit .env.local with your credentials

# Run tests to verify setup
python3 test_suite.py
```

### Basic Usage

#### Command Line
```bash
# Run a single request
python3 lead_pipeline.py --state TN --city Memphis --quantity 20 --pms AppFolio

# Process queue from Supabase
python3 queue_worker.py --limit 5
```

#### Queue Processing
The system processes enrichment requests from Supabase:
```json
{
  "parameters": {
    "quantity": 30,
    "priority_locations": ["Memphis, TN"],
    "pms_include": ["AppFolio"],
    "notify_email": "user@example.com"
  }
}
```

## Configuration

### Required Environment Variables

```bash
# Supabase Configuration
SUPABASE_URL=https://your-project.supabase.co
SUPABASE_API_KEY=your-api-key

# N8N Webhook Endpoints
DISCOVERY_WEBHOOK=https://n8n.example.com/webhook/discovery
ENRICHMENT_WEBHOOK=https://n8n.example.com/webhook/enrichment
CONTACT_WEBHOOK=https://n8n.example.com/webhook/contact
EMAIL_VERIFICATION_WEBHOOK=https://n8n.example.com/webhook/verify

# HubSpot Configuration
HUBSPOT_API_KEY=your-hubspot-key
HUBSPOT_COMPANIES_LIST_ID=your-companies-list
HUBSPOT_CONTACTS_LIST_ID=your-contacts-list

# Email Notifications
SMTP_HOST=smtp.gmail.com
SMTP_PORT=587
SMTP_USER=your-email@gmail.com
SMTP_PASSWORD=your-app-password
EMAIL_FROM=Lead Generator <your-email@gmail.com>
NOTIFY_EMAIL=recipient@example.com

# Optional Configuration
DISCOVERY_REQUEST_TIMEOUT=90  # Discovery timeout in seconds
ENRICHMENT_CONCURRENCY=10     # Parallel enrichment workers
CONTACT_CONCURRENCY=20         # Parallel contact workers
REQUEST_PROCESSING_STALE_SECONDS=3600  # Stale request threshold
TOPUP_MAX_ROUNDS=3            # Maximum top-up attempts
CONTACT_MIN_PERSONAL_ANECDOTES=1
CONTACT_MIN_PROFESSIONAL_ANECDOTES=2
CONTACT_MIN_TOTAL_ANECDOTES=3
CONTACT_ALLOW_PERSONALIZATION_FALLBACK=true
CONTACT_ALLOW_SEED_URL_FALLBACK=true
```

## Pipeline Architecture

### Pipeline Phases

1. **Phase 1: Database Query**
   - Query existing companies from Supabase research database
   - Apply location, PMS, and unit count filters
   - Apply HubSpot suppression list

2. **Phase 2: Discovery**
   - Call discovery webhook for new companies if needed
   - Target 1.5x requested quantity for buffer
   - Apply location and property type filters
   - Remove duplicates and suppressed companies

3. **Phase 3: Enrichment**
   - Enrich companies with decision-maker data
   - Verify email addresses (multiple attempts if needed)
   - Enrich contacts with professional/personal anecdotes
   - Apply quality gates (minimum anecdote requirements)

4. **Phase 4: Top-up**
   - Fetch additional companies if target not met
   - Apply all filters consistently (location, property type)
   - Prevent infinite loops with max rounds limit

### Key Components

- **`lead_pipeline.py`**: Main pipeline orchestrator with all core logic
- **`log_capture.py`**: Log capture and storage to Supabase run_logs field
- **`queue_worker.py`**: Queue processing from Supabase enrichment_requests
- **`test_suite.py`**: Comprehensive test suite for validation
- **`deploy_to_ec2.sh`**: Deployment script for EC2 instances

## Testing

Run the comprehensive test suite before deployment:

```bash
# Run all tests
python3 test_suite.py

# Run specific test categories
python3 test_suite.py --test location   # Location parsing
python3 test_suite.py --test property   # Property filtering
python3 test_suite.py --test zero       # Zero results handling
python3 test_suite.py --test log        # Log capture
python3 test_suite.py --test request    # Request processing
python3 test_suite.py --test webhook    # Webhook mocking
python3 test_suite.py --test error      # Error handling
python3 test_suite.py --test config     # Configuration
python3 test_suite.py --test persist    # Data persistence

# Run with verbose output
python3 test_suite.py -v
```

### Pytest Suite (fast, no external APIs except OpenAI)

```bash
# Optional: enable nano QA during tests (keeps payloads tiny)
export OPENAI_API_KEY=... 
export QA_VALIDATOR_ENABLED=true
export QA_MODEL=gpt-5-nano
export SKIP_HEALTH_CHECK=true

# Run pytest
pytest -q

# Run specific tests
pytest -q tests/test_splitter.py
pytest -q tests/test_orchestrator_fakes.py
```

The pytest suite fakes Supabase, n8n, HubSpot, and email calls using in‑memory stubs, so tests run quickly and deterministically. Only OpenAI may be invoked if you enable the nano QA or splitter tests with your API key.

## Deployment

### Deploy to EC2

```bash
# Basic deployment
./deploy_to_ec2.sh user@host

# With custom remote directory
./deploy_to_ec2.sh user@host /path/to/remote/dir

# With SSH key
SSH_OPTS="-i ~/.ssh/your-key" ./deploy_to_ec2.sh ec2-user@10.0.0.1

# Without service setup
./deploy_to_ec2.sh user@host --no-service
```

The deployment script:
- Syncs code to the remote server
- Sets up Python virtual environment
- Installs dependencies
- Configures systemd service for queue processing

### Systemd Service

The queue worker runs as a systemd service on EC2:

```bash
# Service management
sudo systemctl status lead-queue    # Check status
sudo systemctl restart lead-queue   # Restart service
sudo systemctl stop lead-queue      # Stop service
sudo systemctl start lead-queue     # Start service

# View logs
sudo journalctl -u lead-queue -f    # Follow logs
sudo journalctl -u lead-queue -n 100  # Last 100 lines
sudo journalctl -u lead-queue --since "1 hour ago"
```

## Filtering Logic

### Location Filtering

The pipeline applies sophisticated location filtering:

- **Input formats supported:**
  - "Memphis, TN" (city, state)
  - "Memphis TN" (city state)
  - "Tennessee" (state only)
  - "Memphis" (city only)

- **State normalization:**
  - Full names → abbreviations ("Tennessee" → "TN")
  - Case-insensitive matching
  - All US states and DC supported

- **Company field matching:**
  - Primary: `city`, `state`
  - Headquarters: `hq_city`, `hq_state`
  - Other: `location`, `region`, `address`
  - Fallback to any location-related field

### Property Management Filtering

Validates companies are genuine property management firms:

- **Positive signals:**
  - ICP fit = "yes"
  - ICP tier in A, B, C
  - Has PMS software listed
  - Has unit count > 0
  - Contains PM keywords in description

- **Negative signals (disqualifiers):**
  - ICP tier D or F
  - Contains non-PM keywords:
    - "software-as-a-service", "saas company"
    - "hotel software", "vacation rental"
    - "marketing agency", "digital agency"
  - Marked as "not property management"

- **Filtering modes:**
  - **Strict**: Requires positive signals
  - **Loose**: Only excludes on negative signals

## Error Handling

### Circuit Breakers
- Prevent cascading failures when services are down
- Track failure rates and open circuit after threshold
- Automatic recovery with exponential backoff

### Retry Strategy
- Configurable max retries (default: 4)
- Exponential backoff: 2s, 4s, 8s, 16s
- Different strategies for different error types

### State Recovery
- Checkpoint state every 10 companies
- Resume from last checkpoint on interruption
- Preserve partial results on failure

### Zero Results Protection
- Pipeline raises error if 0 companies after filtering
- Prevents marking empty requests as "completed"
- Forces retry with adjusted parameters

## Logging System

### Log Capture
All request processing logs are captured and stored in Supabase:

```json
{
  "run_logs": {
    "summary": {
      "total_logs": 1234,
      "level_counts": {
        "INFO": 1000,
        "WARNING": 200,
        "ERROR": 34
      },
      "key_events": [
        {
          "timestamp": "2025-10-30 12:00:00",
          "level": "INFO",
          "message": "Pipeline complete: 30 fully enriched companies"
        }
      ]
    },
    "full_logs": {
      "captured_at": "2025-10-30T12:00:00",
      "log_count": 1234,
      "logs": [...],
      "raw": "Full log text..."
    }
  }
}
```

### Key Events Tracked
- Pipeline start/complete/failed
- Companies found at each phase
- Filtering statistics
- Enrichment success/failure
- Email verification attempts
- Error occurrences

## Quality Assurance

### Contact Quality Requirements

- **Minimum anecdotes:**
  - Personal: 1+ (configurable)
  - Professional: 2+ (configurable)
  - Total: 3+ (configurable)

- **Fallback strategies:**
  - Personalization fallback for low-quality contacts
  - Seed URL fallback for additional context

### Data Validation

- **Person name validation:**
  - Minimum 4 characters
  - No special characters or numbers
  - Blocks generic terms (e.g., "office", "support")
  - Requires first and last name

- **Email verification:**
  - Multiple attempts with backoff
  - Tracks verification status
  - Only verified emails proceed

- **Company validation:**
  - Domain normalization
  - Duplicate detection
  - Suppression list checking

## Metrics and Monitoring

### Pipeline Metrics
```json
{
  "start_time": 1698765432.123,
  "phases": {
    "discovery": {
      "attempts": 3,
      "companies_found": 150,
      "duration": 45.2
    },
    "enrichment": {
      "processed": 75,
      "succeeded": 70,
      "failed": 5,
      "duration": 120.5
    }
  },
  "totals": {
    "companies_processed": 75,
    "contacts_found": 210,
    "emails_verified": 180,
    "final_delivered": 50
  }
}
```

### Health Checks
```python
# Run health check
python3 -c "from lead_pipeline import Config, HealthChecker; \
            c = Config(); h = HealthChecker(c); \
            ok, errors = h.check_all(); \
            print('Health:', 'OK' if ok else 'FAILED', errors)"
```

## Troubleshooting

### Common Issues

1. **No companies returned**
   - Check location spelling and format
   - Verify PMS software availability in area
   - Review run_logs for filtering details
   - Ensure discovery webhook is responsive

2. **Wrong locations returned**
   - Fixed in October 2025 update
   - Ensure latest version deployed
   - Check priority_locations format
   - Verify location filters in all phases

3. **Pipeline hangs**
   - Check webhook connectivity
   - Review timeout settings (DISCOVERY_REQUEST_TIMEOUT)
   - Look for stale processing requests
   - Verify circuit breakers aren't open

4. **Low contact quality**
   - Adjust anecdote thresholds
   - Enable fallback strategies
   - Check contact enrichment webhook
   - Review personalization settings

### Debug Commands

```bash
# Test location parsing
python3 -c "from lead_pipeline import parse_location_to_city_state; \
            print(parse_location_to_city_state('Memphis, TN'))"

# Test property filtering
python3 -c "from lead_pipeline import evaluate_property_management_status; \
            company = {'pms': 'AppFolio', 'unit_count': 100}; \
            print(evaluate_property_management_status(company, strict=True))"

# Check specific request logs
python3 -c "from lead_pipeline import SupabaseResearchClient, Config; \
            c = Config(); s = SupabaseResearchClient(c); \
            # Query your request by ID"

# Validate configuration
python3 -c "from lead_pipeline import Config; c = Config(); c.validate()"
```

## Recent Updates (October 2025)

### Critical Fixes
- ✅ **Zero Results Bug**: Requests no longer marked "complete" with 0 results
- ✅ **Location Filter Bug**: Top-up phase now properly applies location filters
- ✅ **Filter Consistency**: All pipeline phases apply filters uniformly

### New Features
- ✅ **Log Capture**: Complete request logs stored in Supabase `run_logs` field
- ✅ **Test Suite**: Unified comprehensive testing in `test_suite.py`
- ✅ **Enhanced Logging**: Detailed filter statistics at each stage
- ✅ **Request Format Support**: Handles priority_locations array format

### Improvements
- Better error messages and debugging information
- More robust location parsing (handles edge cases)
- Consistent property management validation
- Protection against incomplete requests

## Performance Considerations

### Concurrency Settings
- **Enrichment**: 10 parallel workers (adjustable)
- **Contacts**: 20 parallel workers (adjustable)
- **Discovery**: Sequential with timeout protection

### Optimization Tips
- Increase concurrency for faster processing (more API load)
- Use circuit breakers to prevent overload
- Monitor webhook response times
- Cache frequently accessed data in Supabase

### Resource Usage
- Memory: ~200MB baseline, scales with concurrency
- CPU: Low, mostly I/O bound
- Network: Depends on webhook response sizes
- Disk: Minimal, only for run artifacts

## Support

For issues or questions:
1. Check `run_logs` field in Supabase for request debugging
2. Run test suite to verify configuration
3. Review this documentation and recent updates
4. Check webhook status and connectivity
5. Verify environment variables are set correctly

## License

Proprietary - All rights reserved

---

*Last updated: October 30, 2025*
*Version: 2.1.0*
