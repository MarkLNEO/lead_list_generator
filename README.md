# Lead List Orchestrator

**Version 2.0** - Production-Ready Lead Generation Pipeline

Enterprise-grade lead generation system with comprehensive resilience, observability, and quality assurance.

---

## 🚀 Quick Start

```bash
# 1. Configure environment
cp .env.local.example .env.local
# Edit .env.local with your credentials

# 2. Run production test
./test_production.sh

# 3. Execute pipeline
python lead_pipeline.py --state CA --pms AppFolio --quantity 50
```

**Full guide**: See [QUICKSTART.md](docs/QUICKSTART.md)

---

## 📋 What It Does

1. **Search**: Query Supabase database for companies matching your criteria
2. **Discover**: Call n8n discovery webhook to find additional companies
3. **Suppress**: Filter out companies already in HubSpot (active/recent)
4. **Enrich**: Get company details, decision makers, ICP scoring
5. **Verify**: Validate contact emails through verification service
6. **Enrich Contacts**: Research professional and personal anecdotes
7. **Deliver**: Generate CSVs, create HubSpot lists, send email report

---

## ✨ Production Features

### Resilience
- ✅ **Circuit Breakers**: Prevent cascading failures
- ✅ **Auto-Recovery**: Resume from interruption without data loss
- ✅ **Exponential Backoff**: Smart retry with progressive delays
- ✅ **Graceful Degradation**: Continue on non-fatal errors

### Quality
- ✅ **Multi-Level Validation**: Person names, emails, anecdotes, domains
- ✅ **Contact Deduplication**: Prevent duplicate processing
- ✅ **Quality Gates**: 3+ anecdotes (personal + professional) required
- ✅ **Role-Based Filtering**: Reject generic office emails

### Observability
- ✅ **Comprehensive Metrics**: Track companies, contacts, API calls, errors
- ✅ **Health Checks**: Pre-flight validation of config and connectivity
- ✅ **Real-Time Monitoring**: Live logs, incremental results
- ✅ **Detailed Reporting**: JSON metrics + human-readable summaries

### Safety
- ✅ **Configuration Validation**: Strict validation of all parameters
- ✅ **Safety Limits**: Cap costs and prevent runaway processing
- ✅ **Credential Protection**: No exposure in logs or errors
- ✅ **Partial Results**: Always save progress on exit

---

## 📦 Requirements

- Python 3.9+
- Network access to:
  - Supabase (database)
  - HubSpot (CRM)
  - n8n (webhooks)
- Environment variables configured (see `.env.local`)

---

## 🔧 Configuration

### Required Variables

```bash
# Supabase
SUPABASE_SERVICE_KEY=<your_key>
SUPABASE_URL=http://10.0.131.72:8000

# HubSpot
HUBSPOT_PRIVATE_APP_TOKEN=<your_token>

# n8n Webhooks
N8N_COMPANY_DISCOVERY_WEBHOOK=<webhook_url>
N8N_COMPANY_ENRICHMENT_WEBHOOK=<webhook_url>
N8N_CONTACT_DISCOVERY_WEBHOOK=<webhook_url>
N8N_EMAIL_DISCOVERY_VERIFY=<webhook_url>
N8N_CONTACT_ENRICH_WEBHOOK=<webhook_url>
```

### Recommended Settings

```bash
# Timeouts (in seconds)
DISCOVERY_REQUEST_TIMEOUT=1800       # 30 minutes (raise only if discovery is progressing slowly)
COMPANY_ENRICHMENT_REQUEST_TIMEOUT=7200
CONTACT_ENRICHMENT_REQUEST_TIMEOUT=7200
EMAIL_VERIFICATION_REQUEST_TIMEOUT=240

# Concurrency
ENRICHMENT_CONCURRENCY=6
CONTACT_CONCURRENCY=4

# Discovery
DISCOVERY_MAX_ROUNDS=8
TOPUP_MAX_ROUNDS=8
```

**Full reference**: See [PRODUCTION_GUIDE.md](docs/operations/PRODUCTION_GUIDE.md#configuration)

---

## 📖 Documentation

| Document | Purpose | Audience |
|----------|---------|----------|
| [QUICKSTART.md](docs/QUICKSTART.md) | 5-minute setup guide | New users |
| [PRODUCTION_GUIDE.md](docs/operations/PRODUCTION_GUIDE.md) | Complete operations manual | Operators |
| [PRODUCTION_READINESS.md](docs/operations/PRODUCTION_READINESS.md) | Deployment readiness report | Technical leadership |
| [CHANGELOG.md](docs/CHANGELOG.md) | Version history | All |
| [TESTING_GUIDE.md](docs/testing/TESTING_GUIDE.md) | Testing documentation | Developers |

---

## 🎯 Usage Examples

### Basic Run
```bash
python lead_pipeline.py \
  --state CA \
  --pms AppFolio \
  --quantity 50
```

### Filtered Search
```bash
python lead_pipeline.py \
  --state TX \
  --city Austin \
  --pms Yardi \
  --quantity 25 \
  --unit-min 100 \
  --unit-max 1000
```

### Large Batch (use screen)
```bash
screen -S leads
python lead_pipeline.py --state FL --quantity 200
# Ctrl+A D to detach
```

### With Requirements
```bash
python lead_pipeline.py \
  --state CA \
  --pms AppFolio \
  --quantity 50 \
  --requirements "NARPM member preferred"
```

---

## 📊 Output Files

Each run creates a directory: `runs/<run_id>/`

```
runs/20251017_143022_abc123/
├── input.json              # Your search parameters
├── output.json             # Full results with metadata
├── companies.csv           # Importable company list ⭐
├── contacts.csv            # Importable contact list ⭐
├── metrics.json            # Performance metrics
├── summary.txt             # Human-readable dashboard
├── run.log                 # Detailed execution log
├── state.json              # Recovery checkpoint
└── incremental_results.json # Progressive saves
```

**Import to CRM**: Use `companies.csv` and `contacts.csv`

---

## 🔍 Monitoring

### Real-Time Progress
```bash
# Watch log
tail -f runs/<run_id>/run.log

# Monitor metrics
watch -n 5 'jq .metrics runs/<run_id>/metrics.json'

# Check status
cat runs/<run_id>/summary.txt
```

### Key Metrics

- **Company Enrichment Rate**: Target >60%
- **Contact Verification Rate**: Target >50%
- **API Success Rate**: Target >95%

### Health Indicators

```bash
# Check for failures
grep ERROR runs/<run_id>/run.log

# Check circuit breakers
grep "Circuit breaker.*OPEN" runs/<run_id>/run.log

# Check completion rate
jq ".companies_returned / .requested_quantity" runs/<run_id>/output.json
```

---

## 🐛 Troubleshooting

### Quick Fixes

| Issue | Solution |
|-------|----------|
| Health check failed | `SKIP_HEALTH_CHECK=true python lead_pipeline.py ...` |
| Discovery timeout | Increase `DISCOVERY_REQUEST_TIMEOUT=3600` (1h) or `=10800` (3h) |
| No results | Widen search (remove city/unit filters) |
| Pipeline interrupted | Re-run with same parameters (auto-resumes) |

**Full troubleshooting**: See [PRODUCTION_GUIDE.md](docs/operations/PRODUCTION_GUIDE.md#troubleshooting)

---

## 🧪 Testing

```bash
# Run automated test suite
./test_production.sh

# Tests:
# ✓ Python version (3.9+)
# ✓ Script syntax
# ✓ Configuration validation
# ✓ Environment variables
# ✓ Network connectivity
# ✓ Directory structure
# ✓ Dry run (2 companies)
```

---

## 📈 Performance

| Scale | Duration | Companies | Notes |
|-------|----------|-----------|-------|
| Small (10) | 45 min | ~7 enriched | Quick test |
| Medium (50) | 2.5 hrs | ~30-35 enriched | Standard |
| Large (100) | 5 hrs | ~60-70 enriched | Heavy batch |

**Bottleneck**: Discovery webhook (30-60 min per round)

---

## 🔒 Security

- ✅ Credentials via environment variables only
- ✅ No secrets in logs or error messages
- ✅ PII-containing outputs in secure `runs/` directory
- ✅ Pre-flight credential validation
- ✅ HTTPS for all external API calls

**Best practices**:
- Never commit `.env.local`
- Rotate tokens quarterly
- Restrict `runs/` directory: `chmod 700 runs/`
- Delete old runs per retention policy

---

## 🚨 Support

1. **Self-Service**: Check [QUICKSTART.md](docs/QUICKSTART.md) and [PRODUCTION_GUIDE.md](docs/operations/PRODUCTION_GUIDE.md)
2. **Logs**: Review `runs/<run_id>/run.log` for error details
3. **Metrics**: Inspect `runs/<run_id>/metrics.json` for performance data
4. **Contact**: mark@nevereverordinary.com (include run_id and logs)

---

## 📝 Changelog

See [CHANGELOG.md](docs/CHANGELOG.md) for version history.

**Latest**: v2.0.0 - Production hardening release (2025-10-17)

---

## 🎓 Architecture

```
┌─────────────┐
│   Supabase  │ ──→ Existing companies
└─────────────┘

┌─────────────┐
│   HubSpot   │ ──→ Suppression filter
└─────────────┘

┌─────────────┐
│ Discovery   │ ──→ New company discovery
│  Webhook    │     (LLM-based search)
└─────────────┘

┌─────────────┐
│  Company    │ ──→ Enrich with ICP data,
│ Enrichment  │     decision makers
└─────────────┘

┌─────────────┐
│  Contact    │ ──→ Find additional contacts
│ Discovery   │     (if needed)
└─────────────┘

┌─────────────┐
│   Email     │ ──→ Verify contact emails
│Verification │
└─────────────┘

┌─────────────┐
│  Contact    │ ──→ Research anecdotes,
│ Enrichment  │     personalization
└─────────────┘

          ↓

    ┌──────────┐
    │ CSV Files│
    │  + Email │
    └──────────┘
```

---

## 🏆 Production Readiness

✅ **Code Quality**: Comprehensive error handling, type hints, docstrings
✅ **Resilience**: Circuit breakers, retries, state persistence
✅ **Observability**: Metrics, health checks, detailed logging
✅ **Testing**: Automated test suite, documented test strategy
✅ **Documentation**: 4 comprehensive guides covering all aspects
✅ **Operations**: Monitoring, troubleshooting, maintenance procedures
✅ **Security**: Credential protection, data privacy, audit trails

**Status**: READY FOR PRODUCTION DEPLOYMENT

See [PRODUCTION_READINESS.md](docs/operations/PRODUCTION_READINESS.md) for full assessment.

---

## 📄 License

Proprietary - All rights reserved

---

## 👥 Credits

**Developer**: Mark Lerner
**Architecture**: Claude (Anthropic)
**Version**: 2.0.0 - Production Hardening Release
**Date**: 2025-10-17

---

**Ready to go?** → [QUICKSTART.md](docs/QUICKSTART.md) → Run your first pipeline in 5 minutes!
