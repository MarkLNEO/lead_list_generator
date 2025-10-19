# Lead Pipeline Production Deployment Guide

## Overview

This guide covers production deployment, monitoring, troubleshooting, and maintenance of the lead generation pipeline.

## Production Features

### Resilience & Reliability
- **Circuit Breakers**: Prevent cascading failures from external services
- **Exponential Backoff**: Progressive retry delays for transient failures
- **State Persistence**: Auto-checkpoint every 5 minutes for recovery
- **Contact Deduplication**: Prevent duplicate processing across enrichment rounds
- **Partial Results**: Save progress on interruption for manual recovery

### Observability
- **Comprehensive Metrics**: Track API calls, success rates, timing
- **Detailed Logging**: Per-run logs with structured error tracking
- **Health Checks**: Pre-flight validation of configuration and connectivity
- **Incremental Saves**: Results saved every 5 companies for recovery

### Safety & Limits
- **Configuration Validation**: Strict validation of all parameters
- **Concurrency Limits**: Bounded parallelism to prevent overload
- **Timeout Controls**: Per-service timeout configuration
- **Quality Gates**: Anecdote validation, role-based email filtering

---

## Configuration

### Required Environment Variables

```bash
# Supabase
SUPABASE_URL=http://10.0.131.72:8000
SUPABASE_SERVICE_KEY=<your_service_key>
SUPABASE_RESEARCH_TABLE=research_database

# HubSpot
HUBSPOT_PRIVATE_APP_TOKEN=<your_token>
HUBSPOT_BASE_URL=https://api.hubspot.com

# n8n Webhooks
N8N_COMPANY_DISCOVERY_WEBHOOK=http://10.0.131.72:5678/webhook/<id>
N8N_COMPANY_ENRICHMENT_WEBHOOK=http://10.0.131.72:5678/webhook/<id>
N8N_CONTACT_DISCOVERY_WEBHOOK=http://10.0.131.72:5678/webhook/<id>
N8N_EMAIL_DISCOVERY_VERIFY=http://10.0.131.72:5678/webhook/<id>
N8N_CONTACT_ENRICH_WEBHOOK=http://10.0.131.72:5678/webhook/<id>

# Email Notifications
SMTP_HOST=smtp.gmail.com
SMTP_PORT=587
SMTP_USER=<your_email>
SMTP_PASSWORD=<your_password>
EMAIL_FROM=automation@rentvine.com
NOTIFICATION_EMAIL=<recipient_email>
FAILSAFE_EMAIL=<fallback_email>
```

### Timeout Configuration

```bash
# Recommended Production Timeouts
DISCOVERY_REQUEST_TIMEOUT=7200          # 2 hours (discovery is slow)
COMPANY_ENRICHMENT_REQUEST_TIMEOUT=7200 # 2 hours
CONTACT_ENRICHMENT_REQUEST_TIMEOUT=7200 # 2 hours
CONTACT_DISCOVERY_REQUEST_TIMEOUT=7200  # 2 hours
EMAIL_VERIFICATION_REQUEST_TIMEOUT=240  # 4 minutes
```

**Important**: Discovery and enrichment can take 30-60 minutes per batch. Set timeouts accordingly.

### Concurrency & Performance

```bash
# Concurrency Settings (tune based on n8n capacity)
ENRICHMENT_CONCURRENCY=6    # Parallel company enrichments
CONTACT_CONCURRENCY=4       # Parallel contact enrichments per company

# Discovery Rounds
DISCOVERY_MAX_ROUNDS=8      # Max discovery attempts
TOPUP_MAX_ROUNDS=8          # Max top-up attempts if target not met
DISCOVERY_ROUND_DELAY=2.0   # Seconds between discovery rounds
```

### Circuit Breaker Configuration

```bash
# Circuit Breaker Settings
CIRCUIT_BREAKER_ENABLED=true        # Enable circuit breakers
CIRCUIT_BREAKER_THRESHOLD=5         # Failures before opening circuit
CIRCUIT_BREAKER_TIMEOUT=300         # Seconds before testing recovery
```

### Safety Limits

```bash
# Safety Limits
MAX_COMPANIES_PER_RUN=500          # Maximum companies per execution
MAX_CONTACTS_PER_COMPANY=10        # Maximum contacts per company
MAX_ENRICHMENT_RETRIES=2           # Retry count for failed enrichments
```

---

## Deployment

### Pre-Flight Checklist

1. **Environment Setup**
   ```bash
   # Copy and configure environment
   cp .env.local.example .env.local
   # Edit .env.local with production credentials
   ```

2. **Health Check**
   ```bash
   # Test configuration
   python lead_pipeline.py --help

   # Dry run (small quantity)
   python lead_pipeline.py --state KS --pms AppFolio --quantity 2
   ```

3. **Network Connectivity**
   - Verify Supabase accessible: `curl http://10.0.131.72:8000/health`
   - Verify n8n accessible: `curl http://10.0.131.72:5678/healthz`
   - Test webhook endpoints with curl

4. **Credentials Validation**
   ```bash
   # Test Supabase
   curl -H "apikey: $SUPABASE_SERVICE_KEY" \
        "$SUPABASE_URL/rest/v1/research_database?limit=1"

   # Test HubSpot
   curl -H "Authorization: Bearer $HUBSPOT_PRIVATE_APP_TOKEN" \
        "https://api.hubspot.com/crm/v3/objects/companies?limit=1"
   ```

### Production Execution

```bash
# Standard production run
python lead_pipeline.py \
  --state CA \
  --pms AppFolio \
  --quantity 50 \
  --log-level INFO

# With optional filters
python lead_pipeline.py \
  --state TX \
  --city Austin \
  --pms "Yardi" \
  --quantity 25 \
  --unit-min 100 \
  --unit-max 5000 \
  --requirements "NARPM member preferred"

# Large batch (use screen/tmux for long runs)
screen -S lead_pipeline
python lead_pipeline.py --state FL --quantity 200
# Ctrl+A D to detach
```

---

## Monitoring

### Real-Time Monitoring

```bash
# Tail run log
tail -f runs/<run_id>/run.log

# Monitor metrics in real-time
watch -n 5 'cat runs/<run_id>/metrics.json | jq ".metrics"'

# Check incremental results
watch -n 10 'jq ".companies_returned" runs/<run_id>/incremental_results.json'
```

### Key Metrics to Watch

1. **Discovery Success Rate**
   - Target: >80% of rounds return companies
   - Alert if: Multiple consecutive failures

2. **Company Enrichment Rate**
   - Target: >60% companies pass enrichment
   - Alert if: <40% success rate

3. **Contact Verification Rate**
   - Target: >50% contacts verified
   - Alert if: <30% success rate

4. **API Call Success Rates**
   - Target: >95% success for all services
   - Alert if: Any service <80%

### Health Indicators

```bash
# Check for circuit breaker trips
grep "Circuit breaker.*OPEN" runs/<run_id>/run.log

# Check error count
jq ".metrics.errors | length" runs/<run_id>/metrics.json

# Check completion rate
jq ".companies_returned / .requested_quantity" runs/<run_id>/output.json
```

---

## Troubleshooting

### Common Issues

#### 1. Health Check Failures

**Symptoms**: Pipeline exits immediately with "Health check failed"

**Diagnosis**:
```bash
# Check configuration
python -c "from lead_pipeline import Config; c = Config(); c.validate()"

# Test connectivity
curl -v http://10.0.131.72:8000
curl -v http://10.0.131.72:5678
```

**Solutions**:
- Verify all required environment variables set
- Check network connectivity to services
- Ensure n8n workflows active and responding
- Skip health check temporarily: `SKIP_HEALTH_CHECK=true python lead_pipeline.py ...`

#### 2. Discovery Timeouts

**Symptoms**: "Discovery webhook attempt X failed" or timeout errors

**Diagnosis**:
```bash
# Check n8n workflow status
curl http://10.0.131.72:5678/webhook/<discovery_webhook_id> \
  -X POST -H "Content-Type: application/json" \
  -d '{"state":"KS","pms":"AppFolio","quantity":1}'

# Check timeout setting
echo $DISCOVERY_REQUEST_TIMEOUT
```

**Solutions**:
- Increase timeout: `DISCOVERY_REQUEST_TIMEOUT=10800` (3 hours)
- Check n8n execution logs for workflow errors
- Verify n8n has sufficient resources (CPU/memory)
- Reduce `quantity` per discovery call

#### 3. Circuit Breaker Opens

**Symptoms**: "Circuit breaker X is OPEN" errors

**Diagnosis**:
```bash
# Check which service is failing
grep "Circuit breaker.*OPEN" runs/<run_id>/run.log

# Check error pattern
grep -A 5 "Circuit breaker.*OPEN" runs/<run_id>/run.log
```

**Solutions**:
- Wait for recovery timeout (default 5 minutes)
- Check external service health
- Increase threshold: `CIRCUIT_BREAKER_THRESHOLD=10`
- Disable circuit breakers temporarily: `CIRCUIT_BREAKER_ENABLED=false`

#### 4. No Contacts Verified

**Symptoms**: Companies enriched but all rejected due to no contacts

**Diagnosis**:
```bash
# Check contact discovery
jq ".metrics.contacts_discovered" runs/<run_id>/metrics.json

# Check verification failures
grep "Email not verified" runs/<run_id>/run.log | wc -l
```

**Solutions**:
- Check email verification webhook responding
- Verify domain resolution working correctly
- Review contact quality filters (may be too strict)
- Check if companies have valid domains (not PMS portals)

#### 5. Insufficient Results

**Symptoms**: Pipeline completes but fewer companies than requested

**Diagnosis**:
```bash
# Check rejection reasons
jq ".metrics.companies_rejected" runs/<run_id>/metrics.json
grep "rejecting" runs/<run_id>/run.log

# Check discovery effectiveness
jq ".metrics.companies_discovered" runs/<run_id>/metrics.json
```

**Solutions**:
- Widen search criteria (remove city filter, expand unit range)
- Increase `TOPUP_MAX_ROUNDS` for more retry attempts
- Check HubSpot suppression not over-filtering
- Review ICP criteria in enrichment workflow

---

## Recovery from Failures

### Checkpoint Recovery

Pipeline auto-saves state every 5 minutes. To resume:

```bash
# Check last checkpoint
cat runs/<run_id>/state.json

# Resume by re-running with same parameters
# Pipeline will skip already-processed companies
python lead_pipeline.py --state KS --pms AppFolio --quantity 50
```

### Partial Results Recovery

If pipeline interrupted:

```bash
# Check partial results
jq ". | length" runs/<run_id>/partial_companies.json

# Check incremental enriched results
jq ". | length" runs/<run_id>/incremental_results.json

# Manually merge and continue
# Edit input.json to exclude already-processed domains
python lead_pipeline.py \
  --state KS \
  --pms AppFolio \
  --quantity <remaining> \
  --exclude domain1.com domain2.com ...
```

---

## Performance Tuning

### Optimizing Throughput

1. **Increase Concurrency** (if n8n can handle):
   ```bash
   ENRICHMENT_CONCURRENCY=10
   CONTACT_CONCURRENCY=6
   ```

2. **Reduce Discovery Rounds** (if Supabase has sufficient data):
   ```bash
   DISCOVERY_MAX_ROUNDS=3
   ```

3. **Parallel Execution** (for multiple states):
   ```bash
   # Run multiple pipelines in parallel
   python lead_pipeline.py --state CA --quantity 50 &
   python lead_pipeline.py --state TX --quantity 50 &
   python lead_pipeline.py --state FL --quantity 50 &
   wait
   ```

### Optimizing Quality

1. **Stricter ICP Filtering**:
   - Adjust enrichment workflow to be more selective
   - Increase unit count minimums

2. **Better Contact Discovery**:
   - Ensure contact discovery webhook returns decision makers
   - Use LinkedIn enrichment for better title matching

3. **Anecdote Quality**:
   - Review contact enrichment workflow prompts
   - Ensure sufficient context provided for personalization

---

## Maintenance

### Regular Tasks

**Daily**:
- Check failed runs: `grep ERROR runs/*/run.log`
- Monitor success rates: Review `runs/*/metrics.json`
- Validate deliveries: Verify email reports received

**Weekly**:
- Clean old run directories: `find runs/ -mtime +30 -delete`
- Review circuit breaker logs for patterns
- Update suppression lists if needed

**Monthly**:
- Review and update configuration based on performance
- Test new webhook endpoints in staging
- Rotate credentials if required

### Log Management

```bash
# Archive old runs
tar -czf runs_archive_$(date +%Y%m).tar.gz runs/
mv runs_archive_*.tar.gz archives/

# Clean old runs (keep last 30 days)
find runs/ -type d -mtime +30 -exec rm -rf {} +

# Aggregate metrics
jq -s '.[] | .metrics' runs/*/metrics.json > monthly_metrics.json
```

---

## Security Considerations

1. **Credential Storage**
   - Never commit `.env.local` to version control
   - Use environment variables or secret management service
   - Rotate tokens quarterly

2. **Data Privacy**
   - Run outputs contain PII (emails, names)
   - Secure `runs/` directory: `chmod 700 runs/`
   - Delete old runs per data retention policy

3. **Network Security**
   - Use VPN for production database access
   - Whitelist IP addresses for webhook endpoints
   - Enable HTTPS for all external API calls

---

## Alerting & Notifications

### Email Notifications

Pipeline automatically sends email on completion:
- **Success**: Sent to `NOTIFICATION_EMAIL` with CSVs attached
- **Failure**: Sent to `FAILSAFE_EMAIL` with error details

### Integrations

For advanced monitoring, integrate with:

```bash
# Slack webhook notification
curl -X POST https://hooks.slack.com/services/YOUR/WEBHOOK/URL \
  -H 'Content-Type: application/json' \
  -d "{\"text\":\"Pipeline completed: $(jq .companies_returned runs/<run_id>/output.json) companies\"}"

# PagerDuty alert on failure
if ! python lead_pipeline.py ...; then
  curl -X POST https://events.pagerduty.com/v2/enqueue \
    -H 'Content-Type: application/json' \
    -d '{"routing_key":"YOUR_KEY","event_action":"trigger","payload":{...}}'
fi
```

---

## FAQ

**Q: How long does a typical run take?**
A: For 50 companies: 2-4 hours. Discovery is the slowest phase (~30-60 min per round).

**Q: Can I pause and resume a run?**
A: Yes, Ctrl+C to interrupt. State saved in checkpoints. Re-run with same parameters to resume.

**Q: Why are some companies rejected?**
A: Common reasons: no decision makers found, no valid emails verified, insufficient anecdotes.

**Q: How do I increase the buffer?**
A: Buffer is automatic (2x for <10 companies, 1.5x otherwise). To override, modify code line ~880.

**Q: Can I run multiple pipelines simultaneously?**
A: Yes, but ensure n8n and external services can handle concurrent load. Start with 2-3 parallel runs.

**Q: What happens if n8n goes down mid-run?**
A: Circuit breakers will trip after 5 failures. Pipeline will save partial results and exit gracefully.

**Q: How do I test changes without affecting production?**
A: Use staging webhooks in `.env.local` and run with `--quantity 2` for fast validation.

---

## Support & Escalation

For issues not covered in this guide:

1. Check `runs/<run_id>/run.log` for detailed error messages
2. Review `runs/<run_id>/metrics.json` for performance data
3. Inspect n8n workflow execution logs
4. Contact: mark@nevereverordinary.com
