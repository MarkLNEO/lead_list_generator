# Lead Pipeline Quick Start

## 5-Minute Setup

### 1. Install Python 3.9+
```bash
python3 --version  # Should be 3.9 or higher
```

### 2. Configure Environment
```bash
# Copy template
cp .env.local.example .env.local 2>/dev/null || touch .env.local

# Add required credentials (edit .env.local)
SUPABASE_SERVICE_KEY=<your_key>
HUBSPOT_PRIVATE_APP_TOKEN=<your_token>
N8N_COMPANY_DISCOVERY_WEBHOOK=<webhook_url>
N8N_COMPANY_ENRICHMENT_WEBHOOK=<webhook_url>
```

### 3. Test Configuration
```bash
# Validate setup
python lead_pipeline.py --help

# Should show usage without errors
```

### 4. Run First Pipeline
```bash
# Small test run (2 companies)
python lead_pipeline.py \
  --state CA \
  --pms AppFolio \
  --quantity 2 \
  --log-level INFO

# Check results
ls -la runs/
```

---

## Common Use Cases

### Standard Lead Generation
```bash
# 50 companies in California using AppFolio
python lead_pipeline.py --state CA --pms AppFolio --quantity 50
```

### Filtered Search
```bash
# Mid-size companies in Austin, TX
python lead_pipeline.py \
  --state TX \
  --city Austin \
  --pms Yardi \
  --quantity 25 \
  --unit-min 100 \
  --unit-max 1000
```

### Large Batch
```bash
# 200 companies (use screen for long runs)
screen -S leads
python lead_pipeline.py --state FL --quantity 200
# Ctrl+A D to detach
# screen -r leads to reattach
```

---

## Understanding Results

### Output Structure
```
runs/<run_id>/
├── input.json              # Your search parameters
├── output.json             # Full results with metadata
├── companies.csv           # Importable company list
├── contacts.csv            # Importable contact list
├── metrics.json            # Performance metrics
├── summary.txt             # Human-readable summary
├── run.log                 # Detailed execution log
└── state.json              # Recovery checkpoint
```

### Key Files

**companies.csv**: Import to CRM
- Columns: Name, Website, Location, Units, ICP Score, PMS, Summary

**contacts.csv**: Import to email marketing
- Columns: Name, Title, Email, Location, Professional/Personal Anecdotes

**metrics.json**: Pipeline performance
- API call counts, success rates, timing
- Use for monitoring and optimization

---

## Quick Troubleshooting

### "Health check failed"
```bash
# Check configuration
python -c "from lead_pipeline import Config; Config().validate()"

# Skip health check
SKIP_HEALTH_CHECK=true python lead_pipeline.py ...
```

### "Discovery webhook timeout"
```bash
# Increase timeout
DISCOVERY_REQUEST_TIMEOUT=10800 python lead_pipeline.py ...
```

### "No results returned"
```bash
# Widen search
python lead_pipeline.py --state CA --quantity 10
# (remove --city and unit filters)
```

### Pipeline interrupted
```bash
# Resume from checkpoint
python lead_pipeline.py --state CA --quantity 50
# (same parameters as before)
```

---

## Tips

1. **Start Small**: Test with `--quantity 2` before large runs
2. **Use Screen**: Long runs (50+) should use `screen` or `tmux`
3. **Monitor Progress**: `tail -f runs/<run_id>/run.log`
4. **Check Metrics**: `cat runs/<run_id>/summary.txt` for quick overview
5. **Save Run ID**: Pipeline prints run_id at start - save for reference

---

## Getting Help

- Full documentation: `PRODUCTION_GUIDE.md`
- Check logs: `runs/<run_id>/run.log`
- Email: mark@nevereverordinary.com
