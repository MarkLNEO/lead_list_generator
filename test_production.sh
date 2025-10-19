#!/bin/bash
# Production Readiness Test Script
# Tests configuration, connectivity, and performs dry run

set -e

echo "========================================="
echo "Lead Pipeline Production Readiness Test"
echo "========================================="
echo ""

# Colors
RED='\033[0;31m'
GREEN='\033[0;32m'
YELLOW='\033[1;33m'
NC='\033[0m' # No Color

pass() { echo -e "${GREEN}✓${NC} $1"; }
fail() { echo -e "${RED}✗${NC} $1"; }
warn() { echo -e "${YELLOW}⚠${NC} $1"; }
info() { echo "  $1"; }

# Test 1: Python version
echo "Test 1: Python Version"
if python3 --version | grep -qE "Python 3\.(9|1[0-9])"; then
    PYTHON_VERSION=$(python3 --version)
    pass "Python version OK: $PYTHON_VERSION"
else
    fail "Python 3.9+ required"
    exit 1
fi
echo ""

# Test 2: Script syntax
echo "Test 2: Script Syntax"
if python3 -m py_compile lead_pipeline.py 2>/dev/null; then
    pass "Script compiles without errors"
else
    fail "Syntax errors in lead_pipeline.py"
    exit 1
fi
echo ""

# Test 3: Configuration validation
echo "Test 3: Configuration Validation"
if python3 -c "from lead_pipeline import Config; Config().validate()" 2>/dev/null; then
    pass "Configuration valid"
else
    fail "Configuration validation failed"
    warn "Check .env.local for required variables:"
    info "- SUPABASE_SERVICE_KEY"
    info "- HUBSPOT_PRIVATE_APP_TOKEN"
    info "- N8N_COMPANY_DISCOVERY_WEBHOOK"
    exit 1
fi
echo ""

# Test 4: Required environment variables
echo "Test 4: Required Environment Variables"
source .env.local 2>/dev/null || true

check_env() {
    if [ -z "${!1}" ]; then
        fail "$1 not set"
        return 1
    else
        pass "$1 configured"
        return 0
    fi
}

ENV_OK=true
check_env "SUPABASE_SERVICE_KEY" || ENV_OK=false
check_env "HUBSPOT_PRIVATE_APP_TOKEN" || ENV_OK=false
check_env "N8N_COMPANY_DISCOVERY_WEBHOOK" || ENV_OK=false
check_env "N8N_COMPANY_ENRICHMENT_WEBHOOK" || ENV_OK=false

if [ "$ENV_OK" = false ]; then
    fail "Missing required environment variables"
    exit 1
fi
echo ""

# Test 5: Network connectivity
echo "Test 5: Network Connectivity"

# Test Supabase
if curl -s -o /dev/null -w "%{http_code}" --max-time 5 "${SUPABASE_URL:-http://10.0.131.72:8000}" | grep -q "200\|301"; then
    pass "Supabase reachable"
else
    warn "Supabase may be unreachable (${SUPABASE_URL:-http://10.0.131.72:8000})"
fi

# Test HubSpot API
if curl -s -o /dev/null -w "%{http_code}" --max-time 5 \
    -H "Authorization: Bearer $HUBSPOT_PRIVATE_APP_TOKEN" \
    "https://api.hubspot.com/crm/v3/objects/companies?limit=1" | grep -q "200"; then
    pass "HubSpot API accessible"
else
    warn "HubSpot API may be inaccessible"
fi

# Test n8n (if URL contains localhost or 10.0.x.x, skip as may require VPN)
if echo "$N8N_COMPANY_DISCOVERY_WEBHOOK" | grep -qE "localhost|127\.0\.0\.1|10\.0\."; then
    info "n8n webhook is local/private - skipping connectivity test"
else
    if curl -s -o /dev/null -w "%{http_code}" --max-time 5 "$N8N_COMPANY_DISCOVERY_WEBHOOK" | grep -qE "200|405|404"; then
        pass "n8n webhook reachable"
    else
        warn "n8n webhook may be unreachable"
    fi
fi
echo ""

# Test 6: Directory structure
echo "Test 6: Directory Structure"
if [ -d "runs" ]; then
    pass "runs/ directory exists"
else
    mkdir -p runs
    pass "Created runs/ directory"
fi
echo ""

# Test 7: Dry run
echo "Test 7: Dry Run (2 companies)"
echo "Running: python3 lead_pipeline.py --state KS --pms AppFolio --quantity 2 --log-level INFO"
echo ""

if python3 lead_pipeline.py --state KS --pms AppFolio --quantity 2 --log-level INFO; then
    pass "Dry run completed successfully"
    echo ""
    echo "Check results:"
    LATEST_RUN=$(ls -t runs/ | head -1)
    if [ -n "$LATEST_RUN" ]; then
        info "Run directory: runs/$LATEST_RUN"
        info "Summary: runs/$LATEST_RUN/summary.txt"
        info "Companies: runs/$LATEST_RUN/companies.csv"
        info "Contacts: runs/$LATEST_RUN/contacts.csv"
        echo ""
        if [ -f "runs/$LATEST_RUN/summary.txt" ]; then
            cat "runs/$LATEST_RUN/summary.txt"
        fi
    fi
else
    fail "Dry run failed"
    echo ""
    warn "Check logs for details:"
    LATEST_RUN=$(ls -t runs/ | head -1)
    if [ -n "$LATEST_RUN" ] && [ -f "runs/$LATEST_RUN/run.log" ]; then
        info "Log: runs/$LATEST_RUN/run.log"
        echo ""
        echo "Last 20 lines:"
        tail -20 "runs/$LATEST_RUN/run.log"
    fi
    exit 1
fi

echo ""
echo "========================================="
echo -e "${GREEN}✓ Production Readiness: PASS${NC}"
echo "========================================="
echo ""
echo "Ready for production deployment!"
echo "See PRODUCTION_GUIDE.md for deployment instructions"
echo ""
