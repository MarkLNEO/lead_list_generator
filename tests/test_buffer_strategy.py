from lead_pipeline import Config, LeadOrchestrator


def make_orchestrator(max_companies_per_run: int = 500) -> LeadOrchestrator:
    config = Config(
        supabase_key="key",
        hubspot_token="token",
        discovery_webhook_url="http://example.com/discovery",
        company_enrichment_webhook="http://example.com/company",
        contact_enrichment_webhook="http://example.com/contact",
        email_verification_webhook="http://example.com/verify",
        contact_discovery_webhook="http://example.com/discover",
        max_companies_per_run=max_companies_per_run,
    )
    return LeadOrchestrator(config)


def test_buffer_multiplier_small_request():
    orchestrator = make_orchestrator()
    target, multiplier = orchestrator._calculate_buffer_target(2)
    assert multiplier == 4.0
    assert target == 8


def test_buffer_multiplier_medium_request():
    orchestrator = make_orchestrator()
    target, multiplier = orchestrator._calculate_buffer_target(18)
    assert multiplier == 2.3
    assert target == 42


def test_buffer_multiplier_large_request():
    orchestrator = make_orchestrator()
    target, multiplier = orchestrator._calculate_buffer_target(70)
    assert multiplier == 1.6
    assert target == 112


def test_buffer_respects_max_cap():
    orchestrator = make_orchestrator(max_companies_per_run=100)
    target, multiplier = orchestrator._calculate_buffer_target(90)
    assert multiplier == 1.5
    assert target == 100
