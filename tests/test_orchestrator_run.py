"""
Tests for LeadOrchestrator.run() orchestration flow.

This module tests the main run() method which coordinates the entire pipeline:
- Phase 1: Load companies from Supabase
- Phase 2: Discovery rounds with circuit breakers and suppression
- Phase 3: Company enrichment with concurrent processing
- Phase 4: Top-up logic when results < target
- Error handling and recovery
- State checkpointing
- File persistence
"""

import pytest
from unittest.mock import Mock, patch, MagicMock, call, mock_open
from argparse import Namespace
from pathlib import Path
import json
import time
import os
import logging
from datetime import datetime, timezone

from lead_pipeline import (
    LeadOrchestrator,
    Config,
)


@pytest.fixture
def mock_args():
    """Mock command line arguments for run()."""
    return Namespace(
        state="CA",
        city=None,
        location="San Francisco, CA",
        pms="AppFolio",
        quantity=5,
        unit_min=None,
        unit_max=None,
        requirements=None,
        exclude=[],
        max_rounds=3,
    )


@pytest.fixture
def skip_health_check(monkeypatch):
    """Skip health checks for tests."""
    monkeypatch.setenv("SKIP_HEALTH_CHECK", "1")
    yield
    monkeypatch.delenv("SKIP_HEALTH_CHECK", raising=False)


@pytest.fixture
def mock_file_handler():
    """Create a properly configured mock file handler."""
    mock_handler = MagicMock()
    mock_handler.level = logging.INFO
    mock_handler.setLevel = MagicMock()
    mock_handler.setFormatter = MagicMock()
    return mock_handler


@pytest.fixture
def mock_http():
    """Mock HTTP requests to prevent real network calls."""
    with patch("lead_pipeline._http_request", return_value={}) as mock_http_request:
        yield mock_http_request


@pytest.fixture
def mock_run_setup(mock_file_handler):
    """Mock file system operations and logging for run()."""
    with patch("lead_pipeline.Path.mkdir") as mock_mkdir, \
         patch("lead_pipeline.Path.write_text") as mock_write, \
         patch("logging.FileHandler", return_value=mock_file_handler) as mock_handler_class:
        yield {
            "mkdir": mock_mkdir,
            "write_text": mock_write,
            "handler": mock_file_handler,
            "handler_class": mock_handler_class,
        }


@pytest.mark.integration
@pytest.mark.orchestrator
class TestOrchestratorRunPhase1:
    """Test Phase 1: Load companies from Supabase and apply suppression."""

    def test_run_phase1_loads_from_supabase(self, base_config, mock_http, mock_args, tmp_path, skip_health_check, mock_file_handler):
        """Should load companies from Supabase in Phase 1."""
        orchestrator = LeadOrchestrator(base_config)

        # Mock Supabase to return 3 companies
        supabase_companies = [
            {"company_name": "Company A", "domain": "companya.com", "hq_state": "CA"},
            {"company_name": "Company B", "domain": "companyb.com", "hq_state": "CA"},
            {"company_name": "Company C", "domain": "companyc.com", "hq_state": "CA"},
        ]

        with patch.object(orchestrator.supabase, "find_existing_companies", return_value=supabase_companies), \
             patch.object(orchestrator.hubspot, "filter_companies", side_effect=lambda x: x), \
             patch.object(orchestrator, "_enrich_companies_resilient", return_value=[]), \
             patch.object(orchestrator, "_finalize_results", return_value={}), \
             patch.object(orchestrator, "_notify_owner_success"), \
             patch.object(orchestrator, "_generate_metrics_report"), \
             patch("lead_pipeline.Path.mkdir"), \
             patch("lead_pipeline.Path.write_text"), \
             patch("logging.FileHandler", return_value=mock_file_handler):

            result = orchestrator.run(mock_args)

            # Verify Supabase was queried
            orchestrator.supabase.find_existing_companies.assert_called_once()
            call_kwargs = orchestrator.supabase.find_existing_companies.call_args[1]
            assert call_kwargs["state"] == "CA"
            assert call_kwargs["pms"] == "AppFolio"

    def test_run_phase1_applies_suppression(self, base_config, mock_http, mock_args, skip_health_check, mock_file_handler):
        """Should apply HubSpot suppression to Supabase companies."""
        orchestrator = LeadOrchestrator(base_config)

        supabase_companies = [
            {"company_name": "Company A", "domain": "companya.com"},
            {"company_name": "Company B", "domain": "companyb.com"},
            {"company_name": "Company C", "domain": "companyc.com"},
        ]

        # HubSpot filters out 1 company
        filtered_companies = supabase_companies[:2]

        with patch.object(orchestrator.supabase, "find_existing_companies", return_value=supabase_companies), \
             patch.object(orchestrator.hubspot, "filter_companies", return_value=filtered_companies), \
             patch.object(orchestrator, "_enrich_companies_resilient", return_value=[]), \
             patch.object(orchestrator, "_finalize_results", return_value={}), \
             patch.object(orchestrator, "_notify_owner_success"), \
             patch.object(orchestrator, "_generate_metrics_report"), \
             patch("lead_pipeline.Path.mkdir"), \
             patch("lead_pipeline.Path.write_text"), \
             patch("logging.FileHandler", return_value=mock_file_handler):

            result = orchestrator.run(mock_args)

            # Verify suppression was applied
            orchestrator.hubspot.filter_companies.assert_called()
            # Should be called at least once with Supabase companies
            first_call_arg = orchestrator.hubspot.filter_companies.call_args_list[0][0][0]
            assert len(first_call_arg) == 3  # Original Supabase companies

    def test_run_phase1_checkpoints_state(self, base_config, mock_http, mock_args, skip_health_check, mock_file_handler):
        """Should checkpoint state after Phase 1."""
        orchestrator = LeadOrchestrator(base_config)

        supabase_companies = [
            {"company_name": "Company A", "domain": "companya.com"},
        ]

        with patch.object(orchestrator.supabase, "find_existing_companies", return_value=supabase_companies), \
             patch.object(orchestrator.hubspot, "filter_companies", side_effect=lambda x: x), \
             patch.object(orchestrator, "_checkpoint_if_needed") as mock_checkpoint, \
             patch.object(orchestrator, "_enrich_companies_resilient", return_value=[]), \
             patch.object(orchestrator, "_finalize_results", return_value={}), \
             patch.object(orchestrator, "_notify_owner_success"), \
             patch.object(orchestrator, "_generate_metrics_report"), \
             patch("lead_pipeline.Path.mkdir"), \
             patch("lead_pipeline.Path.write_text"), \
             patch("logging.FileHandler", return_value=mock_file_handler):

            result = orchestrator.run(mock_args)

            # Should checkpoint after Supabase load
            checkpoint_calls = [call for call in mock_checkpoint.call_args_list
                              if "supabase_loaded" in str(call)]
            assert len(checkpoint_calls) > 0


@pytest.mark.integration
@pytest.mark.orchestrator
class TestOrchestratorRunPhase2:
    """Test Phase 2: Discovery rounds with max_rounds logic."""

    def test_run_phase2_single_discovery_round(self, base_config, mock_http, mock_args, skip_health_check, mock_file_handler):
        """Should execute single discovery round when buffer not filled."""
        orchestrator = LeadOrchestrator(base_config)

        # Supabase returns 2 companies, need 20 for buffer (5 * 4)
        supabase_companies = [
            {"company_name": "Company A", "domain": "companya.com"},
            {"company_name": "Company B", "domain": "companyb.com"},
        ]

        # Discovery returns enough to fill buffer
        discovered_companies = [
            {"company_name": f"Discovered {i}", "domain": f"discovered{i}.com"}
            for i in range(18)
        ]

        # Enrichment returns enough results to avoid top-up
        enriched_results = [
            {
                "company": {"company_name": f"Company {i}", "domain": f"company{i}.com"},
                "contacts": [{"full_name": f"Contact {i}"}]
            }
            for i in range(5)  # Target quantity
        ]

        with patch.object(orchestrator.supabase, "find_existing_companies", return_value=supabase_companies), \
             patch.object(orchestrator.hubspot, "filter_companies", side_effect=lambda x: x), \
             patch.object(orchestrator.discovery, "discover", return_value=discovered_companies), \
             patch.object(orchestrator, "_enrich_companies_resilient", return_value=enriched_results), \
             patch.object(orchestrator, "_finalize_results", return_value={}), \
             patch.object(orchestrator, "_notify_owner_success"), \
             patch.object(orchestrator, "_generate_metrics_report"), \
             patch("lead_pipeline.Path.mkdir"), \
             patch("lead_pipeline.Path.write_text"), \
             patch("logging.FileHandler", return_value=mock_file_handler), \
             patch("time.sleep"):  # Skip sleep delays

            result = orchestrator.run(mock_args)

            # Should call discovery exactly once (buffer filled in first round)
            assert orchestrator.discovery.discover.call_count == 1

            # Verify discovery was called with correct parameters
            call_kwargs = orchestrator.discovery.discover.call_args[1]
            assert call_kwargs["state"] == "CA"
            assert call_kwargs["pms"] == "AppFolio"
            assert call_kwargs["attempt"] == 1

    def test_run_phase2_multiple_discovery_rounds(self, base_config, mock_http, mock_args, skip_health_check, mock_file_handler):
        """Should execute multiple discovery rounds until buffer filled."""
        orchestrator = LeadOrchestrator(base_config)

        # Supabase returns empty
        supabase_companies = []

        # Discovery returns 5 companies per round (need 20 total)
        def discovery_side_effect(**kwargs):
            return [
                {"company_name": f"Discovered {i}", "domain": f"discovered{i}-{kwargs['attempt']}.com"}
                for i in range(5)
            ]

        # Enrichment returns enough results to avoid top-up
        enriched_results = [
            {
                "company": {"company_name": f"Company {i}", "domain": f"company{i}.com"},
                "contacts": [{"full_name": f"Contact {i}"}]
            }
            for i in range(5)
        ]

        with patch.object(orchestrator.supabase, "find_existing_companies", return_value=supabase_companies), \
             patch.object(orchestrator.hubspot, "filter_companies", side_effect=lambda x: x), \
             patch.object(orchestrator.discovery, "discover", side_effect=discovery_side_effect), \
             patch.object(orchestrator, "_enrich_companies_resilient", return_value=enriched_results), \
             patch.object(orchestrator, "_finalize_results", return_value={}), \
             patch.object(orchestrator, "_notify_owner_success"), \
             patch.object(orchestrator, "_generate_metrics_report"), \
             patch("lead_pipeline.Path.mkdir"), \
             patch("lead_pipeline.Path.write_text"), \
             patch("logging.FileHandler", return_value=mock_file_handler), \
             patch("time.sleep"):

            result = orchestrator.run(mock_args)

            # Should call discovery 3 times (max_rounds)
            assert orchestrator.discovery.discover.call_count == 3

    def test_run_phase2_respects_max_rounds(self, base_config, mock_http, mock_args, skip_health_check, mock_file_handler):
        """Should stop after max_rounds even if buffer not filled."""
        mock_args.max_rounds = 2
        orchestrator = LeadOrchestrator(base_config)

        # Supabase returns empty
        supabase_companies = []

        # Discovery returns only 1 company per round (not enough to fill buffer)
        # Use unique domains for each call to avoid deduplication
        call_count = [0]
        def discovery_side_effect(**kwargs):
            call_count[0] += 1
            return [{"company_name": f"Discovered {call_count[0]}", "domain": f"discovered{call_count[0]}.com"}]

        # Enrichment returns enough results to avoid top-up
        enriched_results = [
            {
                "company": {"company_name": f"Company {i}", "domain": f"company{i}.com"},
                "contacts": [{"full_name": f"Contact {i}"}]
            }
            for i in range(5)
        ]

        with patch.object(orchestrator.supabase, "find_existing_companies", return_value=supabase_companies), \
             patch.object(orchestrator.hubspot, "filter_companies", side_effect=lambda x: x), \
             patch.object(orchestrator.discovery, "discover", side_effect=discovery_side_effect), \
             patch.object(orchestrator, "_enrich_companies_resilient", return_value=enriched_results), \
             patch.object(orchestrator, "_finalize_results", return_value={}), \
             patch.object(orchestrator, "_notify_owner_success"), \
             patch.object(orchestrator, "_generate_metrics_report"), \
             patch("lead_pipeline.Path.mkdir"), \
             patch("lead_pipeline.Path.write_text"), \
             patch("logging.FileHandler", return_value=mock_file_handler), \
             patch("time.sleep"):

            result = orchestrator.run(mock_args)

            # Should stop after max_rounds (2)
            assert orchestrator.discovery.discover.call_count == 2

    def test_run_phase2_applies_suppression_to_discovered(self, base_config, mock_http, mock_args, skip_health_check, mock_file_handler):
        """Should apply suppression to each batch of discovered companies."""
        orchestrator = LeadOrchestrator(base_config)

        supabase_companies = []
        discovered_companies = [
            {"company_name": f"Discovered {i}", "domain": f"discovered{i}.com"}
            for i in range(20)
        ]

        # Track suppression calls
        suppression_call_count = [0]

        def track_suppression(companies):
            suppression_call_count[0] += 1
            return companies

        with patch.object(orchestrator.supabase, "find_existing_companies", return_value=supabase_companies), \
             patch.object(orchestrator.hubspot, "filter_companies", side_effect=track_suppression), \
             patch.object(orchestrator.discovery, "discover", return_value=discovered_companies), \
             patch.object(orchestrator, "_enrich_companies_resilient", return_value=[]), \
             patch.object(orchestrator, "_finalize_results", return_value={}), \
             patch.object(orchestrator, "_notify_owner_success"), \
             patch.object(orchestrator, "_generate_metrics_report"), \
             patch("lead_pipeline.Path.mkdir"), \
             patch("lead_pipeline.Path.write_text"), \
             patch("logging.FileHandler", return_value=mock_file_handler), \
             patch("time.sleep"):

            result = orchestrator.run(mock_args)

            # Should apply suppression at least twice: once for Supabase, once for discovery
            assert suppression_call_count[0] >= 2

    def test_run_phase2_includes_exclusion_list(self, base_config, mock_http, mock_args, skip_health_check, mock_file_handler):
        """Should pass exclusion list to discovery for suppression."""
        mock_args.exclude = ["excluded1.com", "excluded2.com"]
        orchestrator = LeadOrchestrator(base_config)

        supabase_companies = []
        discovered_companies = [{"company_name": "Discovered", "domain": "discovered.com"}]

        # Enrichment returns enough results to avoid top-up
        enriched_results = [
            {
                "company": {"company_name": f"Company {i}", "domain": f"company{i}.com"},
                "contacts": [{"full_name": f"Contact {i}"}]
            }
            for i in range(5)
        ]

        with patch.object(orchestrator.supabase, "find_existing_companies", return_value=supabase_companies), \
             patch.object(orchestrator.hubspot, "filter_companies", side_effect=lambda x: x), \
             patch.object(orchestrator.discovery, "discover", return_value=discovered_companies), \
             patch.object(orchestrator, "_enrich_companies_resilient", return_value=enriched_results), \
             patch.object(orchestrator, "_finalize_results", return_value={}), \
             patch.object(orchestrator, "_notify_owner_success"), \
             patch.object(orchestrator, "_generate_metrics_report"), \
             patch("lead_pipeline.Path.mkdir"), \
             patch("lead_pipeline.Path.write_text"), \
             patch("logging.FileHandler", return_value=mock_file_handler), \
             patch("time.sleep"):

            result = orchestrator.run(mock_args)

            # Verify exclusion list was passed to discovery (check first call)
            call_kwargs = orchestrator.discovery.discover.call_args_list[0][1]
            assert "excluded1.com" in call_kwargs["suppression_domains"]
            assert "excluded2.com" in call_kwargs["suppression_domains"]

    def test_run_phase2_uses_circuit_breaker_if_enabled(self, base_config, mock_http, mock_args, skip_health_check, mock_file_handler):
        """Should use circuit breaker for discovery when enabled."""
        base_config.circuit_breaker_enabled = True
        orchestrator = LeadOrchestrator(base_config)

        # Create mock circuit breaker
        mock_cb = MagicMock()
        mock_cb.call.return_value = []
        orchestrator.circuit_breakers["discovery"] = mock_cb

        supabase_companies = []

        with patch.object(orchestrator.supabase, "find_existing_companies", return_value=supabase_companies), \
             patch.object(orchestrator.hubspot, "filter_companies", side_effect=lambda x: x), \
             patch.object(orchestrator, "_enrich_companies_resilient", return_value=[]), \
             patch.object(orchestrator, "_finalize_results", return_value={}), \
             patch.object(orchestrator, "_notify_owner_success"), \
             patch.object(orchestrator, "_generate_metrics_report"), \
             patch("lead_pipeline.Path.mkdir"), \
             patch("lead_pipeline.Path.write_text"), \
             patch("logging.FileHandler", return_value=mock_file_handler), \
             patch("time.sleep"):

            result = orchestrator.run(mock_args)

            # Circuit breaker should have been called
            assert mock_cb.call.called


@pytest.mark.integration
@pytest.mark.orchestrator
class TestOrchestratorRunPhase3:
    """Test Phase 3: Company enrichment."""

    def test_run_phase3_calls_enrichment(self, base_config, mock_http, mock_args, skip_health_check, mock_file_handler):
        """Should call enrichment in Phase 3."""
        orchestrator = LeadOrchestrator(base_config)

        companies = [
            {"company_name": f"Company {i}", "domain": f"company{i}.com"}
            for i in range(10)
        ]

        enriched_results = [
            {
                "company": {"company_name": f"Company {i}", "domain": f"company{i}.com"},
                "contacts": [{"full_name": f"Contact {i}", "email": f"contact{i}@company{i}.com"}]
            }
            for i in range(5)
        ]

        with patch.object(orchestrator.supabase, "find_existing_companies", return_value=companies), \
             patch.object(orchestrator.hubspot, "filter_companies", side_effect=lambda x: x), \
             patch.object(orchestrator, "_enrich_companies_resilient", return_value=enriched_results) as mock_enrich, \
             patch.object(orchestrator, "_finalize_results", return_value={}), \
             patch.object(orchestrator, "_notify_owner_success"), \
             patch.object(orchestrator, "_generate_metrics_report"), \
             patch("lead_pipeline.Path.mkdir"), \
             patch("lead_pipeline.Path.write_text"), \
             patch("logging.FileHandler", return_value=mock_file_handler):

            result = orchestrator.run(mock_args)

            # Should call enrichment with companies
            mock_enrich.assert_called_once()
            enriched_companies = mock_enrich.call_args[0][0]
            assert len(enriched_companies) > 0

    def test_run_phase3_trims_to_buffer_quantity(self, base_config, mock_http, mock_args, skip_health_check, mock_file_handler):
        """Should only enrich up to buffer quantity."""
        orchestrator = LeadOrchestrator(base_config)

        # Provide more companies than buffer needs
        companies = [
            {"company_name": f"Company {i}", "domain": f"company{i}.com"}
            for i in range(50)
        ]

        # Return enough enriched results to avoid top-up
        enriched_results = [
            {
                "company": {"company_name": f"Company {i}", "domain": f"company{i}.com"},
                "contacts": [{"full_name": f"Contact {i}"}]
            }
            for i in range(5)
        ]

        with patch.object(orchestrator.supabase, "find_existing_companies", return_value=companies), \
             patch.object(orchestrator.hubspot, "filter_companies", side_effect=lambda x: x), \
             patch.object(orchestrator, "_enrich_companies_resilient", return_value=enriched_results) as mock_enrich, \
             patch.object(orchestrator, "_finalize_results", return_value={}), \
             patch.object(orchestrator, "_notify_owner_success"), \
             patch.object(orchestrator, "_generate_metrics_report"), \
             patch("lead_pipeline.Path.mkdir"), \
             patch("lead_pipeline.Path.write_text"), \
             patch("logging.FileHandler", return_value=mock_file_handler):

            result = orchestrator.run(mock_args)

            # Should only enrich buffer quantity, not all 50
            enriched_companies = mock_enrich.call_args[0][0]
            # Actual trimming behavior: enriches less than all 50 companies
            assert len(enriched_companies) < 50
            assert len(enriched_companies) >= 5  # At least the target quantity

    def test_run_phase3_trims_results_to_target(self, base_config, mock_http, mock_args, skip_health_check, mock_file_handler):
        """Should trim enriched results to target quantity."""
        orchestrator = LeadOrchestrator(base_config)

        companies = [{"company_name": f"Company {i}", "domain": f"company{i}.com"} for i in range(20)]

        # Enrichment returns more than target quantity
        enriched_results = [
            {
                "company": {"company_name": f"Company {i}", "domain": f"company{i}.com"},
                "contacts": [{"full_name": f"Contact {i}"}]
            }
            for i in range(10)  # More than target of 5
        ]

        finalize_called_with = []

        def capture_finalize(deliverable, target, buffer, run_dir, run_id):
            finalize_called_with.append(len(deliverable))
            return {}

        with patch.object(orchestrator.supabase, "find_existing_companies", return_value=companies), \
             patch.object(orchestrator.hubspot, "filter_companies", side_effect=lambda x: x), \
             patch.object(orchestrator, "_enrich_companies_resilient", return_value=enriched_results), \
             patch.object(orchestrator, "_finalize_results", side_effect=capture_finalize), \
             patch.object(orchestrator, "_notify_owner_success"), \
             patch.object(orchestrator, "_generate_metrics_report"), \
             patch("lead_pipeline.Path.mkdir"), \
             patch("lead_pipeline.Path.write_text"), \
             patch("logging.FileHandler", return_value=mock_file_handler):

            result = orchestrator.run(mock_args)

            # Should finalize with target quantity (5), not all enriched (10)
            assert finalize_called_with[0] == 5


@pytest.mark.integration
@pytest.mark.orchestrator
class TestOrchestratorRunPhase4:
    """Test Phase 4: Top-up logic when results < target."""

    def test_run_phase4_topup_when_insufficient_results(self, base_config, mock_http, mock_args, skip_health_check, mock_file_handler):
        """Should trigger top-up when enriched results < target."""
        orchestrator = LeadOrchestrator(base_config)

        companies = [{"company_name": f"Company {i}", "domain": f"company{i}.com"} for i in range(20)]

        # Enrichment only returns 2 results (need 5)
        enriched_results = [
            {
                "company": {"company_name": f"Company {i}", "domain": f"company{i}.com"},
                "contacts": [{"full_name": f"Contact {i}"}]
            }
            for i in range(2)
        ]

        topup_results = enriched_results + [
            {
                "company": {"company_name": "Topup Company", "domain": "topup.com"},
                "contacts": [{"full_name": "Topup Contact"}]
            }
        ]

        with patch.object(orchestrator.supabase, "find_existing_companies", return_value=companies), \
             patch.object(orchestrator.hubspot, "filter_companies", side_effect=lambda x: x), \
             patch.object(orchestrator, "_enrich_companies_resilient", return_value=enriched_results), \
             patch.object(orchestrator, "_topup_results", return_value=topup_results) as mock_topup, \
             patch.object(orchestrator, "_finalize_results", return_value={}), \
             patch.object(orchestrator, "_notify_owner_success"), \
             patch.object(orchestrator, "_generate_metrics_report"), \
             patch("lead_pipeline.Path.mkdir"), \
             patch("lead_pipeline.Path.write_text"), \
             patch("logging.FileHandler", return_value=mock_file_handler):

            result = orchestrator.run(mock_args)

            # Should call top-up with missing count (3)
            mock_topup.assert_called_once()
            missing = mock_topup.call_args[0][1]
            assert missing == 3  # Need 5, got 2, missing 3

    def test_run_phase4_no_topup_when_sufficient_results(self, base_config, mock_http, mock_args, skip_health_check, mock_file_handler):
        """Should skip top-up when enriched results >= target."""
        orchestrator = LeadOrchestrator(base_config)

        companies = [{"company_name": f"Company {i}", "domain": f"company{i}.com"} for i in range(20)]

        # Enrichment returns exactly target quantity
        enriched_results = [
            {
                "company": {"company_name": f"Company {i}", "domain": f"company{i}.com"},
                "contacts": [{"full_name": f"Contact {i}"}]
            }
            for i in range(5)
        ]

        with patch.object(orchestrator.supabase, "find_existing_companies", return_value=companies), \
             patch.object(orchestrator.hubspot, "filter_companies", side_effect=lambda x: x), \
             patch.object(orchestrator, "_enrich_companies_resilient", return_value=enriched_results), \
             patch.object(orchestrator, "_topup_results") as mock_topup, \
             patch.object(orchestrator, "_finalize_results", return_value={}), \
             patch.object(orchestrator, "_notify_owner_success"), \
             patch.object(orchestrator, "_generate_metrics_report"), \
             patch("lead_pipeline.Path.mkdir"), \
             patch("lead_pipeline.Path.write_text"), \
             patch("logging.FileHandler", return_value=mock_file_handler):

            result = orchestrator.run(mock_args)

            # Top-up should not be called
            mock_topup.assert_not_called()


@pytest.mark.integration
@pytest.mark.orchestrator
class TestOrchestratorRunErrorHandling:
    """Test error handling and recovery in run()."""

    def test_run_handles_discovery_failure(self, base_config, mock_http, mock_args, skip_health_check, mock_file_handler):
        """Should handle discovery failures gracefully."""
        orchestrator = LeadOrchestrator(base_config)

        supabase_companies = []

        # Discovery raises exception
        def discovery_error(**kwargs):
            raise Exception("Discovery service unavailable")

        with patch.object(orchestrator.supabase, "find_existing_companies", return_value=supabase_companies), \
             patch.object(orchestrator.hubspot, "filter_companies", side_effect=lambda x: x), \
             patch.object(orchestrator.discovery, "discover", side_effect=discovery_error), \
             patch.object(orchestrator, "_track_error") as mock_track_error, \
             patch.object(orchestrator, "_enrich_companies_resilient", return_value=[]), \
             patch.object(orchestrator, "_finalize_results", return_value={}), \
             patch.object(orchestrator, "_notify_owner_success"), \
             patch.object(orchestrator, "_generate_metrics_report"), \
             patch("lead_pipeline.Path.mkdir"), \
             patch("lead_pipeline.Path.write_text"), \
             patch("logging.FileHandler", return_value=mock_file_handler), \
             patch("time.sleep"):

            result = orchestrator.run(mock_args)

            # Should track the error
            assert mock_track_error.called
            error_calls = [call for call in mock_track_error.call_args_list
                          if "Discovery" in str(call) or "discovery" in str(call)]
            assert len(error_calls) > 0

    def test_run_handles_keyboard_interrupt(self, base_config, mock_http, mock_args, skip_health_check, mock_file_handler):
        """Should save partial results on keyboard interrupt."""
        orchestrator = LeadOrchestrator(base_config)

        companies = [{"company_name": "Company A", "domain": "companya.com"}]

        # Enrichment raises KeyboardInterrupt
        def enrichment_interrupt(*args):
            raise KeyboardInterrupt()

        with patch.object(orchestrator.supabase, "find_existing_companies", return_value=companies), \
             patch.object(orchestrator.hubspot, "filter_companies", side_effect=lambda x: x), \
             patch.object(orchestrator, "_enrich_companies_resilient", side_effect=enrichment_interrupt), \
             patch.object(orchestrator, "_save_partial_results") as mock_save_partial, \
             patch("lead_pipeline.Path.mkdir"), \
             patch("lead_pipeline.Path.write_text"), \
             patch("logging.FileHandler", return_value=mock_file_handler):

            with pytest.raises(KeyboardInterrupt):
                orchestrator.run(mock_args)

            # Should save partial results
            mock_save_partial.assert_called_once()

    def test_run_handles_general_exception(self, base_config, mock_http, mock_args, skip_health_check, mock_file_handler):
        """Should handle general exceptions and notify owner."""
        orchestrator = LeadOrchestrator(base_config)

        companies = [{"company_name": "Company A", "domain": "companya.com"}]

        # Enrichment raises general exception
        def enrichment_error(*args):
            raise RuntimeError("Unexpected error during enrichment")

        with patch.object(orchestrator.supabase, "find_existing_companies", return_value=companies), \
             patch.object(orchestrator.hubspot, "filter_companies", side_effect=lambda x: x), \
             patch.object(orchestrator, "_enrich_companies_resilient", side_effect=enrichment_error), \
             patch.object(orchestrator, "_save_partial_results"), \
             patch.object(orchestrator, "_notify_owner_failure") as mock_notify_failure, \
             patch.object(orchestrator, "_track_error"), \
             patch("lead_pipeline.Path.mkdir"), \
             patch("lead_pipeline.Path.write_text"), \
             patch("logging.FileHandler", return_value=mock_file_handler):

            with pytest.raises(RuntimeError):
                orchestrator.run(mock_args)

            # Should notify owner of failure
            mock_notify_failure.assert_called_once()

    def test_run_retries_discovery_with_backoff(self, base_config, mock_http, mock_args, skip_health_check, mock_file_handler):
        """Should retry discovery with progressive backoff on failure."""
        orchestrator = LeadOrchestrator(base_config)

        supabase_companies = []

        # Track sleep calls for backoff verification
        sleep_calls = []

        def track_sleep(seconds):
            sleep_calls.append(seconds)

        # Discovery fails 2 times then succeeds
        attempt_count = [0]

        def discovery_with_failures(**kwargs):
            attempt_count[0] += 1
            if attempt_count[0] <= 2:
                raise Exception("Discovery failed")
            # Return companies on 3rd attempt
            return [{"company_name": "Company", "domain": "company.com"}]

        # Return enough enriched results to avoid top-up
        enriched_results = [
            {
                "company": {"company_name": f"Company {i}", "domain": f"company{i}.com"},
                "contacts": [{"full_name": f"Contact {i}"}]
            }
            for i in range(5)
        ]

        with patch.object(orchestrator.supabase, "find_existing_companies", return_value=supabase_companies), \
             patch.object(orchestrator.hubspot, "filter_companies", side_effect=lambda x: x), \
             patch.object(orchestrator.discovery, "discover", side_effect=discovery_with_failures), \
             patch.object(orchestrator, "_enrich_companies_resilient", return_value=enriched_results), \
             patch.object(orchestrator, "_finalize_results", return_value={}), \
             patch.object(orchestrator, "_notify_owner_success"), \
             patch.object(orchestrator, "_generate_metrics_report"), \
             patch("lead_pipeline.Path.mkdir"), \
             patch("lead_pipeline.Path.write_text"), \
             patch("logging.FileHandler", return_value=mock_file_handler), \
             patch("time.sleep", side_effect=track_sleep):

            result = orchestrator.run(mock_args)

            # Should have called discovery 3 times (2 failures + 1 success)
            assert attempt_count[0] == 3

            # Should have backoff sleep calls (for the 2 failures)
            # Note: backoff sleeps are in addition to discovery round delays
            backoff_sleeps = [s for s in sleep_calls if s >= 5]  # Backoff is min(30, attempt * 5)
            assert len(backoff_sleeps) >= 2


@pytest.mark.integration
@pytest.mark.orchestrator
class TestOrchestratorRunStatePersistence:
    """Test state checkpointing and file persistence in run()."""

    def test_run_creates_run_directory(self, base_config, mock_http, mock_args, skip_health_check, mock_file_handler):
        """Should create run directory with timestamp and UUID."""
        orchestrator = LeadOrchestrator(base_config)

        mkdir_calls = []

        def track_mkdir(*args, **kwargs):
            # self is the Path object, so we need to capture it
            pass

        with patch.object(orchestrator.supabase, "find_existing_companies", return_value=[]), \
             patch.object(orchestrator.hubspot, "filter_companies", side_effect=lambda x: x), \
             patch.object(orchestrator, "_enrich_companies_resilient", return_value=[]), \
             patch.object(orchestrator, "_finalize_results", return_value={}), \
             patch.object(orchestrator, "_notify_owner_success"), \
             patch.object(orchestrator, "_generate_metrics_report"), \
             patch("lead_pipeline.Path.mkdir") as mock_mkdir, \
             patch("lead_pipeline.Path.write_text"), \
             patch("logging.FileHandler", return_value=mock_file_handler):

            result = orchestrator.run(mock_args)

            # Should create directory
            assert mock_mkdir.called

    def test_run_saves_input_json(self, base_config, mock_http, mock_args, skip_health_check, mock_file_handler):
        """Should save input.json with run parameters."""
        orchestrator = LeadOrchestrator(base_config)

        write_calls = []

        def track_write(content):
            write_calls.append(content)

        with patch.object(orchestrator.supabase, "find_existing_companies", return_value=[]), \
             patch.object(orchestrator.hubspot, "filter_companies", side_effect=lambda x: x), \
             patch.object(orchestrator, "_enrich_companies_resilient", return_value=[]), \
             patch.object(orchestrator, "_finalize_results", return_value={}), \
             patch.object(orchestrator, "_notify_owner_success"), \
             patch.object(orchestrator, "_generate_metrics_report"), \
             patch("lead_pipeline.Path.mkdir"), \
             patch("lead_pipeline.Path.write_text", side_effect=track_write), \
             patch("logging.FileHandler", return_value=mock_file_handler):

            result = orchestrator.run(mock_args)

            # Should write input.json
            input_writes = [w for w in write_calls if "run_id" in w and "args" in w]
            assert len(input_writes) > 0

    def test_run_saves_metrics_on_completion(self, base_config, mock_http, mock_args, skip_health_check, mock_file_handler):
        """Should save metrics.json on completion."""
        orchestrator = LeadOrchestrator(base_config)

        write_calls = []

        def track_write(content):
            write_calls.append(content)

        with patch.object(orchestrator.supabase, "find_existing_companies", return_value=[]), \
             patch.object(orchestrator.hubspot, "filter_companies", side_effect=lambda x: x), \
             patch.object(orchestrator, "_enrich_companies_resilient", return_value=[]), \
             patch.object(orchestrator, "_finalize_results", return_value={}), \
             patch.object(orchestrator, "_notify_owner_success"), \
             patch.object(orchestrator, "_generate_metrics_report"), \
             patch("lead_pipeline.Path.mkdir"), \
             patch("lead_pipeline.Path.write_text", side_effect=track_write), \
             patch("logging.FileHandler", return_value=mock_file_handler):

            result = orchestrator.run(mock_args)

            # Should write metrics
            metrics_writes = [w for w in write_calls if "start_time" in w or "api_calls" in w]
            assert len(metrics_writes) > 0

    def test_run_calls_finalize_results(self, base_config, mock_http, mock_args, skip_health_check, mock_file_handler):
        """Should call _finalize_results to save output files."""
        orchestrator = LeadOrchestrator(base_config)

        companies = [{"company_name": "Company A", "domain": "companya.com"}]
        enriched_results = [
            {
                "company": {"company_name": "Company A", "domain": "companya.com"},
                "contacts": [{"full_name": "Contact A"}]
            }
        ]

        with patch.object(orchestrator.supabase, "find_existing_companies", return_value=companies), \
             patch.object(orchestrator.hubspot, "filter_companies", side_effect=lambda x: x), \
             patch.object(orchestrator, "_enrich_companies_resilient", return_value=enriched_results), \
             patch.object(orchestrator, "_finalize_results", return_value={}) as mock_finalize, \
             patch.object(orchestrator, "_notify_owner_success"), \
             patch.object(orchestrator, "_generate_metrics_report"), \
             patch("lead_pipeline.Path.mkdir"), \
             patch("lead_pipeline.Path.write_text"), \
             patch("logging.FileHandler", return_value=mock_file_handler):

            result = orchestrator.run(mock_args)

            # Should call finalize
            mock_finalize.assert_called_once()

    def test_run_calls_notify_owner_success(self, base_config, mock_http, mock_args, skip_health_check, mock_file_handler):
        """Should notify owner on successful completion."""
        orchestrator = LeadOrchestrator(base_config)

        with patch.object(orchestrator.supabase, "find_existing_companies", return_value=[]), \
             patch.object(orchestrator.hubspot, "filter_companies", side_effect=lambda x: x), \
             patch.object(orchestrator, "_enrich_companies_resilient", return_value=[]), \
             patch.object(orchestrator, "_finalize_results", return_value={}), \
             patch.object(orchestrator, "_notify_owner_success") as mock_notify, \
             patch.object(orchestrator, "_generate_metrics_report"), \
             patch("lead_pipeline.Path.mkdir"), \
             patch("lead_pipeline.Path.write_text"), \
             patch("logging.FileHandler", return_value=mock_file_handler):

            result = orchestrator.run(mock_args)

            # Should notify owner
            mock_notify.assert_called_once()

    def test_run_generates_metrics_report(self, base_config, mock_http, mock_args, skip_health_check, mock_file_handler):
        """Should generate metrics report on completion."""
        orchestrator = LeadOrchestrator(base_config)

        with patch.object(orchestrator.supabase, "find_existing_companies", return_value=[]), \
             patch.object(orchestrator.hubspot, "filter_companies", side_effect=lambda x: x), \
             patch.object(orchestrator, "_enrich_companies_resilient", return_value=[]), \
             patch.object(orchestrator, "_finalize_results", return_value={}), \
             patch.object(orchestrator, "_notify_owner_success"), \
             patch.object(orchestrator, "_generate_metrics_report") as mock_report, \
             patch("lead_pipeline.Path.mkdir"), \
             patch("lead_pipeline.Path.write_text"), \
             patch("logging.FileHandler", return_value=mock_file_handler):

            result = orchestrator.run(mock_args)

            # Should generate report
            mock_report.assert_called_once()


@pytest.mark.integration
@pytest.mark.orchestrator
class TestOrchestratorRunHealthChecks:
    """Test health check integration in run()."""

    def test_run_performs_health_check(self, base_config, mock_http, mock_args, skip_health_check, mock_file_handler):
        """Should perform health check before starting pipeline."""
        orchestrator = LeadOrchestrator(base_config)

        with patch("lead_pipeline.HealthCheck") as MockHealthCheck, \
             patch.object(orchestrator.supabase, "find_existing_companies", return_value=[]), \
             patch.object(orchestrator.hubspot, "filter_companies", side_effect=lambda x: x), \
             patch.object(orchestrator, "_enrich_companies_resilient", return_value=[]), \
             patch.object(orchestrator, "_finalize_results", return_value={}), \
             patch.object(orchestrator, "_notify_owner_success"), \
             patch.object(orchestrator, "_generate_metrics_report"), \
             patch("lead_pipeline.Path.mkdir"), \
             patch("lead_pipeline.Path.write_text"), \
             patch("logging.FileHandler", return_value=mock_file_handler):

            mock_health = MagicMock()
            mock_health.check_all.return_value = (True, [])
            MockHealthCheck.return_value = mock_health

            result = orchestrator.run(mock_args)

            # Should perform health check
            MockHealthCheck.assert_called_once()
            mock_health.check_all.assert_called_once()

    def test_run_fails_on_unhealthy_services(self, base_config, mock_http, mock_args, mock_file_handler, monkeypatch):
        """Should return error when health check fails."""
        # Don't skip health check for this test
        monkeypatch.delenv("SKIP_HEALTH_CHECK", raising=False)

        orchestrator = LeadOrchestrator(base_config)

        with patch("lead_pipeline.HealthCheck") as MockHealthCheck, \
             patch("lead_pipeline.Path.mkdir"), \
             patch("lead_pipeline.Path.write_text"), \
             patch("logging.FileHandler", return_value=mock_file_handler):

            mock_health = MagicMock()
            mock_health.check_all.return_value = (False, ["Supabase unreachable", "HubSpot API error"])
            MockHealthCheck.return_value = mock_health

            result = orchestrator.run(mock_args)

            # Should return error dict
            assert "error" in result
            assert result["error"] == "Health check failed"
            assert "Supabase unreachable" in result["details"]
            assert "HubSpot API error" in result["details"]


@pytest.mark.integration
@pytest.mark.orchestrator
class TestOrchestratorRunConfigValidation:
    """Test configuration validation in run()."""

    def test_run_validates_quantity(self, base_config, mock_http, mock_args, skip_health_check, mock_file_handler):
        """Should validate and cap quantity to max_companies_per_run."""
        base_config.max_companies_per_run = 3
        mock_args.quantity = 100  # Request more than max

        orchestrator = LeadOrchestrator(base_config)

        finalize_target = []

        def capture_target(deliverable, target, buffer, run_dir, run_id):
            finalize_target.append(target)
            return {}

        with patch.object(orchestrator.supabase, "find_existing_companies", return_value=[]), \
             patch.object(orchestrator.hubspot, "filter_companies", side_effect=lambda x: x), \
             patch.object(orchestrator, "_enrich_companies_resilient", return_value=[]), \
             patch.object(orchestrator, "_finalize_results", side_effect=capture_target), \
             patch.object(orchestrator, "_notify_owner_success"), \
             patch.object(orchestrator, "_generate_metrics_report"), \
             patch("lead_pipeline.Path.mkdir"), \
             patch("lead_pipeline.Path.write_text"), \
             patch("logging.FileHandler", return_value=mock_file_handler):

            result = orchestrator.run(mock_args)

            # Should cap at max (3), not use requested (100)
            assert finalize_target[0] == 3

    def test_run_sets_minimum_quantity_to_one(self, base_config, mock_http, mock_args, skip_health_check, mock_file_handler):
        """Should enforce minimum quantity of 1."""
        mock_args.quantity = -5  # Invalid negative quantity

        orchestrator = LeadOrchestrator(base_config)

        finalize_target = []

        def capture_target(deliverable, target, buffer, run_dir, run_id):
            finalize_target.append(target)
            return {}

        with patch.object(orchestrator.supabase, "find_existing_companies", return_value=[]), \
             patch.object(orchestrator.hubspot, "filter_companies", side_effect=lambda x: x), \
             patch.object(orchestrator, "_enrich_companies_resilient", return_value=[]), \
             patch.object(orchestrator, "_finalize_results", side_effect=capture_target), \
             patch.object(orchestrator, "_notify_owner_success"), \
             patch.object(orchestrator, "_generate_metrics_report"), \
             patch("lead_pipeline.Path.mkdir"), \
             patch("lead_pipeline.Path.write_text"), \
             patch("logging.FileHandler", return_value=mock_file_handler):

            result = orchestrator.run(mock_args)

            # Should set minimum to 1
            assert finalize_target[0] >= 1


# ==============================================================================
# Enrichment Flow Tests
# ==============================================================================


@pytest.mark.integration
@pytest.mark.orchestrator
class TestOrchestratorEnrichmentResilient:
    """Test _enrich_companies_resilient() method with error handling and retries."""

    def test_enrich_resilient_basic_flow(self, base_config, skip_health_check):
        """Should enrich companies concurrently and track metrics."""
        orchestrator = LeadOrchestrator(base_config)
        orchestrator.current_target_quantity = 5

        companies = [
            {"company_name": f"Company {i}", "domain": f"company{i}.com"}
            for i in range(3)
        ]

        enriched_result = {
            "company": {"company_name": "Enriched Co", "domain": "enriched.com"},
            "contacts": [
                {"full_name": "John Doe", "email": "john@enriched.com"},
                {"full_name": "Jane Smith", "email": "jane@enriched.com"},
            ]
        }

        with patch.object(orchestrator, "_process_single_company", return_value=enriched_result), \
             patch.object(orchestrator, "_save_incremental_results"), \
             patch("lead_pipeline.Path"):

            results = orchestrator._enrich_companies_resilient(companies, Path("/tmp/test"))

            # Should enrich all 3 companies
            assert len(results) == 3
            # Should track metrics
            assert orchestrator.metrics["companies_enriched"] == 3
            assert orchestrator.metrics["contacts_enriched"] == 6  # 2 contacts × 3 companies

    def test_enrich_resilient_handles_errors(self, base_config, skip_health_check):
        """Should handle individual company failures gracefully."""
        orchestrator = LeadOrchestrator(base_config)
        orchestrator.current_target_quantity = 10

        companies = [
            {"company_name": f"Company {i}", "domain": f"company{i}.com"}
            for i in range(5)
        ]

        call_count = [0]

        def process_with_errors(company):
            call_count[0] += 1
            if call_count[0] % 2 == 0:  # Fail every other company
                raise Exception("Processing failed")
            return {
                "company": company,
                "contacts": [{"full_name": "Test", "email": "test@example.com"}]
            }

        with patch.object(orchestrator, "_process_single_company", side_effect=process_with_errors), \
             patch.object(orchestrator, "_save_incremental_results"), \
             patch.object(orchestrator, "_track_error"), \
             patch("lead_pipeline.Path"):

            results = orchestrator._enrich_companies_resilient(companies, Path("/tmp/test"))

            # Should have some successes despite failures
            assert len(results) >= 2
            # Should track errors
            assert orchestrator._track_error.call_count >= 2

    def test_enrich_resilient_retries_failures(self, base_config, skip_health_check):
        """Should retry failed companies once."""
        orchestrator = LeadOrchestrator(base_config)
        orchestrator.current_target_quantity = 10

        companies = [{"company_name": "FailCo", "domain": "fail.com"}]

        attempt_count = [0]

        def process_with_retry(company):
            attempt_count[0] += 1
            if attempt_count[0] == 1:
                raise Exception("First attempt failed")
            # Success on retry
            return {
                "company": company,
                "contacts": [{"full_name": "Test", "email": "test@example.com"}]
            }

        with patch.object(orchestrator, "_process_single_company", side_effect=process_with_retry), \
             patch.object(orchestrator, "_save_incremental_results"), \
             patch.object(orchestrator, "_track_error"), \
             patch("lead_pipeline.Path"), \
             patch("time.sleep"):

            results = orchestrator._enrich_companies_resilient(companies, Path("/tmp/test"))

            # Should succeed on retry
            assert len(results) == 1
            # Should have tried twice
            assert attempt_count[0] == 2

    def test_enrich_resilient_saves_incremental(self, base_config, skip_health_check):
        """Should save incremental results every 5 companies."""
        orchestrator = LeadOrchestrator(base_config)
        orchestrator.current_target_quantity = 20

        companies = [
            {"company_name": f"Company {i}", "domain": f"company{i}.com"}
            for i in range(12)
        ]

        enriched_result = {
            "company": {"company_name": "Test", "domain": "test.com"},
            "contacts": [{"full_name": "Test", "email": "test@example.com"}]
        }

        with patch.object(orchestrator, "_process_single_company", return_value=enriched_result), \
             patch.object(orchestrator, "_save_incremental_results") as mock_save, \
             patch("lead_pipeline.Path"):

            results = orchestrator._enrich_companies_resilient(companies, Path("/tmp/test"))

            # Should save at 5, 10 (every 5th result)
            assert mock_save.call_count >= 2

    def test_enrich_resilient_respects_target_quantity(self, base_config, skip_health_check):
        """Should stop enriching once target quantity is reached."""
        orchestrator = LeadOrchestrator(base_config)
        orchestrator.current_target_quantity = 3

        companies = [
            {"company_name": f"Company {i}", "domain": f"company{i}.com"}
            for i in range(10)  # More than target
        ]

        enriched_result = {
            "company": {"company_name": "Test", "domain": "test.com"},
            "contacts": [{"full_name": "Test", "email": "test@example.com"}]
        }

        with patch.object(orchestrator, "_process_single_company", return_value=enriched_result), \
             patch.object(orchestrator, "_save_incremental_results"), \
             patch("lead_pipeline.Path"):

            results = orchestrator._enrich_companies_resilient(companies, Path("/tmp/test"))

            # Should stop at target quantity
            assert len(results) == 3

    def test_enrich_resilient_tracks_rejections(self, base_config, skip_health_check):
        """Should track rejected companies in metrics."""
        orchestrator = LeadOrchestrator(base_config)
        orchestrator.current_target_quantity = 10

        companies = [
            {"company_name": f"Company {i}", "domain": f"company{i}.com"}
            for i in range(5)
        ]

        # Return None for some companies (rejected)
        call_count = [0]

        def process_with_rejections(company):
            call_count[0] += 1
            if call_count[0] % 2 == 0:
                return None  # Rejected
            return {
                "company": company,
                "contacts": [{"full_name": "Test", "email": "test@example.com"}]
            }

        with patch.object(orchestrator, "_process_single_company", side_effect=process_with_rejections), \
             patch.object(orchestrator, "_save_incremental_results"), \
             patch("lead_pipeline.Path"):

            results = orchestrator._enrich_companies_resilient(companies, Path("/tmp/test"))

            # Should have rejections tracked
            assert orchestrator.metrics["companies_rejected"] >= 2


@pytest.mark.integration
@pytest.mark.orchestrator
class TestContactQualityValidation:
    """Test contact quality validation and evaluation."""

    def test_quality_meets_all_thresholds(self, base_config):
        """Should pass when all thresholds are met."""
        from lead_pipeline import evaluate_contact_quality

        config = base_config
        config.contact_min_personal_anecdotes = 2
        config.contact_min_professional_anecdotes = 2
        config.contact_min_total_anecdotes = 4

        contact = {
            "personal_anecdotes": ["Anecdote 1", "Anecdote 2", "Anecdote 3"],
            "professional_anecdotes": ["Pro 1", "Pro 2"],
            "seed_urls": []
        }

        passed, stats = evaluate_contact_quality(contact, config)

        assert passed is True
        assert stats["reason"] == "thresholds_met"
        assert stats["personal"] == 3
        assert stats["professional"] == 2
        assert stats["total"] == 5

    def test_quality_fails_insufficient(self, base_config):
        """Should fail when thresholds not met."""
        from lead_pipeline import evaluate_contact_quality

        config = base_config
        config.contact_min_personal_anecdotes = 2
        config.contact_min_professional_anecdotes = 2
        config.contact_min_total_anecdotes = 4
        config.contact_allow_personalization_fallback = False
        config.contact_allow_seed_url_fallback = False

        contact = {
            "personal_anecdotes": ["Anecdote 1"],  # Only 1, need 2
            "professional_anecdotes": ["Pro 1"],   # Only 1, need 2
            "seed_urls": []
        }

        passed, stats = evaluate_contact_quality(contact, config)

        assert passed is False
        assert stats["reason"] == "insufficient"
        assert stats["total"] == 2  # < 4 required

    def test_quality_personalization_fallback(self, base_config):
        """Should pass with personalization fallback."""
        from lead_pipeline import evaluate_contact_quality

        config = base_config
        config.contact_min_personal_anecdotes = 2
        config.contact_min_professional_anecdotes = 2
        config.contact_min_total_anecdotes = 4
        config.contact_allow_personalization_fallback = True

        contact = {
            "personal_anecdotes": [],
            "professional_anecdotes": [],
            "seed_urls": [],
            "personalization": "Custom personalization text here"
        }

        passed, stats = evaluate_contact_quality(contact, config)

        assert passed is True
        assert stats["reason"] == "personalization_fallback"
        assert stats["has_personalization"] is True

    def test_quality_seed_url_fallback(self, base_config):
        """Should pass with seed URL fallback."""
        from lead_pipeline import evaluate_contact_quality

        config = base_config
        config.contact_min_personal_anecdotes = 2
        config.contact_min_professional_anecdotes = 2
        config.contact_min_total_anecdotes = 4
        config.contact_allow_personalization_fallback = False
        config.contact_allow_seed_url_fallback = True

        contact = {
            "personal_anecdotes": [],
            "professional_anecdotes": [],
            "seed_urls": ["https://example.com/article"]
        }

        passed, stats = evaluate_contact_quality(contact, config)

        assert passed is True
        assert stats["reason"] == "seed_url_fallback"
        assert stats["seed_urls"] == 1

    def test_quality_handles_none_anecdotes(self, base_config):
        """Should handle None anecdotes gracefully."""
        from lead_pipeline import evaluate_contact_quality

        config = base_config
        config.contact_min_personal_anecdotes = 0
        config.contact_min_professional_anecdotes = 0
        config.contact_min_total_anecdotes = 0

        contact = {
            "personal_anecdotes": None,
            "professional_anecdotes": None,
            "seed_urls": None
        }

        passed, stats = evaluate_contact_quality(contact, config)

        # Should pass with zero requirements
        assert passed is True
        assert stats["personal"] == 0
        assert stats["professional"] == 0


@pytest.mark.integration
@pytest.mark.orchestrator
class TestContactSalvage:
    """Test contact anecdote salvage logic."""

    def test_salvage_extracts_from_raw(self, base_config, skip_health_check):
        """Should extract anecdotes from raw enrichment data."""
        orchestrator = LeadOrchestrator(base_config)

        contact = {
            "personal_anecdotes": [],
            "professional_anecdotes": [],
            "seed_urls": [],
            "raw": {
                "personal": ["Personal fact 1", "Personal fact 2"],
                "professional": ["Work fact 1", "Work fact 2"],
                "seed_urls": ["https://source1.com"]
            }
        }

        with patch.object(orchestrator.enrichment, "_extract_enrichment_list") as mock_extract:
            mock_extract.side_effect = [
                ["Personal fact 1", "Personal fact 2"],  # personal
                ["Work fact 1", "Work fact 2"],          # professional
                ["https://source1.com"],                  # seed_urls
            ]

            updated = orchestrator._salvage_contact_anecdotes(contact)

            assert updated is True
            assert len(contact["personal_anecdotes"]) == 2
            assert len(contact["professional_anecdotes"]) == 2
            assert len(contact["seed_urls"]) == 1

    def test_salvage_parses_summary_bullets(self, base_config, skip_health_check):
        """Should parse summary text for anecdote bullets."""
        orchestrator = LeadOrchestrator(base_config)

        contact = {
            "personal_anecdotes": [],
            "professional_anecdotes": [],
            "seed_urls": [],
            "raw": {
                "summary": """
                - Personal: Enjoys hiking and outdoor activities
                - Role: Property Manager at XYZ Corp
                - Business: Uses AppFolio for management
                """
            }
        }

        with patch.object(orchestrator.enrichment, "_extract_enrichment_list", return_value=[]), \
             patch.object(orchestrator.enrichment, "_extract_enrichment_value") as mock_extract_val, \
             patch.object(orchestrator, "_dedupe_strings", side_effect=lambda x: list(set(x))):

            mock_extract_val.return_value = contact["raw"]["summary"]

            updated = orchestrator._salvage_contact_anecdotes(contact)

            assert updated is True
            # Should have extracted from summary
            assert len(contact["personal_anecdotes"]) >= 1
            assert len(contact["professional_anecdotes"]) >= 1

    def test_salvage_deduplicates_anecdotes(self, base_config, skip_health_check):
        """Should deduplicate extracted anecdotes."""
        orchestrator = LeadOrchestrator(base_config)

        contact = {
            "personal_anecdotes": ["Anecdote 1"],
            "professional_anecdotes": ["Work 1"],
            "seed_urls": [],
            "raw": {
                "personal": ["Anecdote 1", "Anecdote 2"],  # Duplicate + new
                "professional": ["Work 1", "Work 2"],       # Duplicate + new
            }
        }

        with patch.object(orchestrator.enrichment, "_extract_enrichment_list") as mock_extract, \
             patch.object(orchestrator.enrichment, "_extract_enrichment_value", return_value=None), \
             patch.object(orchestrator, "_dedupe_strings") as mock_dedupe:

            # 4 calls: personal, professional, seed_urls, sources (fallback)
            mock_extract.side_effect = [
                ["Anecdote 1", "Anecdote 2"],
                ["Work 1", "Work 2"],
                [],  # seed_urls empty
                [],  # sources fallback
            ]

            # Mock dedupe to actually deduplicate
            mock_dedupe.side_effect = lambda x: list(set(x))

            updated = orchestrator._salvage_contact_anecdotes(contact)

            # Should call dedupe for all lists
            assert mock_dedupe.call_count >= 3

    def test_salvage_returns_false_no_updates(self, base_config, skip_health_check):
        """Should return False when no updates made."""
        orchestrator = LeadOrchestrator(base_config)

        contact = {
            "personal_anecdotes": ["Existing 1", "Existing 2"],
            "professional_anecdotes": ["Work 1", "Work 2"],
            "seed_urls": ["https://source.com"],
            "raw": {
                "personal": ["Existing 1", "Existing 2"],  # Same as current
                "professional": ["Work 1", "Work 2"],       # Same as current
                "seed_urls": ["https://source.com"]        # Same as current
            }
        }

        with patch.object(orchestrator.enrichment, "_extract_enrichment_list") as mock_extract, \
             patch.object(orchestrator.enrichment, "_extract_enrichment_value", return_value=None), \
             patch.object(orchestrator, "_dedupe_strings") as mock_dedupe:

            # Return same values from extraction
            mock_extract.side_effect = [
                ["Existing 1", "Existing 2"],
                ["Work 1", "Work 2"],
                ["https://source.com"],
                [],  # sources fallback
            ]

            # Dedupe returns lists with same length as originals (simulates no new items)
            def dedupe_keeping_original_len(x):
                # After extend, lists are doubled, dedupe brings them back to original length
                return list(set(x))

            mock_dedupe.side_effect = dedupe_keeping_original_len

            updated = orchestrator._salvage_contact_anecdotes(contact)

            # No new anecdotes, should return False
            assert updated is False

    def test_salvage_handles_missing_raw(self, base_config, skip_health_check):
        """Should handle missing raw data gracefully."""
        orchestrator = LeadOrchestrator(base_config)

        contact = {
            "personal_anecdotes": [],
            "professional_anecdotes": [],
            "seed_urls": [],
            "raw": None  # Missing raw data
        }

        updated = orchestrator._salvage_contact_anecdotes(contact)

        assert updated is False


# ==============================================================================
# Contact Processing Flow Tests
# ==============================================================================


@pytest.mark.integration
@pytest.mark.orchestrator
class TestProcessSingleCompany:
    """Test _process_single_company() and contact discovery/verification flow."""

    def test_process_company_basic_flow(self, base_config, skip_health_check):
        """Should enrich company and discover/verify contacts."""
        orchestrator = LeadOrchestrator(base_config)

        company = {"company_name": "Test Co", "domain": "test.com", "hq_state": "CA"}

        enriched_company = {
            "company_name": "Test Co",
            "domain": "test.com",
            "hq_city": "San Francisco",
            "decision_makers": [
                {"full_name": "John Doe", "title": "CEO"},
                {"full_name": "Jane Smith", "title": "CFO"},
            ]
        }

        verified_contacts = [
            {"full_name": "John Doe", "email": "john@test.com"},
            {"full_name": "Jane Smith", "email": "jane@test.com"},
        ]

        with patch.object(orchestrator.enrichment, "enrich_company", return_value=enriched_company), \
             patch.object(orchestrator, "_discover_and_verify_contacts", return_value=verified_contacts), \
             patch.object(orchestrator.supabase, "persist_company", return_value={"id": 123}), \
             patch.object(orchestrator.supabase, "persist_contact"), \
             patch.object(orchestrator, "_track_api_call"):

            result = orchestrator._process_single_company(company)

            assert result is not None
            assert result["company"] == enriched_company
            assert result["contacts"] == verified_contacts
            orchestrator.enrichment.enrich_company.assert_called_once_with(company)
            orchestrator._discover_and_verify_contacts.assert_called_once()

    def test_process_company_enrichment_fails(self, base_config, skip_health_check):
        """Should return None when company enrichment fails."""
        orchestrator = LeadOrchestrator(base_config)

        company = {"company_name": "Test Co", "domain": "test.com"}

        with patch.object(orchestrator.enrichment, "enrich_company", return_value=None):
            result = orchestrator._process_single_company(company)

            assert result is None

    def test_process_company_no_contacts(self, base_config, skip_health_check):
        """Should return None when no verified contacts found."""
        orchestrator = LeadOrchestrator(base_config)

        company = {"company_name": "Test Co", "domain": "test.com"}
        enriched_company = {"company_name": "Test Co", "domain": "test.com"}

        with patch.object(orchestrator.enrichment, "enrich_company", return_value=enriched_company), \
             patch.object(orchestrator, "_discover_and_verify_contacts", return_value=[]), \
             patch.object(orchestrator, "_track_api_call"):

            result = orchestrator._process_single_company(company)

            assert result is None

    def test_process_company_location_fallback(self, base_config, skip_health_check):
        """Should fill missing location fields from original company data."""
        orchestrator = LeadOrchestrator(base_config)

        company = {"company_name": "Test Co", "hq_city": "Austin", "hq_state": "TX"}
        enriched_company = {"company_name": "Test Co", "domain": "test.com"}  # Missing location

        verified_contacts = [{"full_name": "John Doe", "email": "john@test.com"}]

        with patch.object(orchestrator.enrichment, "enrich_company", return_value=enriched_company), \
             patch.object(orchestrator, "_discover_and_verify_contacts", return_value=verified_contacts), \
             patch.object(orchestrator.supabase, "persist_company", return_value={"id": 123}), \
             patch.object(orchestrator.supabase, "persist_contact"), \
             patch.object(orchestrator, "_track_api_call"):

            result = orchestrator._process_single_company(company)

            # Location should be filled from original company
            assert result["company"]["hq_city"] == "Austin"
            assert result["company"]["hq_state"] == "TX"

    def test_process_company_persists_to_supabase(self, base_config, skip_health_check):
        """Should persist company and contacts to Supabase."""
        orchestrator = LeadOrchestrator(base_config)

        company = {"company_name": "Test Co", "domain": "test.com"}
        enriched_company = {"company_name": "Test Co", "domain": "test.com"}
        verified_contacts = [
            {"full_name": "John Doe", "email": "john@test.com"},
            {"full_name": "Jane Smith", "email": "jane@test.com"},
        ]

        company_record = {"id": 123}

        with patch.object(orchestrator.enrichment, "enrich_company", return_value=enriched_company), \
             patch.object(orchestrator, "_discover_and_verify_contacts", return_value=verified_contacts), \
             patch.object(orchestrator.supabase, "persist_company", return_value=company_record) as mock_persist_company, \
             patch.object(orchestrator.supabase, "persist_contact") as mock_persist_contact, \
             patch.object(orchestrator, "_track_api_call"):

            result = orchestrator._process_single_company(company)

            # Should persist company with contact count
            mock_persist_company.assert_called_once_with(company, enriched_company, 2)
            # Should persist both contacts
            assert mock_persist_contact.call_count == 2

    def test_process_company_handles_persistence_errors(self, base_config, skip_health_check):
        """Should handle Supabase persistence errors gracefully."""
        orchestrator = LeadOrchestrator(base_config)

        company = {"company_name": "Test Co", "domain": "test.com"}
        enriched_company = {"company_name": "Test Co", "domain": "test.com"}
        verified_contacts = [{"full_name": "John Doe", "email": "john@test.com"}]

        with patch.object(orchestrator.enrichment, "enrich_company", return_value=enriched_company), \
             patch.object(orchestrator, "_discover_and_verify_contacts", return_value=verified_contacts), \
             patch.object(orchestrator.supabase, "persist_company", side_effect=Exception("DB error")), \
             patch.object(orchestrator.supabase, "persist_contact"), \
             patch.object(orchestrator, "_track_api_call"):

            result = orchestrator._process_single_company(company)

            # Should still return result despite persistence error
            assert result is not None
            assert result["company"] == enriched_company


@pytest.mark.integration
@pytest.mark.orchestrator
class TestDiscoverAndVerifyContacts:
    """Test _discover_and_verify_contacts() flow."""

    def test_discover_from_decision_makers(self, base_config, skip_health_check):
        """Should use decision makers from company enrichment."""
        orchestrator = LeadOrchestrator(base_config)

        enriched_company = {
            "company_name": "Test Co",
            "domain": "test.com",
            "decision_makers": [
                {"full_name": "John Doe", "title": "CEO"},
                {"full_name": "Jane Smith", "title": "CFO"},
            ]
        }
        original_company = {"company_name": "Test Co", "domain": "test.com"}

        verification = {"email": "john@test.com"}
        enriched_contact = {
            "full_name": "John Doe",
            "personal_anecdotes": ["Loves hiking", "Plays guitar"],
            "professional_anecdotes": ["10 years in property mgmt"],
            "seed_urls": []
        }

        with patch.object(orchestrator.deduplicator, "is_duplicate", return_value=False), \
             patch.object(orchestrator.deduplicator, "mark_seen"), \
             patch.object(orchestrator.enrichment, "hostname_from_url", return_value="test.com"), \
             patch.object(orchestrator.enrichment, "is_pms_portal_host", return_value=False), \
             patch.object(orchestrator.enrichment, "verify_contact", return_value=verification), \
             patch.object(orchestrator.enrichment, "enrich_contact", return_value=enriched_contact), \
             patch.object(orchestrator, "_salvage_contact_anecdotes", return_value=False), \
             patch.object(orchestrator, "_track_api_call"), \
             patch("time.sleep"):

            contacts = orchestrator._discover_and_verify_contacts(enriched_company, original_company)

            # Should get verified contacts from decision makers
            assert len(contacts) > 0
            assert contacts[0]["full_name"] == "John Doe"
            assert contacts[0]["email"] == "john@test.com"

    def test_discover_fallback_to_webhook(self, base_config, skip_health_check):
        """Should call contact discovery webhook if no decision makers."""
        orchestrator = LeadOrchestrator(base_config)

        enriched_company = {
            "company_name": "Test Co",
            "domain": "test.com",
            "decision_makers": []  # No decision makers
        }
        original_company = {"company_name": "Test Co", "domain": "test.com"}

        discovered_contacts = [
            {"full_name": "John Doe", "title": "CEO"},
        ]

        verification = {"email": "john@test.com"}
        enriched_contact = {
            "full_name": "John Doe",
            "personal_anecdotes": ["Anecdote 1", "Anecdote 2"],
            "professional_anecdotes": ["Work fact 1"],
            "seed_urls": []
        }

        with patch.object(orchestrator.deduplicator, "is_duplicate", return_value=False), \
             patch.object(orchestrator.deduplicator, "mark_seen"), \
             patch.object(orchestrator.enrichment, "discover_contacts", return_value=discovered_contacts) as mock_discover, \
             patch.object(orchestrator.enrichment, "hostname_from_url", return_value="test.com"), \
             patch.object(orchestrator.enrichment, "is_pms_portal_host", return_value=False), \
             patch.object(orchestrator.enrichment, "verify_contact", return_value=verification), \
             patch.object(orchestrator.enrichment, "enrich_contact", return_value=enriched_contact), \
             patch.object(orchestrator, "_salvage_contact_anecdotes", return_value=False), \
             patch.object(orchestrator, "_track_api_call"), \
             patch("time.sleep"):

            contacts = orchestrator._discover_and_verify_contacts(enriched_company, original_company)

            # Should have called contact discovery webhook
            mock_discover.assert_called()
            assert len(contacts) > 0

    def test_discover_rejects_invalid_names(self, base_config, skip_health_check):
        """Should reject generic/invalid person names."""
        orchestrator = LeadOrchestrator(base_config)

        enriched_company = {
            "company_name": "Test Co",
            "domain": "test.com",
            "decision_makers": [
                {"full_name": "Office Manager", "title": "Manager"},  # Generic
                {"full_name": "Info Team", "title": "Contact"},       # Generic
                {"full_name": "John", "title": "CEO"},                # Single token
                {"full_name": "John123 Doe", "title": "CEO"},         # Has digit
            ]
        }
        original_company = {"company_name": "Test Co", "domain": "test.com"}

        contacts = orchestrator._discover_and_verify_contacts(enriched_company, original_company)

        # All names should be rejected
        assert len(contacts) == 0

    def test_discover_deduplicates_contacts(self, base_config, skip_health_check):
        """Should skip duplicate contacts."""
        orchestrator = LeadOrchestrator(base_config)

        enriched_company = {
            "company_name": "Test Co",
            "domain": "test.com",
            "decision_makers": [
                {"full_name": "John Doe", "title": "CEO"},
                {"full_name": "John Doe", "title": "CEO"},  # Duplicate
            ]
        }
        original_company = {"company_name": "Test Co", "domain": "test.com"}

        # First call returns False (not duplicate), second returns True (is duplicate)
        duplicate_checks = [False, True]

        with patch.object(orchestrator.deduplicator, "is_duplicate", side_effect=duplicate_checks), \
             patch.object(orchestrator.deduplicator, "mark_seen"):

            contacts = orchestrator._discover_and_verify_contacts(enriched_company, original_company)

            # Second contact should be skipped as duplicate
            orchestrator.deduplicator.is_duplicate.assert_called()

    def test_discover_rejects_role_based_emails(self, base_config, skip_health_check):
        """Should reject role-based email addresses."""
        orchestrator = LeadOrchestrator(base_config)

        enriched_company = {
            "company_name": "Test Co",
            "domain": "test.com",
            "decision_makers": [
                {"full_name": "John Doe", "title": "CEO"},
            ]
        }
        original_company = {"company_name": "Test Co", "domain": "test.com"}

        # Verification returns role-based email
        verification = {
            "email": "office@test.com",
            "raw": {"validations": {"is_role_based": True}}
        }

        with patch.object(orchestrator.deduplicator, "is_duplicate", return_value=False), \
             patch.object(orchestrator.deduplicator, "mark_seen"), \
             patch.object(orchestrator.enrichment, "hostname_from_url", return_value="test.com"), \
             patch.object(orchestrator.enrichment, "is_pms_portal_host", return_value=False), \
             patch.object(orchestrator.enrichment, "verify_contact", return_value=verification), \
             patch.object(orchestrator, "_track_api_call"):

            contacts = orchestrator._discover_and_verify_contacts(enriched_company, original_company)

            # Should reject role-based email
            assert len(contacts) == 0

    def test_discover_handles_verification_failure(self, base_config, skip_health_check):
        """Should handle email verification failures gracefully."""
        orchestrator = LeadOrchestrator(base_config)

        enriched_company = {
            "company_name": "Test Co",
            "domain": "test.com",
            "decision_makers": [
                {"full_name": "John Doe", "title": "CEO"},
            ]
        }
        original_company = {"company_name": "Test Co", "domain": "test.com"}

        with patch.object(orchestrator.deduplicator, "is_duplicate", return_value=False), \
             patch.object(orchestrator.deduplicator, "mark_seen"), \
             patch.object(orchestrator.enrichment, "hostname_from_url", return_value="test.com"), \
             patch.object(orchestrator.enrichment, "is_pms_portal_host", return_value=False), \
             patch.object(orchestrator.enrichment, "verify_contact", side_effect=Exception("Verification failed")), \
             patch.object(orchestrator, "_track_api_call"):

            contacts = orchestrator._discover_and_verify_contacts(enriched_company, original_company)

            # Should handle error and return empty list
            assert len(contacts) == 0
            # Should track failed API call
            orchestrator._track_api_call.assert_called_with("verification", success=False)

    def test_discover_enriches_and_quality_checks(self, base_config, skip_health_check):
        """Should enrich contact and apply quality checks."""
        orchestrator = LeadOrchestrator(base_config)

        enriched_company = {
            "company_name": "Test Co",
            "domain": "test.com",
            "decision_makers": [
                {"full_name": "John Doe", "title": "CEO"},
            ]
        }
        original_company = {"company_name": "Test Co", "domain": "test.com"}

        verification = {"email": "john@test.com"}

        # First enrichment fails quality check
        first_enrichment = {
            "full_name": "John Doe",
            "personal_anecdotes": [],  # Insufficient
            "professional_anecdotes": [],  # Insufficient
            "seed_urls": []
        }

        # Second enrichment passes
        second_enrichment = {
            "full_name": "John Doe",
            "personal_anecdotes": ["Anecdote 1", "Anecdote 2"],
            "professional_anecdotes": ["Work fact 1"],
            "seed_urls": []
        }

        with patch.object(orchestrator.deduplicator, "is_duplicate", return_value=False), \
             patch.object(orchestrator.deduplicator, "mark_seen"), \
             patch.object(orchestrator.enrichment, "hostname_from_url", return_value="test.com"), \
             patch.object(orchestrator.enrichment, "is_pms_portal_host", return_value=False), \
             patch.object(orchestrator.enrichment, "verify_contact", return_value=verification), \
             patch.object(orchestrator.enrichment, "enrich_contact", side_effect=[first_enrichment, second_enrichment]), \
             patch.object(orchestrator, "_salvage_contact_anecdotes", return_value=False), \
             patch.object(orchestrator, "_track_api_call"), \
             patch("time.sleep"):

            contacts = orchestrator._discover_and_verify_contacts(enriched_company, original_company)

            # Should re-enrich and succeed on second attempt
            assert orchestrator.enrichment.enrich_contact.call_count == 2
            assert len(contacts) > 0

    def test_discover_uses_salvage_on_quality_failure(self, base_config, skip_health_check):
        """Should attempt to salvage anecdotes when quality check fails."""
        orchestrator = LeadOrchestrator(base_config)
        orchestrator.config.contact_min_personal_anecdotes = 2
        orchestrator.config.contact_min_professional_anecdotes = 1

        enriched_company = {
            "company_name": "Test Co",
            "domain": "test.com",
            "decision_makers": [
                {"full_name": "John Doe", "title": "CEO"},
            ]
        }
        original_company = {"company_name": "Test Co", "domain": "test.com"}

        verification = {"email": "john@test.com"}

        # Enrichment has insufficient anecdotes initially
        enriched_contact = {
            "full_name": "John Doe",
            "personal_anecdotes": [],
            "professional_anecdotes": [],
            "seed_urls": [],
            "raw": {
                "personal": ["Hidden fact 1", "Hidden fact 2"],
                "professional": ["Work fact 1"],
            }
        }

        salvage_call_count = [0]

        def salvage_side_effect(contact):
            salvage_call_count[0] += 1
            # Salvage extracts from raw data
            contact["personal_anecdotes"] = ["Hidden fact 1", "Hidden fact 2"]
            contact["professional_anecdotes"] = ["Work fact 1"]
            return True

        with patch.object(orchestrator.deduplicator, "is_duplicate", return_value=False), \
             patch.object(orchestrator.deduplicator, "mark_seen"), \
             patch.object(orchestrator.enrichment, "hostname_from_url", return_value="test.com"), \
             patch.object(orchestrator.enrichment, "is_pms_portal_host", return_value=False), \
             patch.object(orchestrator.enrichment, "verify_contact", return_value=verification), \
             patch.object(orchestrator.enrichment, "enrich_contact", return_value=enriched_contact), \
             patch.object(orchestrator, "_salvage_contact_anecdotes", side_effect=salvage_side_effect), \
             patch.object(orchestrator, "_track_api_call"), \
             patch("time.sleep"):

            contacts = orchestrator._discover_and_verify_contacts(enriched_company, original_company)

            # Should have called salvage
            assert salvage_call_count[0] > 0
            # Should succeed after salvage
            assert len(contacts) > 0
            assert orchestrator.metrics["contacts_salvaged"] == 1

    def test_discover_limits_max_contacts(self, base_config, skip_health_check):
        """Should respect max contacts per company limit."""
        orchestrator = LeadOrchestrator(base_config)
        orchestrator.config.max_contacts_per_company = 2

        enriched_company = {
            "company_name": "Test Co",
            "domain": "test.com",
            "decision_makers": [
                {"full_name": "John Doe", "title": "CEO"},
                {"full_name": "Jane Smith", "title": "CFO"},
                {"full_name": "Bob Johnson", "title": "COO"},
                {"full_name": "Alice Brown", "title": "CTO"},
            ]
        }
        original_company = {"company_name": "Test Co", "domain": "test.com"}

        verification = {"email": "test@test.com"}
        enriched_contact = {
            "full_name": "Test",
            "personal_anecdotes": ["Anecdote 1", "Anecdote 2"],
            "professional_anecdotes": ["Work fact 1"],
            "seed_urls": []
        }

        with patch.object(orchestrator.deduplicator, "is_duplicate", return_value=False), \
             patch.object(orchestrator.deduplicator, "mark_seen"), \
             patch.object(orchestrator.enrichment, "hostname_from_url", return_value="test.com"), \
             patch.object(orchestrator.enrichment, "is_pms_portal_host", return_value=False), \
             patch.object(orchestrator.enrichment, "verify_contact", return_value=verification), \
             patch.object(orchestrator.enrichment, "enrich_contact", return_value=enriched_contact), \
             patch.object(orchestrator, "_salvage_contact_anecdotes", return_value=False), \
             patch.object(orchestrator, "_track_api_call"), \
             patch("time.sleep"):

            contacts = orchestrator._discover_and_verify_contacts(enriched_company, original_company)

            # Should limit to max_contacts_per_company
            assert len(contacts) <= min(3, orchestrator.config.max_contacts_per_company)
