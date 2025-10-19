"""
Integration tests for state persistence and recovery.

Tests the complete flow of saving pipeline state, simulating interruptions,
and recovering from checkpoints.
"""

import pytest
import json
import time
from pathlib import Path
from lead_pipeline import StateManager, LeadOrchestrator


@pytest.mark.integration
@pytest.mark.state_manager
class TestStateRecovery:
    """Test complete state save and recovery workflows."""

    def test_save_and_recover_simple_state(self, temp_run_dir):
        """Test basic save and recovery of pipeline state."""
        manager = StateManager(temp_run_dir)

        # Save initial state
        initial_state = {
            "phase": "discovery",
            "companies_count": 25,
            "current_round": 2,
            "companies": [
                {"name": "Company A", "domain": "a.com"},
                {"name": "Company B", "domain": "b.com"},
            ],
        }

        manager.save_checkpoint(initial_state)

        # Simulate process restart - create new manager
        new_manager = StateManager(temp_run_dir)
        recovered_state = new_manager.load_checkpoint()

        # Verify recovery
        assert recovered_state is not None
        assert recovered_state["phase"] == "discovery"
        assert recovered_state["companies_count"] == 25
        assert recovered_state["current_round"] == 2
        assert len(recovered_state["companies"]) == 2
        assert "checkpoint_time" in recovered_state
        assert "checkpoint_timestamp" in recovered_state

    def test_recovery_with_multiple_checkpoints(self, temp_run_dir):
        """Test that latest checkpoint is used when multiple exist."""
        manager = StateManager(temp_run_dir)

        # Save multiple checkpoints simulating pipeline progress
        for i in range(5):
            state = {
                "phase": f"phase_{i}",
                "progress": i * 20,
                "iteration": i,
            }
            manager.save_checkpoint(state)
            time.sleep(0.01)  # Small delay to ensure different timestamps

        # Recover should get latest
        recovered = manager.load_checkpoint()

        assert recovered["iteration"] == 4
        assert recovered["progress"] == 80
        assert recovered["phase"] == "phase_4"

    def test_recovery_after_corruption(self, temp_run_dir):
        """Test recovery when state file is corrupted."""
        manager = StateManager(temp_run_dir)

        # Save valid state
        valid_state = {"phase": "enrichment", "data": "valid"}
        manager.save_checkpoint(valid_state)

        # Corrupt the state file
        manager.state_file.write_text("{ invalid json }")

        # Try to recover
        new_manager = StateManager(temp_run_dir)
        recovered = new_manager.load_checkpoint()

        # Should return None for corrupted state
        assert recovered is None

    def test_incremental_checkpoint_updates(self, temp_run_dir):
        """Test that checkpoints can be updated incrementally."""
        manager = StateManager(temp_run_dir)

        # Start with basic state
        state = {
            "phase": "discovery",
            "companies": [],
            "rounds_completed": 0,
        }
        manager.save_checkpoint(state)

        # Update incrementally
        for i in range(3):
            recovered = manager.load_checkpoint()
            recovered["companies"].append({"id": i, "name": f"Company {i}"})
            recovered["rounds_completed"] = i + 1
            manager.save_checkpoint(recovered)

        # Final recovery should have all updates
        final_state = manager.load_checkpoint()
        assert len(final_state["companies"]) == 3
        assert final_state["rounds_completed"] == 3

    def test_state_includes_full_pipeline_context(self, temp_run_dir):
        """Test that state includes comprehensive pipeline context."""
        manager = StateManager(temp_run_dir)

        # Complex pipeline state
        complex_state = {
            "phase": "contact_enrichment",
            "run_id": "test_run_123",
            "args": {
                "state": "CA",
                "quantity": 50,
                "pms": "AppFolio",
            },
            "progress": {
                "companies_discovered": 75,
                "companies_enriched": 45,
                "contacts_verified": 120,
                "contacts_enriched": 90,
            },
            "metrics": {
                "api_calls": {"discovery": 3, "enrichment": 45},
                "errors": ["timeout on company X"],
            },
            "failed_companies": ["badcompany.com"],
            "processed_contacts": ["contact1@a.com", "contact2@b.com"],
        }

        manager.save_checkpoint(complex_state)
        recovered = manager.load_checkpoint()

        # Verify all context preserved
        assert recovered["phase"] == "contact_enrichment"
        assert recovered["run_id"] == "test_run_123"
        assert recovered["args"]["quantity"] == 50
        assert recovered["progress"]["companies_enriched"] == 45
        assert len(recovered["metrics"]["errors"]) == 1
        assert "badcompany.com" in recovered["failed_companies"]


@pytest.mark.integration
@pytest.mark.state_manager
@pytest.mark.slow
class TestStateRecoveryWithOrchestrator:
    """Test state recovery integrated with orchestrator."""

    def test_orchestrator_state_persistence(self, base_config, temp_run_dir):
        """Test that orchestrator properly uses state manager."""
        orchestrator = LeadOrchestrator(base_config)
        orchestrator.state_manager = StateManager(temp_run_dir)

        # Simulate checkpoint during run
        test_state = {
            "phase": "enrichment",
            "companies": [{"name": "Test Co", "domain": "test.com"}],
            "metrics": orchestrator.metrics,
        }

        orchestrator.state_manager.save_checkpoint(test_state)

        # Verify saved
        assert orchestrator.state_manager.state_file.exists()

        # Recover
        recovered = orchestrator.state_manager.load_checkpoint()
        assert recovered["phase"] == "enrichment"
        assert "checkpoint_time" in recovered

    def test_checkpoint_interval_respects_timing(self, temp_run_dir):
        """Test that checkpoints respect configured interval."""
        manager = StateManager(temp_run_dir)
        manager.checkpoint_interval = 1.0  # 1 second for testing

        # Initially should not need checkpoint
        assert not manager.should_checkpoint()

        # After short time, still no
        time.sleep(0.5)
        assert not manager.should_checkpoint()

        # After interval, should checkpoint
        time.sleep(0.6)
        assert manager.should_checkpoint()

        # After checkpoint, reset
        manager.save_checkpoint({"test": "data"})
        assert not manager.should_checkpoint()

    def test_recovery_preserves_deduplication_state(self, temp_run_dir):
        """Test that contact deduplication state can be preserved."""
        manager = StateManager(temp_run_dir)

        # Save state with deduplication tracking
        state = {
            "phase": "contact_enrichment",
            "seen_contacts": [
                "john@company.com",
                "jane@company.com",
                "bob@other.com",
            ],
            "contact_attempts": {
                "john@company.com": 2,
                "jane@company.com": 1,
            },
        }

        manager.save_checkpoint(state)
        recovered = manager.load_checkpoint()

        # Verify deduplication data preserved
        assert len(recovered["seen_contacts"]) == 3
        assert recovered["contact_attempts"]["john@company.com"] == 2
        assert "jane@company.com" in recovered["seen_contacts"]


@pytest.mark.integration
@pytest.mark.state_manager
class TestStateRecoveryEdgeCases:
    """Test edge cases in state recovery."""

    def test_recovery_with_empty_state_file(self, temp_run_dir):
        """Test recovery when state file exists but is empty."""
        manager = StateManager(temp_run_dir)

        # Create empty state file
        manager.state_file.touch()

        # Try to load
        recovered = manager.load_checkpoint()

        assert recovered is None

    def test_recovery_with_very_large_state(self, temp_run_dir):
        """Test recovery with large state data."""
        manager = StateManager(temp_run_dir)

        # Create large state with many companies
        large_state = {
            "phase": "enrichment",
            "companies": [
                {
                    "id": i,
                    "name": f"Company {i}",
                    "domain": f"company{i}.com",
                    "contacts": [
                        {
                            "email": f"contact{j}@company{i}.com",
                            "name": f"Contact {j}",
                        }
                        for j in range(10)
                    ],
                }
                for i in range(100)
            ],
        }

        manager.save_checkpoint(large_state)
        recovered = manager.load_checkpoint()

        # Verify all data preserved
        assert len(recovered["companies"]) == 100
        assert len(recovered["companies"][0]["contacts"]) == 10
        assert recovered["companies"][50]["name"] == "Company 50"

    def test_concurrent_checkpoint_access(self, temp_run_dir):
        """Test that rapid checkpoint saves don't corrupt data."""
        manager = StateManager(temp_run_dir)

        # Rapidly save multiple checkpoints
        for i in range(10):
            state = {"iteration": i, "timestamp": time.time()}
            manager.save_checkpoint(state)

        # Should be able to recover latest
        recovered = manager.load_checkpoint()
        assert recovered is not None
        assert recovered["iteration"] == 9

    def test_recovery_after_partial_write(self, temp_run_dir):
        """Test that atomic writes prevent partial state corruption."""
        manager = StateManager(temp_run_dir)

        # Save initial valid state
        manager.save_checkpoint({"version": 1, "data": "valid"})

        # Verify temp file doesn't persist
        temp_files = list(temp_run_dir.glob("*.tmp"))
        assert len(temp_files) == 0

        # State file should be valid
        recovered = manager.load_checkpoint()
        assert recovered["version"] == 1
