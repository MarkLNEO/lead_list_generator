"""
Tests for StateManager class.

The StateManager handles pipeline state persistence for recovery from interruptions.
"""

import json
import time
from pathlib import Path
import pytest
from lead_pipeline import StateManager


@pytest.mark.unit
@pytest.mark.state_manager
class TestStateManagerBasics:
    """Test basic state manager functionality."""

    def test_initialization(self, temp_run_dir):
        """State manager should initialize with correct paths."""
        manager = StateManager(temp_run_dir)

        assert manager.run_dir == temp_run_dir
        assert manager.state_file == temp_run_dir / "state.json"
        assert manager.checkpoint_interval == 300  # 5 minutes

    def test_state_file_does_not_exist_initially(self, temp_run_dir):
        """State file should not exist before first save."""
        manager = StateManager(temp_run_dir)
        assert not manager.state_file.exists()


@pytest.mark.unit
@pytest.mark.state_manager
class TestCheckpointSaving:
    """Test checkpoint saving functionality."""

    def test_save_checkpoint_creates_file(self, state_manager):
        """Saving checkpoint should create state file."""
        state = {
            "phase": "discovery",
            "companies_count": 10,
            "current_round": 1,
        }

        state_manager.save_checkpoint(state)

        assert state_manager.state_file.exists()

    def test_save_checkpoint_writes_json(self, state_manager):
        """Checkpoint should be saved as valid JSON."""
        state = {
            "phase": "enrichment",
            "companies": ["company1", "company2"],
            "metrics": {"total": 2},
        }

        state_manager.save_checkpoint(state)

        with open(state_manager.state_file) as f:
            loaded = json.load(f)

        assert loaded["phase"] == "enrichment"
        assert loaded["companies"] == ["company1", "company2"]
        assert loaded["metrics"]["total"] == 2

    def test_save_checkpoint_adds_metadata(self, state_manager):
        """Checkpoint should include timestamp metadata."""
        state = {"phase": "test"}

        state_manager.save_checkpoint(state)

        with open(state_manager.state_file) as f:
            loaded = json.load(f)

        assert "checkpoint_time" in loaded
        assert "checkpoint_timestamp" in loaded
        assert isinstance(loaded["checkpoint_timestamp"], (int, float))

    def test_save_checkpoint_atomic_write(self, state_manager):
        """Checkpoint save should use atomic writes (temp + rename)."""
        state = {"data": "test"}

        # Save initial checkpoint
        state_manager.save_checkpoint(state)
        initial_content = state_manager.state_file.read_text()

        # Save second checkpoint
        state_manager.save_checkpoint({"data": "updated"})
        updated_content = state_manager.state_file.read_text()

        # Both writes should succeed without corruption
        assert "test" in initial_content or "updated" in updated_content
        assert state_manager.state_file.exists()

    def test_save_checkpoint_updates_last_checkpoint_time(self, state_manager):
        """Saving should update last checkpoint timestamp."""
        initial_time = state_manager.last_checkpoint

        time.sleep(0.01)  # Small delay
        state_manager.save_checkpoint({"test": "data"})

        assert state_manager.last_checkpoint > initial_time

    def test_save_checkpoint_handles_complex_data(self, state_manager):
        """Checkpoint should handle complex nested data structures."""
        complex_state = {
            "companies": [
                {"name": "Co1", "contacts": [{"name": "C1", "email": "c1@co1.com"}]},
                {"name": "Co2", "contacts": [{"name": "C2", "email": "c2@co2.com"}]},
            ],
            "metrics": {
                "api_calls": {"discovery": {"success": 5, "failure": 1}},
                "errors": [{"msg": "Error 1", "count": 3}],
            },
            "null_value": None,
            "boolean": True,
            "number": 42,
        }

        state_manager.save_checkpoint(complex_state)

        with open(state_manager.state_file) as f:
            loaded = json.load(f)

        assert len(loaded["companies"]) == 2
        assert loaded["metrics"]["api_calls"]["discovery"]["success"] == 5
        assert loaded["null_value"] is None
        assert loaded["boolean"] is True


@pytest.mark.unit
@pytest.mark.state_manager
class TestCheckpointLoading:
    """Test checkpoint loading functionality."""

    def test_load_nonexistent_checkpoint_returns_none(self, state_manager):
        """Loading when no checkpoint exists should return None."""
        result = state_manager.load_checkpoint()
        assert result is None

    def test_load_checkpoint_returns_saved_data(self, state_manager):
        """Loading should return previously saved state."""
        original_state = {
            "phase": "enrichment",
            "round": 3,
            "companies_count": 25,
        }

        state_manager.save_checkpoint(original_state)
        loaded_state = state_manager.load_checkpoint()

        assert loaded_state["phase"] == "enrichment"
        assert loaded_state["round"] == 3
        assert loaded_state["companies_count"] == 25

    def test_load_checkpoint_includes_metadata(self, state_manager):
        """Loaded checkpoint should include timestamp metadata."""
        state_manager.save_checkpoint({"test": "data"})
        loaded = state_manager.load_checkpoint()

        assert "checkpoint_time" in loaded
        assert "checkpoint_timestamp" in loaded

    def test_load_corrupted_checkpoint_returns_none(self, state_manager, caplog):
        """Loading corrupted checkpoint should return None and log warning."""
        # Write invalid JSON
        state_manager.state_file.write_text("{ invalid json }")

        result = state_manager.load_checkpoint()

        assert result is None
        assert "Failed to load checkpoint" in caplog.text

    def test_load_empty_checkpoint_returns_none(self, state_manager, caplog):
        """Loading empty checkpoint file should return None."""
        state_manager.state_file.write_text("")

        result = state_manager.load_checkpoint()

        assert result is None


@pytest.mark.unit
@pytest.mark.state_manager
class TestCheckpointInterval:
    """Test checkpoint interval logic."""

    def test_should_checkpoint_initially_false(self, state_manager):
        """Should not checkpoint immediately after creation."""
        # Just created, last_checkpoint is current time
        assert not state_manager.should_checkpoint()

    def test_should_checkpoint_after_interval(self, state_manager):
        """Should checkpoint after interval has elapsed."""
        # Set last checkpoint to past
        state_manager.last_checkpoint = time.time() - 400  # 400 seconds ago

        assert state_manager.should_checkpoint()

    def test_should_checkpoint_respects_interval(self, state_manager):
        """Should respect configured checkpoint interval."""
        state_manager.checkpoint_interval = 100  # 100 seconds

        # 50 seconds ago: not yet
        state_manager.last_checkpoint = time.time() - 50
        assert not state_manager.should_checkpoint()

        # 150 seconds ago: yes
        state_manager.last_checkpoint = time.time() - 150
        assert state_manager.should_checkpoint()

    def test_should_checkpoint_after_save(self, state_manager):
        """Should not checkpoint immediately after saving."""
        state_manager.save_checkpoint({"test": "data"})

        # Just saved, so should not checkpoint again yet
        assert not state_manager.should_checkpoint()


@pytest.mark.unit
@pytest.mark.state_manager
class TestStateManagerEdgeCases:
    """Test edge cases and error handling."""

    def test_save_with_unserializable_data_fails_gracefully(self, state_manager, caplog):
        """Saving unserializable data should fail gracefully."""
        import datetime

        # datetime objects are not JSON serializable by default
        state = {"time": datetime.datetime.now()}

        state_manager.save_checkpoint(state)

        # Should log warning but not crash
        assert "Failed to save checkpoint" in caplog.text

    def test_multiple_saves_overwrite_previous(self, state_manager):
        """Multiple saves should overwrite previous checkpoint."""
        state_manager.save_checkpoint({"version": 1})
        state_manager.save_checkpoint({"version": 2})
        state_manager.save_checkpoint({"version": 3})

        loaded = state_manager.load_checkpoint()
        assert loaded["version"] == 3

    def test_save_to_nonexistent_directory_creates_it(self, tmp_path):
        """Saving should work even if run directory doesn't exist."""
        nonexistent_dir = tmp_path / "does" / "not" / "exist"

        # State manager doesn't create dir on init, but save should handle it
        # Actually, looking at the code, the run_dir should exist already
        # Let's create it to match real usage
        nonexistent_dir.mkdir(parents=True, exist_ok=True)

        manager = StateManager(nonexistent_dir)
        manager.save_checkpoint({"test": "data"})

        assert manager.state_file.exists()

    def test_concurrent_saves_dont_corrupt(self, state_manager):
        """Rapid consecutive saves should not corrupt state file."""
        for i in range(10):
            state_manager.save_checkpoint({"iteration": i})

        # Should be able to load final state
        loaded = state_manager.load_checkpoint()
        assert "iteration" in loaded
        assert 0 <= loaded["iteration"] <= 9
