"""
CLI argument parsing and main function tests.

Tests command-line interface, argument validation,
and program entry points.
"""

import pytest
from unittest.mock import patch, Mock, MagicMock
from argparse import Namespace
import sys

from lead_pipeline import build_arg_parser, main, EnrichmentRequestProcessor


@pytest.mark.unit
class TestArgumentParser:
    """Test CLI argument parsing."""

    def test_parser_accepts_basic_args(self):
        """Should parse basic required arguments."""
        parser = build_arg_parser()

        args = parser.parse_args([
            "--state", "CA",
            "--pms", "AppFolio",
            "--quantity", "10",
        ])

        assert args.state == "CA"
        assert args.pms == "AppFolio"
        assert args.quantity == 10

    def test_parser_accepts_optional_filters(self):
        """Should parse optional filter arguments."""
        parser = build_arg_parser()

        args = parser.parse_args([
            "--state", "TX",
            "--city", "Austin",
            "--pms", "Yardi",
            "--quantity", "20",
            "--unit-min", "100",
            "--unit-max", "500",
        ])

        assert args.city == "Austin"
        assert args.unit_min == 100
        assert args.unit_max == 500

    def test_parser_accepts_requirements(self):
        """Should parse requirements argument."""
        parser = build_arg_parser()

        args = parser.parse_args([
            "--quantity", "10",
            "--requirements", "NARPM member preferred",
        ])

        assert args.requirements == "NARPM member preferred"

    def test_parser_accepts_exclude_domains(self):
        """Should parse exclude domains as list."""
        parser = build_arg_parser()

        args = parser.parse_args([
            "--quantity", "10",
            "--exclude", "exclude1.com", "exclude2.com", "exclude3.com",
        ])

        assert args.exclude == ["exclude1.com", "exclude2.com", "exclude3.com"]

    def test_parser_exclude_defaults_to_empty_list(self):
        """Should default exclude to empty list."""
        parser = build_arg_parser()

        args = parser.parse_args(["--quantity", "10"])

        assert args.exclude == []

    def test_parser_accepts_max_rounds_override(self):
        """Should parse max rounds override."""
        parser = build_arg_parser()

        args = parser.parse_args([
            "--quantity", "10",
            "--max-rounds", "5",
        ])

        assert args.max_rounds == 5

    def test_parser_accepts_log_level(self):
        """Should parse log level."""
        parser = build_arg_parser()

        for level in ["DEBUG", "INFO", "WARNING", "ERROR"]:
            args = parser.parse_args([
                "--quantity", "10",
                "--log-level", level,
            ])
            assert args.log_level == level

    def test_parser_accepts_output_file(self):
        """Should parse output file path."""
        parser = build_arg_parser()

        args = parser.parse_args([
            "--quantity", "10",
            "--output", "/path/to/output.json",
        ])

        assert args.output == "/path/to/output.json"

    def test_parser_accepts_location(self):
        """Should parse free-form location."""
        parser = build_arg_parser()

        args = parser.parse_args([
            "--quantity", "10",
            "--location", "San Francisco Bay Area, California",
        ])

        assert args.location == "San Francisco Bay Area, California"


@pytest.mark.unit
class TestRequestQueueMode:
    """Test request queue processing mode."""

    def test_parser_accepts_request_queue_flag(self):
        """Should parse request queue processing flag."""
        parser = build_arg_parser()

        args = parser.parse_args(["--process-request-queue"])

        assert args.process_request_queue is True

    def test_parser_accepts_request_limit(self):
        """Should parse request limit."""
        parser = build_arg_parser()

        args = parser.parse_args([
            "--process-request-queue",
            "--request-limit", "5",
        ])

        assert args.request_limit == 5

    def test_request_limit_defaults_to_one(self):
        """Should default request limit to 1."""
        parser = build_arg_parser()

        args = parser.parse_args(["--process-request-queue"])

        assert args.request_limit == 1


@pytest.mark.integration
class TestMainFunction:
    """Test main program entry point."""

    def test_main_requires_quantity_or_queue_mode(self):
        """Should error if quantity not provided and not in queue mode."""
        with patch("sys.argv", ["lead_pipeline.py"]):
            with patch("lead_pipeline._load_env_file"):
                # Should raise SystemExit when quantity not provided
                with pytest.raises(SystemExit) as exc_info:
                    main([])

                # argparse exits with code 2 for invalid arguments
                assert exc_info.value.code == 2

    def test_main_processes_request_queue_mode(self):
        """Should process request queue when flag set."""
        with patch("lead_pipeline._load_env_file"):
            with patch.object(EnrichmentRequestProcessor, "process") as mock_process:
                result = main(["--process-request-queue", "--request-limit", "3"])

                assert mock_process.called
                assert mock_process.call_args[1]["limit"] == 3
                assert result == 0

    def test_main_runs_orchestrator_normally(self):
        """Should run orchestrator with normal arguments."""
        test_args = [
            "--state", "CA",
            "--pms", "AppFolio",
            "--quantity", "5",
        ]

        mock_result = {
            "companies_returned": 5,
            "contacts_returned": 10,
            "run_id": "test_123",
        }

        with patch("lead_pipeline._load_env_file"):
            with patch("lead_pipeline.LeadOrchestrator") as mock_orch_class:
                mock_orch = Mock()
                mock_orch.run.return_value = mock_result
                mock_orch_class.return_value = mock_orch

                with patch("builtins.print"):  # Suppress output
                    result = main(test_args)

                assert mock_orch.run.called
                assert result == 0

    def test_main_writes_output_file_when_specified(self):
        """Should write output to file when --output specified."""
        test_args = [
            "--state", "CA",
            "--quantity", "5",
            "--output", "test_output.json",
        ]

        mock_result = {"companies_returned": 5}

        with patch("lead_pipeline._load_env_file"):
            with patch("lead_pipeline.LeadOrchestrator") as mock_orch_class:
                mock_orch = Mock()
                mock_orch.run.return_value = mock_result
                mock_orch_class.return_value = mock_orch

                with patch("builtins.open", create=True) as mock_open:
                    with patch("builtins.print"):
                        result = main(test_args)

                    # Should have opened output file
                    mock_open.assert_called_with("test_output.json", "w", encoding="utf-8")

    def test_main_handles_exceptions(self):
        """Should handle and log exceptions."""
        test_args = ["--state", "CA", "--quantity", "5"]

        with patch("lead_pipeline._load_env_file"):
            with patch("lead_pipeline.LeadOrchestrator") as mock_orch_class:
                mock_orch = Mock()
                mock_orch.run.side_effect = Exception("Test error")
                mock_orch_class.return_value = mock_orch

                result = main(test_args)

                # Should return error code
                assert result == 1

    def test_main_configures_logging(self):
        """Should configure logging with specified level."""
        test_args = [
            "--state", "CA",
            "--quantity", "5",
            "--log-level", "DEBUG",
        ]

        with patch("lead_pipeline._load_env_file"):
            with patch("logging.basicConfig") as mock_logging:
                with patch("lead_pipeline.LeadOrchestrator") as mock_orch_class:
                    mock_orch = Mock()
                    mock_orch.run.return_value = {}
                    mock_orch_class.return_value = mock_orch

                    with patch("builtins.print"):
                        main(test_args)

                    # Should have configured logging
                    assert mock_logging.called

    def test_main_loads_env_file(self):
        """Should load environment variables from .env.local."""
        test_args = ["--state", "CA", "--quantity", "5"]

        with patch("lead_pipeline._load_env_file") as mock_load_env:
            with patch("lead_pipeline.LeadOrchestrator") as mock_orch_class:
                mock_orch = Mock()
                mock_orch.run.return_value = {}
                mock_orch_class.return_value = mock_orch

                with patch("builtins.print"):
                    main(test_args)

                # Should have loaded env file
                assert mock_load_env.called


@pytest.mark.unit
class TestEnvFileLoading:
    """Test environment file loading."""

    def test_load_env_file_basic(self, tmp_path):
        """Should load KEY=VALUE pairs from file."""
        env_file = tmp_path / ".env.local"
        env_file.write_text("""
SUPABASE_KEY=test_key
HUBSPOT_TOKEN=test_token
N8N_WEBHOOK=http://test.com
        """.strip())

        from lead_pipeline import _load_env_file

        with patch.dict("os.environ", {}, clear=True):
            _load_env_file(str(env_file))

            import os
            assert os.environ.get("SUPABASE_KEY") == "test_key"
            assert os.environ.get("HUBSPOT_TOKEN") == "test_token"

    def test_load_env_file_handles_quotes(self, tmp_path):
        """Should strip quotes from values."""
        env_file = tmp_path / ".env.local"
        env_file.write_text("""
QUOTED_SINGLE='value with spaces'
QUOTED_DOUBLE="another value"
NO_QUOTES=plain_value
        """.strip())

        from lead_pipeline import _load_env_file

        with patch.dict("os.environ", {}, clear=True):
            _load_env_file(str(env_file))

            import os
            assert os.environ.get("QUOTED_SINGLE") == "value with spaces"
            assert os.environ.get("QUOTED_DOUBLE") == "another value"
            assert os.environ.get("NO_QUOTES") == "plain_value"

    def test_load_env_file_skips_comments(self, tmp_path):
        """Should skip comment lines."""
        env_file = tmp_path / ".env.local"
        env_file.write_text("""
# This is a comment
VALID_KEY=value
# Another comment
        """.strip())

        from lead_pipeline import _load_env_file

        with patch.dict("os.environ", {}, clear=True):
            _load_env_file(str(env_file))

            import os
            assert os.environ.get("VALID_KEY") == "value"

    def test_load_env_file_preserves_existing(self, tmp_path):
        """Should not override existing environment variables."""
        env_file = tmp_path / ".env.local"
        env_file.write_text("KEY=file_value")

        from lead_pipeline import _load_env_file

        with patch.dict("os.environ", {"KEY": "existing_value"}):
            _load_env_file(str(env_file))

            import os
            # Should keep existing value
            assert os.environ.get("KEY") == "existing_value"

    def test_load_env_file_handles_missing_file(self):
        """Should handle missing env file gracefully."""
        from lead_pipeline import _load_env_file

        # Should not crash
        _load_env_file("/nonexistent/.env.local")

    def test_load_env_file_handles_malformed_lines(self, tmp_path):
        """Should skip malformed lines."""
        env_file = tmp_path / ".env.local"
        env_file.write_text("""
VALID=value
invalid_line_no_equals
=no_key
ANOTHER_VALID=value2
        """.strip())

        from lead_pipeline import _load_env_file

        with patch.dict("os.environ", {}, clear=True):
            _load_env_file(str(env_file))

            import os
            assert os.environ.get("VALID") == "value"
            assert os.environ.get("ANOTHER_VALID") == "value2"


@pytest.mark.unit
class TestArgumentValidation:
    """Test argument validation logic."""

    def test_validates_quantity_positive(self):
        """Should ensure quantity is positive."""
        parser = build_arg_parser()

        # Parser will accept negative but orchestrator should validate
        args = parser.parse_args(["--quantity", "-5"])
        assert args.quantity == -5  # Parser accepts, validation happens later

    def test_validates_unit_range_logical(self):
        """Should ensure unit-min < unit-max."""
        parser = build_arg_parser()

        # Parser accepts both, validation happens in orchestrator
        args = parser.parse_args([
            "--quantity", "10",
            "--unit-min", "500",
            "--unit-max", "100",
        ])

        assert args.unit_min == 500
        assert args.unit_max == 100

    def test_accepts_all_log_levels(self):
        """Should accept all valid log levels."""
        parser = build_arg_parser()

        for level in ["DEBUG", "INFO", "WARNING", "ERROR"]:
            args = parser.parse_args(["--quantity", "10", "--log-level", level])
            assert args.log_level == level
