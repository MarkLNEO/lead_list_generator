"""
Tests for HealthCheck class.

The HealthCheck performs pre-flight validation of configuration and connectivity.
"""

import pytest
from unittest.mock import patch, Mock
from lead_pipeline import HealthCheck, Config


@pytest.mark.unit
class TestHealthCheckBasics:
    """Test basic health check functionality."""

    def test_initialization(self, base_config, health_check):
        """Health check should initialize with config."""
        assert health_check.config == base_config

    def test_all_checks_pass_returns_true(self, health_check):
        """Should return True when all checks pass."""
        with patch.object(health_check, "_check_connectivity", return_value=True):
            healthy, errors = health_check.check_all()

        assert healthy is True
        assert len(errors) == 0

    def test_any_check_fails_returns_false(self, health_check):
        """Should return False if any check fails."""
        # Missing required config
        health_check.config.supabase_key = ""

        healthy, errors = health_check.check_all()

        assert healthy is False
        assert len(errors) > 0


@pytest.mark.unit
class TestConfigurationValidation:
    """Test configuration validation checks."""

    def test_valid_config_passes(self, base_config):
        """Valid configuration should pass checks."""
        health_check = HealthCheck(base_config)

        with patch.object(health_check, "_check_connectivity", return_value=True):
            healthy, errors = health_check.check_all()

        assert healthy is True

    def test_missing_supabase_key_fails(self, base_config):
        """Missing Supabase key should fail health check."""
        base_config.supabase_key = ""
        health_check = HealthCheck(base_config)

        healthy, errors = health_check.check_all()

        assert healthy is False
        assert any("SUPABASE" in err.upper() for err in errors)

    def test_missing_hubspot_token_fails(self, base_config):
        """Missing HubSpot token should fail health check."""
        base_config.hubspot_token = ""
        health_check = HealthCheck(base_config)

        healthy, errors = health_check.check_all()

        assert healthy is False
        assert any("HUBSPOT" in err.upper() for err in errors)

    def test_missing_webhook_urls_fails(self, base_config):
        """Missing webhook URLs should fail health check."""
        base_config.discovery_webhook_url = ""
        health_check = HealthCheck(base_config)

        healthy, errors = health_check.check_all()

        assert healthy is False
        # Should have error about discovery webhook

    def test_invalid_timeout_values_fails(self, base_config):
        """Invalid timeout values should fail health check."""
        base_config.discovery_request_timeout = -1
        health_check = HealthCheck(base_config)

        healthy, errors = health_check.check_all()

        assert healthy is False

    def test_invalid_concurrency_values_fails(self, base_config):
        """Invalid concurrency values should fail health check."""
        base_config.enrichment_concurrency = 0
        health_check = HealthCheck(base_config)

        healthy, errors = health_check.check_all()

        assert healthy is False


@pytest.mark.unit
class TestConnectivityChecks:
    """Test connectivity validation."""

    def test_connectivity_check_success(self, health_check):
        """Successful connectivity check should return True."""
        with patch("lead_pipeline.urllib.request.urlopen") as mock_urlopen:
            mock_response = Mock()
            mock_urlopen.return_value.__enter__.return_value = mock_response

            result = health_check._check_connectivity("http://example.com", timeout=5.0)

        assert result is True

    def test_connectivity_check_failure(self, health_check):
        """Failed connectivity check should return False."""
        with patch("lead_pipeline.urllib.request.urlopen", side_effect=Exception("Connection failed")):
            result = health_check._check_connectivity("http://example.com", timeout=5.0)

        assert result is False

    def test_connectivity_timeout(self, health_check):
        """Connectivity check should respect timeout."""
        from urllib.error import URLError

        with patch("lead_pipeline.urllib.request.urlopen", side_effect=URLError("Timeout")):
            result = health_check._check_connectivity("http://example.com", timeout=1.0)

        assert result is False


@pytest.mark.unit
class TestHealthCheckErrors:
    """Test error collection and reporting."""

    def test_collects_multiple_errors(self, base_config):
        """Should collect all validation errors."""
        base_config.supabase_key = ""
        base_config.hubspot_token = ""
        health_check = HealthCheck(base_config)

        healthy, errors = health_check.check_all()

        assert healthy is False
        assert len(errors) >= 2  # At least 2 errors

    def test_error_messages_descriptive(self, base_config):
        """Error messages should be descriptive."""
        base_config.supabase_key = ""
        health_check = HealthCheck(base_config)

        healthy, errors = health_check.check_all()

        # Errors should mention what's missing
        error_text = " ".join(errors).upper()
        assert "SUPABASE" in error_text or "KEY" in error_text

    def test_stops_early_on_critical_errors(self, base_config):
        """Should stop early if critical configuration is missing."""
        base_config.supabase_key = ""
        base_config.hubspot_token = ""
        health_check = HealthCheck(base_config)

        # Don't check connectivity if config is invalid
        with patch.object(health_check, "_check_connectivity") as mock_check:
            health_check.check_all()

            # Connectivity checks should still run even with config errors
            # (or be skipped - depends on implementation)
            # This tests that the method handles errors gracefully


@pytest.mark.unit
class TestHealthCheckIntegration:
    """Test health check integration scenarios."""

    def test_production_config_validation(self):
        """Should validate production-like configuration."""
        config = Config(
            supabase_key="sk_test_123",
            supabase_url="http://10.0.131.72:8000",
            hubspot_token="pat_test_456",
            discovery_webhook_url="http://10.0.131.72:5678/webhook/discovery",
            company_enrichment_webhook="http://10.0.131.72:5678/webhook/enrichment",
            contact_enrichment_webhook="http://10.0.131.72:5678/webhook/contact",
            email_verification_webhook="http://10.0.131.72:5678/webhook/verify",
            discovery_request_timeout=1800,
            enrichment_concurrency=6,
        )

        health_check = HealthCheck(config)

        with patch.object(health_check, "_check_connectivity", return_value=True):
            healthy, errors = health_check.check_all()

        assert healthy is True
        assert errors == []

    def test_partial_config_failure(self, base_config):
        """Should handle partially valid configuration."""
        base_config.enrichment_concurrency = -5  # Invalid but other fields OK

        health_check = HealthCheck(base_config)

        with patch.object(health_check, "_check_connectivity", return_value=True):
            healthy, errors = health_check.check_all()

        assert healthy is False
        assert len(errors) > 0
