"""
Advanced configuration validation tests.

Tests edge cases, error conditions, and validation logic
for the Config class.
"""

import pytest
import os
from unittest.mock import patch

from lead_pipeline import Config


@pytest.mark.unit
class TestConfigEnvironmentVariables:
    """Test environment variable loading and precedence."""

    def test_config_uses_environment_variables(self):
        """Should load values from environment variables."""
        with patch.dict(os.environ, {
            "SUPABASE_SERVICE_KEY": "env_key",
            "HUBSPOT_PRIVATE_APP_TOKEN": "env_token",
        }):
            config = Config()

            assert config.supabase_key == "env_key"
            assert config.hubspot_token == "env_token"

    def test_config_fallback_order(self):
        """Should use fallback environment variable names."""
        with patch.dict(os.environ, {
            "SUPABASE_ANON_KEY": "anon_key",  # Fallback
            "HUBSPOT_API_KEY": "api_key",  # Fallback
        }, clear=True):
            config = Config()

            # Should use fallback when primary not set
            assert config.supabase_key == "anon_key"
            assert config.hubspot_token == "api_key"

    def test_config_primary_overrides_fallback(self):
        """Should prefer primary environment variable over fallback."""
        with patch.dict(os.environ, {
            "SUPABASE_SERVICE_KEY": "service_key",
            "SUPABASE_ANON_KEY": "anon_key",
        }):
            config = Config()

            # Should use primary
            assert config.supabase_key == "service_key"

    def test_config_default_values(self):
        """Should use default values when env vars not set."""
        with patch.dict(os.environ, {}, clear=True):
            config = Config()

            # Should have defaults
            assert config.supabase_url == "http://10.0.131.72:8000"
            assert config.supabase_research_table == "research_database"
            assert config.enrichment_concurrency == 3
            assert config.contact_concurrency == 3


@pytest.mark.unit
class TestConfigValidation:
    """Test configuration validation logic."""

    def test_validate_requires_supabase_key(self):
        """Should require Supabase key."""
        config = Config(supabase_key="")

        with pytest.raises(ValueError, match="Missing required configuration"):
            config.validate()

    def test_validate_requires_hubspot_token(self):
        """Should require HubSpot token."""
        config = Config(
            supabase_key="test",
            hubspot_token="",
        )

        with pytest.raises(ValueError, match="Missing required configuration"):
            config.validate()

    def test_validate_requires_discovery_webhook(self):
        """Should require discovery webhook URL."""
        config = Config(
            supabase_key="test",
            hubspot_token="test",
            discovery_webhook_url="",
        )

        with pytest.raises(ValueError, match="Missing required configuration"):
            config.validate()

    def test_validate_requires_company_enrichment_webhook(self):
        """Should require company enrichment webhook URL."""
        config = Config(
            supabase_key="test",
            hubspot_token="test",
            discovery_webhook_url="http://test.com",
            company_enrichment_webhook="",
        )

        with pytest.raises(ValueError, match="Missing required configuration"):
            config.validate()

    def test_validate_timeout_minimum(self):
        """Should enforce minimum timeout values."""
        config = Config(
            supabase_key="test",
            hubspot_token="test",
            discovery_webhook_url="http://test.com",
            company_enrichment_webhook="http://test.com",
            discovery_request_timeout=30,  # Too low (min 60)
        )

        with pytest.raises(ValueError, match="DISCOVERY_REQUEST_TIMEOUT too low"):
            config.validate()

    def test_validate_timeout_maximum(self):
        """Should enforce maximum timeout values."""
        config = Config(
            supabase_key="test",
            hubspot_token="test",
            discovery_webhook_url="http://test.com",
            company_enrichment_webhook="http://test.com",
            discovery_request_timeout=20000,  # Too high (max 14400)
        )

        with pytest.raises(ValueError, match="DISCOVERY_REQUEST_TIMEOUT too high"):
            config.validate()

    def test_validate_concurrency_minimum(self):
        """Should enforce minimum concurrency."""
        config = Config(
            supabase_key="test",
            hubspot_token="test",
            discovery_webhook_url="http://test.com",
            company_enrichment_webhook="http://test.com",
            enrichment_concurrency=0,
        )

        with pytest.raises(ValueError, match="ENRICHMENT_CONCURRENCY out of range"):
            config.validate()

    def test_validate_concurrency_maximum(self):
        """Should enforce maximum concurrency."""
        config = Config(
            supabase_key="test",
            hubspot_token="test",
            discovery_webhook_url="http://test.com",
            company_enrichment_webhook="http://test.com",
            enrichment_concurrency=30,  # Too high (max 20)
        )

        with pytest.raises(ValueError, match="ENRICHMENT_CONCURRENCY out of range"):
            config.validate()

    def test_validate_negative_anecdote_counts(self):
        """Should reject negative anecdote counts."""
        config = Config(
            supabase_key="test",
            hubspot_token="test",
            discovery_webhook_url="http://test.com",
            company_enrichment_webhook="http://test.com",
            contact_min_personal_anecdotes=-1,
        )

        with pytest.raises(ValueError, match="cannot be negative"):
            config.validate()

    def test_validate_anecdote_count_logic(self):
        """Should ensure total >= personal + professional."""
        config = Config(
            supabase_key="test",
            hubspot_token="test",
            discovery_webhook_url="http://test.com",
            company_enrichment_webhook="http://test.com",
            contact_min_personal_anecdotes=2,
            contact_min_professional_anecdotes=2,
            contact_min_total_anecdotes=3,  # Too low (should be >= 4)
        )

        with pytest.raises(ValueError, match="must be at least the sum"):
            config.validate()

    def test_validate_max_companies_minimum(self):
        """Should enforce minimum max_companies_per_run."""
        config = Config(
            supabase_key="test",
            hubspot_token="test",
            discovery_webhook_url="http://test.com",
            company_enrichment_webhook="http://test.com",
            max_companies_per_run=0,
        )

        with pytest.raises(ValueError, match="MAX_COMPANIES_PER_RUN too low"):
            config.validate()

    def test_validate_max_contacts_minimum(self):
        """Should enforce minimum max_contacts_per_company."""
        config = Config(
            supabase_key="test",
            hubspot_token="test",
            discovery_webhook_url="http://test.com",
            company_enrichment_webhook="http://test.com",
            max_contacts_per_company=0,
        )

        with pytest.raises(ValueError, match="MAX_CONTACTS_PER_COMPANY too low"):
            config.validate()

    def test_validate_max_enrichment_retries_nonnegative(self):
        """Should reject negative enrichment retries."""
        config = Config(
            supabase_key="test",
            hubspot_token="test",
            discovery_webhook_url="http://test.com",
            company_enrichment_webhook="http://test.com",
            max_enrichment_retries=-1,
        )

        with pytest.raises(ValueError, match="cannot be negative"):
            config.validate()

    def test_validate_all_checks_pass(self, base_config):
        """Should pass validation with valid config."""
        # Should not raise
        base_config.validate()


@pytest.mark.unit
class TestConfigDefaults:
    """Test default configuration values."""

    def test_default_timeouts(self):
        """Should have reasonable default timeouts."""
        with patch.dict(os.environ, {}, clear=True):
            config = Config()

            assert config.discovery_request_timeout == 1800  # 30 min
            assert config.company_enrichment_timeout == 7200  # 2 hours
            assert config.contact_enrichment_timeout == 7200  # 2 hours
            assert config.email_verification_timeout == 240  # 4 min

    def test_default_concurrency(self):
        """Should have reasonable default concurrency."""
        with patch.dict(os.environ, {}, clear=True):
            config = Config()

            assert config.enrichment_concurrency == 3
            assert config.contact_concurrency == 3

    def test_default_anecdote_requirements(self):
        """Should have reasonable default anecdote requirements."""
        with patch.dict(os.environ, {}, clear=True):
            config = Config()

            assert config.contact_min_personal_anecdotes == 0
            assert config.contact_min_professional_anecdotes == 0
            assert config.contact_min_total_anecdotes == 1

    def test_default_fallback_flags(self):
        """Should enable fallbacks by default."""
        with patch.dict(os.environ, {}, clear=True):
            config = Config()

            assert config.contact_allow_personalization_fallback is True
            assert config.contact_allow_seed_url_fallback is True

    def test_default_safety_limits(self):
        """Should have safety limits."""
        with patch.dict(os.environ, {}, clear=True):
            config = Config()

            assert config.max_companies_per_run == 500
            assert config.max_contacts_per_company == 10
            assert config.max_enrichment_retries == 2

    def test_default_circuit_breaker_settings(self):
        """Should have circuit breaker defaults."""
        with patch.dict(os.environ, {}, clear=True):
            config = Config()

            assert config.circuit_breaker_enabled is True
            assert config.circuit_breaker_threshold == 5
            assert config.circuit_breaker_timeout == 300.0


@pytest.mark.unit
class TestConfigTypeConversion:
    """Test type conversion from environment variables."""

    def test_converts_int_values(self):
        """Should convert string env vars to integers."""
        with patch.dict(os.environ, {
            "ENRICHMENT_CONCURRENCY": "5",
            "MAX_COMPANIES_PER_RUN": "100",
        }):
            config = Config()

            assert isinstance(config.enrichment_concurrency, int)
            assert config.enrichment_concurrency == 5
            assert isinstance(config.max_companies_per_run, int)
            assert config.max_companies_per_run == 100

    def test_converts_float_values(self):
        """Should convert string env vars to floats."""
        with patch.dict(os.environ, {
            "DISCOVERY_REQUEST_TIMEOUT": "1800.5",
        }):
            config = Config()

            assert isinstance(config.discovery_request_timeout, float)
            assert config.discovery_request_timeout == 1800.5

    def test_converts_boolean_values(self):
        """Should convert string env vars to booleans."""
        with patch.dict(os.environ, {
            "CIRCUIT_BREAKER_ENABLED": "false",
            "CONTACT_ALLOW_PERSONALIZATION_FALLBACK": "true",
        }):
            config = Config()

            assert isinstance(config.circuit_breaker_enabled, bool)
            assert config.circuit_breaker_enabled is False
            assert config.contact_allow_personalization_fallback is True


@pytest.mark.unit
class TestConfigEdgeCases:
    """Test edge cases and error conditions."""

    def test_handles_missing_optional_webhooks(self):
        """Should handle missing optional webhooks."""
        config = Config(
            supabase_key="test",
            hubspot_token="test",
            discovery_webhook_url="http://test.com",
            company_enrichment_webhook="http://test.com",
            contact_enrichment_webhook="",  # Optional
            email_verification_webhook="",  # Optional
        )

        # Should not raise during initialization
        assert config.contact_enrichment_webhook == ""

    def test_url_normalization(self):
        """Should normalize trailing slashes in URLs."""
        config = Config(
            supabase_url="http://test.com/",  # Has trailing slash
        )

        # SupabaseClient should strip trailing slash
        from lead_pipeline import SupabaseResearchClient
        client = SupabaseResearchClient(config)
        assert client.base_url == "http://test.com"

    def test_multiple_validation_errors(self):
        """Should report multiple validation errors."""
        config = Config(
            supabase_key="",  # Missing
            hubspot_token="",  # Missing
            discovery_request_timeout=10,  # Too low
        )

        with pytest.raises(ValueError) as exc_info:
            config.validate()

        # Should mention multiple issues
        error_msg = str(exc_info.value)
        assert "Missing required configuration" in error_msg or "Invalid configuration" in error_msg


@pytest.mark.unit
class TestConfigEmailSettings:
    """Test email notification configuration."""

    def test_email_configuration(self):
        """Should configure email settings."""
        with patch.dict(os.environ, {
            "NOTIFICATION_EMAIL": "test@example.com",
            "SMTP_HOST": "smtp.example.com",
            "SMTP_PORT": "587",
            "SMTP_USER": "user@example.com",
            "SMTP_PASSWORD": "password",
        }):
            config = Config()

            assert config.notify_email == "test@example.com"
            assert config.smtp_host == "smtp.example.com"
            assert config.smtp_port == 587
            assert config.smtp_user == "user@example.com"

    def test_fallback_email_addresses(self):
        """Should have fallback email addresses."""
        with patch.dict(os.environ, {}, clear=True):
            config = Config()

            assert config.failsafe_email == "mark@nevereverordinary.com"
            assert config.owner_email == "mlerner@rebarhq.ai"
