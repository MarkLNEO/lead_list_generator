"""
Tests for ContactDeduplicator class.

The deduplicator prevents duplicate processing of contacts across enrichment rounds.
"""

import pytest
from lead_pipeline import ContactDeduplicator


@pytest.mark.unit
class TestContactDeduplicatorBasics:
    """Test basic contact deduplication functionality."""

    def test_initialization(self, contact_deduplicator):
        """Deduplicator should initialize with empty seen set."""
        assert len(contact_deduplicator.seen_keys) == 0

    def test_first_contact_is_not_duplicate(self, contact_deduplicator, sample_contact, sample_company):
        """First occurrence of contact should not be marked as duplicate."""
        is_dup = contact_deduplicator.is_duplicate(sample_contact, sample_company)

        assert is_dup is False

    def test_second_occurrence_is_duplicate(self, contact_deduplicator, sample_contact, sample_company):
        """Second occurrence of same contact should be marked as duplicate."""
        # Mark as seen
        contact_deduplicator.mark_seen(sample_contact, sample_company)

        # Check again
        is_dup = contact_deduplicator.is_duplicate(sample_contact, sample_company)

        assert is_dup is True

    def test_mark_seen_updates_seen_set(self, contact_deduplicator, sample_contact, sample_company):
        """Marking contact as seen should add to seen set."""
        initial_count = len(contact_deduplicator.seen_keys)

        contact_deduplicator.mark_seen(sample_contact, sample_company)

        assert len(contact_deduplicator.seen_keys) == initial_count + 1


@pytest.mark.unit
class TestContactKeyGeneration:
    """Test contact key generation for deduplication."""

    def test_email_based_key(self, contact_deduplicator):
        """Primary key should be based on email if present."""
        contact = {"email": "john@example.com", "full_name": "John Smith"}
        company = {"domain": "example.com"}

        key = contact_deduplicator._contact_key(contact, company)

        assert "john@example.com" in key.lower()

    def test_linkedin_fallback(self, contact_deduplicator):
        """Should use LinkedIn URL if no email."""
        contact = {
            "full_name": "John Smith",
            "linkedin_url": "https://linkedin.com/in/johnsmith",
        }
        company = {"domain": "example.com"}

        key = contact_deduplicator._contact_key(contact, company)

        assert "linkedin.com/in/johnsmith" in key.lower()

    def test_name_company_fallback(self, contact_deduplicator):
        """Should use name+company if no email or LinkedIn."""
        contact = {"full_name": "John Smith"}
        company = {"domain": "example.com"}

        key = contact_deduplicator._contact_key(contact, company)

        assert "john smith" in key.lower()
        assert "example.com" in key.lower()

    def test_key_normalization(self, contact_deduplicator):
        """Keys should be normalized (lowercase, stripped)."""
        contact1 = {"email": "  John@Example.COM  "}
        contact2 = {"email": "john@example.com"}
        company = {"domain": "example.com"}

        key1 = contact_deduplicator._contact_key(contact1, company)
        key2 = contact_deduplicator._contact_key(contact2, company)

        # Should generate same key despite different casing/whitespace
        assert key1 == key2


@pytest.mark.unit
class TestDeduplicationScenarios:
    """Test various deduplication scenarios."""

    def test_different_emails_not_duplicates(self, contact_deduplicator, sample_company):
        """Contacts with different emails should not be duplicates."""
        contact1 = {"email": "john@example.com", "full_name": "John Smith"}
        contact2 = {"email": "jane@example.com", "full_name": "Jane Doe"}

        contact_deduplicator.mark_seen(contact1, sample_company)
        is_dup = contact_deduplicator.is_duplicate(contact2, sample_company)

        assert is_dup is False

    def test_same_name_different_company_not_duplicate(self, contact_deduplicator):
        """Same name at different companies should not be duplicate."""
        contact = {"full_name": "John Smith"}
        company1 = {"domain": "company1.com"}
        company2 = {"domain": "company2.com"}

        contact_deduplicator.mark_seen(contact, company1)
        is_dup = contact_deduplicator.is_duplicate(contact, company2)

        assert is_dup is False

    def test_email_case_insensitive(self, contact_deduplicator, sample_company):
        """Email deduplication should be case-insensitive."""
        contact1 = {"email": "John.Smith@Example.COM"}
        contact2 = {"email": "john.smith@example.com"}

        contact_deduplicator.mark_seen(contact1, sample_company)
        is_dup = contact_deduplicator.is_duplicate(contact2, sample_company)

        assert is_dup is True

    def test_multiple_contacts_tracked(self, contact_deduplicator, sample_company):
        """Should track multiple different contacts."""
        contacts = [
            {"email": "person1@example.com"},
            {"email": "person2@example.com"},
            {"email": "person3@example.com"},
        ]

        for contact in contacts:
            contact_deduplicator.mark_seen(contact, sample_company)

        assert len(contact_deduplicator.seen_keys) == 3

    def test_linkedin_deduplication(self, contact_deduplicator, sample_company):
        """Should deduplicate based on LinkedIn URL."""
        contact1 = {
            "full_name": "John Smith",
            "linkedin_url": "https://linkedin.com/in/johnsmith",
        }
        contact2 = {
            "full_name": "John Smith",
            "linkedin_url": "https://linkedin.com/in/johnsmith",
        }

        contact_deduplicator.mark_seen(contact1, sample_company)
        is_dup = contact_deduplicator.is_duplicate(contact2, sample_company)

        assert is_dup is True


@pytest.mark.unit
class TestAttemptCounting:
    """Test attempt counting functionality."""

    def test_initial_attempt_count_zero(self, contact_deduplicator, sample_contact, sample_company):
        """Attempt count for new contact should be zero."""
        count = contact_deduplicator.attempt_count_for(sample_contact, sample_company)

        assert count == 0

    def test_attempt_count_increments(self, contact_deduplicator, sample_contact, sample_company):
        """Attempt count should increment when marking as seen."""
        contact_deduplicator.mark_seen(sample_contact, sample_company)
        count1 = contact_deduplicator.attempt_count_for(sample_contact, sample_company)

        contact_deduplicator.mark_seen(sample_contact, sample_company)
        count2 = contact_deduplicator.attempt_count_for(sample_contact, sample_company)

        assert count1 == 1
        assert count2 == 2

    def test_different_contacts_separate_counts(self, contact_deduplicator, sample_company):
        """Different contacts should have separate attempt counts."""
        contact1 = {"email": "john@example.com"}
        contact2 = {"email": "jane@example.com"}

        contact_deduplicator.mark_seen(contact1, sample_company)
        contact_deduplicator.mark_seen(contact1, sample_company)
        contact_deduplicator.mark_seen(contact2, sample_company)

        count1 = contact_deduplicator.attempt_count_for(contact1, sample_company)
        count2 = contact_deduplicator.attempt_count_for(contact2, sample_company)

        assert count1 == 2
        assert count2 == 1


@pytest.mark.unit
class TestEdgeCases:
    """Test edge cases and error handling."""

    def test_empty_contact(self, contact_deduplicator):
        """Should handle contact with no identifying information."""
        contact = {}
        company = {"domain": "example.com"}

        # Should not crash
        key = contact_deduplicator._contact_key(contact, company)
        assert isinstance(key, str)

    def test_contact_with_only_company(self, contact_deduplicator):
        """Should handle contact with only company info."""
        contact = {"company": "Acme Corp"}
        company = {"domain": "acme.com"}

        key = contact_deduplicator._contact_key(contact, company)
        assert isinstance(key, str)

    def test_missing_company_domain(self, contact_deduplicator):
        """Should handle missing company domain."""
        contact = {"email": "john@example.com"}
        company = {}

        # Should not crash
        key = contact_deduplicator._contact_key(contact, company)
        assert "john@example.com" in key.lower()

    def test_none_values_handled(self, contact_deduplicator):
        """Should handle None values gracefully."""
        contact = {"email": None, "linkedin_url": None, "full_name": None}
        company = {"domain": None}

        # Should not crash
        key = contact_deduplicator._contact_key(contact, company)
        assert isinstance(key, str)

    def test_whitespace_only_values(self, contact_deduplicator):
        """Should handle whitespace-only values."""
        contact = {"email": "   ", "full_name": "\t\n"}
        company = {"domain": "example.com"}

        # Should not crash
        key = contact_deduplicator._contact_key(contact, company)
        assert isinstance(key, str)
