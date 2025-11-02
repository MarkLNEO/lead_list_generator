import os
import sys

sys.path.insert(0, os.path.abspath(os.path.join(os.path.dirname(__file__), '..')))
from lead_pipeline import Config, evaluate_contact_quality


def test_reject_free_email_domains():
    cfg = Config()
    contact = {
        "email": "hs4020@gmail.com",
        "personal_anecdotes": ["great"]
    }
    passed, stats = evaluate_contact_quality(contact, cfg)
    assert not passed
    assert stats["reason"] == "non_business_email"

