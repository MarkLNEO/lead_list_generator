import os
import sys
import pytest

sys.path.insert(0, os.path.abspath(os.path.join(os.path.dirname(__file__), '..')))

from request_splitter import LLMRequestSplitter


def test_splitter_fallback_broad_areas(monkeypatch):
    # Ensure no LLM key so it uses fallback
    monkeypatch.delenv("OPENAI_API_KEY", raising=False)
    sp = LLMRequestSplitter(llm_provider="openai")
    chunks = sp.split_request(1, {"quantity": 20, "location": "Miami"})
    assert 2 <= len(chunks) <= 5
    # Each chunk should carry a requirements_suffix for discovery
    assert all("requirements_suffix" in c.parameters for c in chunks)


@pytest.mark.skipif(not os.getenv("OPENAI_API_KEY"), reason="requires OpenAI key")
def test_splitter_llm_neighborhoods():
    sp = LLMRequestSplitter(llm_provider="openai")
    chunks = sp.split_request(1, {"quantity": 20, "city": "Miami"})
    assert len(chunks) >= 2
    # Neighborhood based criteria in criteria string or suffix present
    assert all("requirements_suffix" in c.parameters for c in chunks)
