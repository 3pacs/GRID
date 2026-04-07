"""Tests for verification/ref_extractor.py"""

import sys
sys.path.insert(0, ".")

from verification.ref_extractor import extract_refs


def test_markdown_link_extraction():
    text = "See [Reuters](https://reuters.com/article) for details."
    refs = extract_refs(text)
    assert len(refs) == 1
    assert refs[0].url == "https://reuters.com/article"
    assert refs[0].anchor_text == "Reuters"
    assert refs[0].ref_type == "markdown_link"


def test_raw_url_extraction():
    text = "Visit https://example.com/data for more info."
    refs = extract_refs(text)
    assert len(refs) == 1
    assert refs[0].url == "https://example.com/data"
    assert refs[0].ref_type == "raw_url"


def test_doi_extraction():
    text = "Published at doi:10.1234/test.2026"
    refs = extract_refs(text)
    assert len(refs) == 1
    assert refs[0].url == "https://doi.org/10.1234/test.2026"
    assert refs[0].ref_type == "doi"


def test_deduplication():
    text = "See [link](https://example.com) and also https://example.com again."
    refs = extract_refs(text)
    assert len(refs) == 1


def test_empty_text():
    assert extract_refs("") == []
    assert extract_refs("No URLs here.") == []


def test_multiple_refs_sorted_by_position():
    text = "A https://first.com B [second](https://second.com) C doi:10.5678/third"
    refs = extract_refs(text)
    assert len(refs) == 3
    assert refs[0].url == "https://first.com"
    assert refs[1].url == "https://second.com"
    assert refs[2].url == "https://doi.org/10.5678/third"


def test_url_with_query_params():
    text = "Check https://api.example.com/data?key=val&page=2 for results."
    refs = extract_refs(text)
    assert len(refs) == 1
    assert "key=val" in refs[0].url


def test_url_trailing_punctuation_stripped():
    text = "Source: https://example.com/article."
    refs = extract_refs(text)
    assert refs[0].url == "https://example.com/article"
