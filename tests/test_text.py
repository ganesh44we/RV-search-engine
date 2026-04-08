from __future__ import annotations

from redis_search_engine.text import contains_phrase, contains_proximity, parse_query, tokenize


def test_tokenize_applies_stemming_lemmatization_and_aliases() -> None:
    assert tokenize("running cars js") == ["run", "car", "javascript"]


def test_parse_query_supports_boolean_phrases_and_alias_resolution() -> None:
    parsed = parse_query('nyc +"search engine" -python -"old engine"')

    assert parsed.optional_terms == ["new york city"]
    assert parsed.required_terms == ["search", "engine"]
    assert parsed.excluded_terms == ["python", "old", "engine"]
    assert parsed.required_phrases == [["search", "engine"]]
    assert parsed.excluded_phrases == [["old", "engine"]]
    assert parsed.alias_matches["nyc"] == "new york city"


def test_parse_query_supports_proximity_normalization() -> None:
    parsed = parse_query('+"redis engine"~3 js')

    assert parsed.required_proximity == [(["redis", "engine"], 3)]
    assert parsed.optional_terms == ["javascript"]
    assert parsed.normalized_query == '+"redis engine"~3 javascript'


def test_contains_phrase_checks_adjacency() -> None:
    assert contains_phrase(["redis", "search", "engine"], ["search", "engine"]) is True
    assert contains_phrase(["redis", "engine", "search"], ["search", "engine"]) is False


def test_contains_proximity_checks_term_distance() -> None:
    assert contains_proximity(["redis", "fast", "engine"], ["redis", "engine"], 2) is True
    assert contains_proximity(["redis", "one", "two", "three", "engine"], ["redis", "engine"], 2) is False
