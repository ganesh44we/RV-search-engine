from __future__ import annotations

import fakeredis
import time

from redis_search_engine.engine import RedisSearchEngine


def test_index_and_search_returns_ranked_results() -> None:
    engine = RedisSearchEngine(fakeredis.FakeRedis())
    engine.add_document("1", "Redis makes search fast and flexible", title="Redis", popularity=2)
    engine.add_document("2", "Search engines rank documents with tf idf", title="Ranking")
    engine.add_document("3", "Redis stores data structures in memory", title="Storage")

    response = engine.search("redis search")

    assert response.total == 3
    assert [result.document_id for result in response.results[:2]] == ["1", "3"]
    assert response.results[0].document["title"] == "Redis"


def test_reindex_replaces_previous_terms() -> None:
    engine = RedisSearchEngine(fakeredis.FakeRedis())
    engine.add_document("1", "redis search", title="Old")
    engine.add_document("1", "vector database", title="New")

    redis_response = engine.search("redis")
    vector_response = engine.search("vector")

    assert redis_response.results == []
    assert vector_response.results[0].document["title"] == "New"


def test_remove_document_cleans_up_document() -> None:
    engine = RedisSearchEngine(fakeredis.FakeRedis())
    engine.add_document("1", "redis search", title="Redis")

    assert engine.remove_document("1") is True
    assert engine.get_document("1") is None
    assert engine.search("redis").results == []


def test_phrase_and_boolean_filters_are_applied() -> None:
    engine = RedisSearchEngine(fakeredis.FakeRedis())
    engine.add_document("1", "redis search engine with phrase matching", title="Phrase")
    engine.add_document("2", "redis engine without exact adjacent search phrase", title="Loose")
    engine.add_document("3", "redis search engine but not python", title="Negative")

    response = engine.search('+"redis search" -python')

    assert [result.document_id for result in response.results] == ["1"]


def test_spelling_correction_expands_nearby_terms() -> None:
    engine = RedisSearchEngine(fakeredis.FakeRedis())
    engine.add_document("1", "redis search engine", title="Redis")

    response = engine.search("redsi", typo_tolerance=True)

    assert response.results[0].document_id == "1"
    assert response.expanded_terms["redsi"] == ["redis"]
    assert response.query_explanation["spelling_corrections"]["redsi"] == "redis"


def test_pagination_metadata_is_returned() -> None:
    engine = RedisSearchEngine(fakeredis.FakeRedis())
    for idx in range(5):
        engine.add_document(str(idx), f"redis search document {idx}", title=f"Doc {idx}", popularity=idx)

    response = engine.search("redis", offset=1, limit=2)

    assert response.total == 5
    assert response.offset == 1
    assert response.limit == 2
    assert len(response.results) == 2


def test_reindex_skips_unchanged_document() -> None:
    engine = RedisSearchEngine(fakeredis.FakeRedis())

    first = engine.add_document("1", "redis search engine", title="Doc", popularity=3)
    second = engine.add_document("1", "redis search engine", title="Doc", popularity=3)

    assert first == second == 4
    response = engine.search("redis")
    assert response.total == 1
    assert response.results[0].document["title"] == "Doc"


def test_batch_add_documents_indexes_multiple_documents() -> None:
    engine = RedisSearchEngine(fakeredis.FakeRedis())

    results = engine.add_documents(
        [
            {"id": "1", "title": "Redis", "content": "redis search engine"},
            {"id": "2", "title": "Vector", "content": "vector search database"},
        ]
    )

    assert [result["id"] for result in results] == ["1", "2"]
    assert engine.search("redis").results[0].document["title"] == "Redis"
    assert engine.search("vector").results[0].document["title"] == "Vector"


def test_synonyms_and_aliases_match_documents() -> None:
    engine = RedisSearchEngine(fakeredis.FakeRedis())
    engine.add_document("1", "car rental near airport", title="Cars")

    response = engine.search("automobile")

    assert response.results[0].document_id == "1"
    assert "car" in response.expanded_terms["car"]


def test_geo_search_filters_results_by_radius() -> None:
    engine = RedisSearchEngine(fakeredis.FakeRedis())
    engine.add_document("1", "hotel in the city center", title="Near", latitude=12.9716, longitude=77.5946)
    engine.add_document("2", "hotel far away", title="Far", latitude=28.6139, longitude=77.2090)

    response = engine.search("hotel", latitude=12.9716, longitude=77.5946, radius_km=10)

    assert [result.document_id for result in response.results] == ["1"]
    assert response.query_explanation["geo_filter"]["radius_km"] == 10


def test_result_explanation_includes_score_breakdown() -> None:
    engine = RedisSearchEngine(fakeredis.FakeRedis())
    engine.add_document("1", "redis search engine", title="Redis", popularity=4)

    response = engine.search('+"redis search"')

    explanation = response.results[0].explanation
    assert explanation["tfidf_score"] >= 0
    assert explanation["phrase_boost"] > 0
    assert "redis" in explanation["matched_terms"]


def test_proximity_search_matches_terms_within_distance() -> None:
    engine = RedisSearchEngine(fakeredis.FakeRedis())
    engine.add_document("1", "redis fast engine for search", title="Near")
    engine.add_document("2", "redis one two three four engine", title="Far")

    response = engine.search('+"redis engine"~2')

    assert [result.document_id for result in response.results] == ["1"]
    assert response.query_explanation["required_proximity"] == [{"terms": "redis engine", "distance": 2}]
    assert response.results[0].explanation["proximity_boost"] > 0


def test_title_matches_rank_above_content_only_matches() -> None:
    engine = RedisSearchEngine(fakeredis.FakeRedis())
    engine.add_document("1", "generic document body", title="Redis Search")
    engine.add_document("2", "redis search appears only in the body text here", title="Generic")

    response = engine.search("redis search")

    assert response.results[0].document_id == "1"


def test_alias_field_contributes_to_ranking() -> None:
    engine = RedisSearchEngine(fakeredis.FakeRedis())
    engine.add_document("1", "plain search content", title="Main", aliases=["rv"])
    engine.add_document("2", "rv mentioned once in content", title="Other")

    response = engine.search("rv")

    assert response.results[0].document_id == "1"


def test_relevance_profile_is_reported_in_query_and_result_explanations() -> None:
    engine = RedisSearchEngine(fakeredis.FakeRedis())
    engine.add_document("1", "redis search engine", title="Redis", popularity=10)

    response = engine.search("redis", relevance_profile="trending")

    assert response.relevance_profile == "trending"
    assert response.query_explanation["relevance_profile"] == "trending"
    assert response.results[0].explanation["relevance_profile"] == "trending"


def test_fresh_profile_boosts_recent_documents_more_than_balanced() -> None:
    engine = RedisSearchEngine(fakeredis.FakeRedis())
    now = time.time()
    engine.add_document("1", "redis search engine", title="Recent", updated_at=now)
    engine.add_document("2", "redis search engine", title="Older", updated_at=now - 86400 * 15, popularity=20)

    balanced = engine.search("redis", relevance_profile="balanced")
    fresh = engine.search("redis", relevance_profile="fresh")

    assert balanced.results[0].document_id in {"1", "2"}
    assert fresh.results[0].document_id == "1"


def test_backlinks_and_load_time_contribute_to_ranking() -> None:
    engine = RedisSearchEngine(fakeredis.FakeRedis())
    engine.add_document("1", "redis search engine", title="Fast", back_links=5, load_time=0.4)
    engine.add_document("2", "redis search engine", title="Slow", back_links=0, load_time=3.5)

    response = engine.search("redis", relevance_profile="balanced")

    assert response.results[0].document_id == "1"
    assert response.results[0].explanation["backlink_boost"] > 0
    assert response.results[0].explanation["load_time_boost"] > 0


def test_metadata_fields_are_indexed() -> None:
    engine = RedisSearchEngine(fakeredis.FakeRedis())
    engine.add_document(
        "1",
        "generic content",
        title="Page",
        description="redis page description",
        headings=["Redis Overview"],
        main_keywords=["redis", "search"],
        url_keywords=["redis-page"],
        back_link_keywords=["redis"],
    )

    response = engine.search("overview")

    assert response.results[0].document_id == "1"


def test_image_index_and_search() -> None:
    engine = RedisSearchEngine(fakeredis.FakeRedis())
    engine.add_image(
        "img-1",
        image_url="https://example.com/a.png",
        site_title="Redis Search",
        site_url="https://example.com",
        alt_tag="redis engine logo",
    )

    results = engine.search_images("logo")

    assert results[0]["id"] == "img-1"
