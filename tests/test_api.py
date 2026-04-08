from __future__ import annotations

import fakeredis
from fastapi.testclient import TestClient

from redis_search_engine.main import create_app
from redis_search_engine.external_search import ExternalSearchError, ExternalSearchResult


class FakeGoogleClient:
    def search(self, query: str, *, limit: int = 5) -> list[ExternalSearchResult]:
        return [
            ExternalSearchResult(
                provider="google",
                title=f"Google result for {query}",
                url="https://example.com/google",
                snippet="External info",
                metadata={"display_link": "example.com"},
            )
        ]


class FakeYahooClient:
    def search(self, query: str, *, limit: int = 5) -> list[ExternalSearchResult]:
        raise ExternalSearchError("Yahoo backend search is unavailable.")


def test_document_lifecycle_and_search() -> None:
    app = create_app(fakeredis.FakeRedis(), google_client=FakeGoogleClient(), yahoo_client=FakeYahooClient())
    client = TestClient(app)

    index_response = client.post(
        "/documents",
        json={
            "id": "doc-1",
            "title": "Redis Search",
            "content": "Redis can power a small tf idf search engine",
            "url": "https://example.com/doc-1",
            "popularity": 5,
            "aliases": ["rv"],
            "latitude": 12.9716,
            "longitude": 77.5946,
        },
    )
    assert index_response.status_code == 200
    assert index_response.json()["indexed_terms"] > 0

    search_response = client.get(
        "/search",
        params={"q": "redsi", "latitude": 12.9716, "longitude": 77.5946, "radius_km": 5},
    )
    assert search_response.status_code == 200
    body = search_response.json()
    assert body["total"] == 1
    assert body["returned"] == 1
    assert body["expanded_terms"]["redsi"] == ["redis"]
    assert body["query_explanation"]["spelling_corrections"]["redsi"] == "redis"
    assert body["did_you_mean"] == "redis"
    assert body["results"][0]["document"]["title"] == "Redis Search"
    assert "explanation" in body["results"][0]

    get_response = client.get("/documents/doc-1")
    assert get_response.status_code == 200
    assert "_tokens" not in get_response.json()

    delete_response = client.delete("/documents/doc-1")
    assert delete_response.status_code == 200
    assert delete_response.json() == {"deleted": True}


def test_search_can_include_external_google_results() -> None:
    app = create_app(fakeredis.FakeRedis(), google_client=FakeGoogleClient(), yahoo_client=FakeYahooClient())
    client = TestClient(app)

    client.post(
        "/documents",
        json={"id": "doc-1", "title": "Redis Search", "content": "redis search engine"},
    )

    response = client.get("/search", params={"q": "redis", "include_external": True, "external_provider": "google"})

    assert response.status_code == 200
    body = response.json()
    assert body["external_error"] is None
    assert body["external_results"][0]["provider"] == "google"
    assert body["external_results"][0]["title"] == "Google result for redis"


def test_search_reports_external_provider_error() -> None:
    app = create_app(fakeredis.FakeRedis(), google_client=FakeGoogleClient(), yahoo_client=FakeYahooClient())
    client = TestClient(app)

    client.post(
        "/documents",
        json={"id": "doc-1", "title": "Redis Search", "content": "redis search engine"},
    )

    response = client.get("/search", params={"q": "redis", "include_external": True, "external_provider": "yahoo"})

    assert response.status_code == 200
    body = response.json()
    assert body["external_results"] == []
    assert body["external_error"] == "Yahoo backend search is unavailable."


def test_search_accepts_relevance_profile() -> None:
    app = create_app(fakeredis.FakeRedis(), google_client=FakeGoogleClient(), yahoo_client=FakeYahooClient())
    client = TestClient(app)

    client.post(
        "/documents",
        json={"id": "doc-1", "title": "Redis Search", "content": "redis search engine"},
    )

    response = client.get("/search", params={"q": "redis", "relevance_profile": "precision"})

    assert response.status_code == 200
    body = response.json()
    assert body["relevance_profile"] == "precision"
    assert body["query_explanation"]["relevance_profile"] == "precision"
    assert isinstance(body["related_queries"], list)


def test_document_endpoint_accepts_website_metadata_and_image_search_endpoint_works() -> None:
    app = create_app(fakeredis.FakeRedis(), google_client=FakeGoogleClient(), yahoo_client=FakeYahooClient())
    client = TestClient(app)

    document_response = client.post(
        "/documents",
        json={
            "id": "doc-1",
            "title": "Redis Search",
            "content": "generic content",
            "description": "redis description",
            "headings": ["Redis Overview"],
            "main_keywords": ["redis", "search"],
            "back_links": 10,
            "load_time": 0.5,
        },
    )
    assert document_response.status_code == 200

    image_response = client.post(
        "/images",
        json={
            "id": "img-1",
            "image_url": "https://example.com/logo.png",
            "site_title": "Redis Search",
            "site_url": "https://example.com",
            "alt_tag": "redis logo",
        },
    )
    assert image_response.status_code == 200

    search_response = client.get("/image-search", params={"q": "logo"})
    assert search_response.status_code == 200
    assert search_response.json()["results"][0]["id"] == "img-1"


def test_bootstrap_demo_and_stats_endpoints_populate_searchable_data() -> None:
    app = create_app(fakeredis.FakeRedis(), google_client=FakeGoogleClient(), yahoo_client=FakeYahooClient())
    client = TestClient(app)

    before = client.get("/stats")
    assert before.status_code == 200
    assert before.json()["documents"] == 0

    bootstrap = client.post("/bootstrap-demo")
    assert bootstrap.status_code == 200
    assert bootstrap.json()["documents"] >= 3
    assert bootstrap.json()["images"] >= 2

    after = client.get("/stats")
    assert after.status_code == 200
    assert after.json()["documents"] >= 3

    web_search = client.get("/search", params={"q": "redis"})
    image_search = client.get("/image-search", params={"q": "logo"})
    suggestions = client.get("/suggest", params={"q": "red"})
    assert web_search.status_code == 200
    assert web_search.json()["total"] >= 1
    assert image_search.status_code == 200
    assert len(image_search.json()["results"]) >= 1
    assert suggestions.status_code == 200
    assert len(suggestions.json()["suggestions"]) >= 1
