from __future__ import annotations

import os
from time import time

from fastapi import FastAPI, HTTPException, Query
from pydantic import BaseModel, Field
from redis import Redis

from redis_search_engine.engine import RedisSearchEngine
from redis_search_engine.external_search import ExternalSearchError, GoogleProgrammableSearchClient, YahooSearchClient


class DocumentIn(BaseModel):
    id: str = Field(..., min_length=1)
    title: str = Field(..., min_length=1)
    content: str = Field(..., min_length=1)
    url: str | None = None
    popularity: float = 0.0
    updated_at: float | None = None
    latitude: float | None = None
    longitude: float | None = None
    aliases: list[str] = Field(default_factory=list)
    synonyms: list[str] = Field(default_factory=list)
    description: str | None = None
    first_few_words: str | None = None
    load_time: float | None = None
    back_links: int = 0
    back_link_keywords: list[str] = Field(default_factory=list)
    url_keywords: list[str] = Field(default_factory=list)
    main_keywords: list[str] = Field(default_factory=list)
    headings: list[str] = Field(default_factory=list)
    favicon: str | None = None
    og_image: str | None = None


class ImageIn(BaseModel):
    id: str = Field(..., min_length=1)
    image_url: str = Field(..., min_length=1)
    site_title: str | None = None
    site_url: str | None = None
    alt_tag: str | None = None


DEMO_DOCUMENTS = [
    {
        "id": "rv-doc-1",
        "title": "Redis Search Overview",
        "content": "Redis can power a search engine with tf idf ranking, aliases, synonyms, and proximity search.",
        "description": "An overview of RV Search Engine features.",
        "headings": ["Redis Search", "Ranking", "Query Processing"],
        "main_keywords": ["redis", "search", "ranking"],
        "url_keywords": ["redis-search"],
        "back_link_keywords": ["redis", "engine"],
        "back_links": 18,
        "load_time": 0.42,
        "url": "https://rv.local/docs/overview",
        "first_few_words": "Redis can power a search engine",
    },
    {
        "id": "rv-doc-2",
        "title": "Local Hotel Discovery",
        "content": "Find hotels near the airport with geo search, freshness ranking, and field-aware matching.",
        "description": "Hotel discovery page with geo-aware relevance.",
        "headings": ["Hotels", "Airport Stay"],
        "main_keywords": ["hotel", "airport", "geo search"],
        "back_links": 8,
        "load_time": 0.31,
        "latitude": 12.9716,
        "longitude": 77.5946,
        "url": "https://rv.local/docs/hotels",
    },
    {
        "id": "rv-doc-3",
        "title": "JavaScript Query Normalization",
        "content": "JS queries normalize to javascript and support spelling correction plus proximity search.",
        "description": "Normalization and typo tolerance guide.",
        "aliases": ["js"],
        "synonyms": ["javascript"],
        "main_keywords": ["javascript", "query normalization"],
        "back_links": 12,
        "load_time": 0.56,
        "url": "https://rv.local/docs/javascript",
    },
]

DEMO_IMAGES = [
    {
        "id": "rv-img-1",
        "image_url": "https://images.unsplash.com/photo-1518770660439-4636190af475",
        "site_title": "Redis Search Overview",
        "site_url": "https://rv.local/docs/overview",
        "alt_tag": "redis engine logo dashboard",
    },
    {
        "id": "rv-img-2",
        "image_url": "https://images.unsplash.com/photo-1500530855697-b586d89ba3ee",
        "site_title": "Local Hotel Discovery",
        "site_url": "https://rv.local/docs/hotels",
        "alt_tag": "hotel near airport exterior",
    },
]


class SearchResponseModel(BaseModel):
    total: int
    offset: int
    limit: int
    returned: int
    took_ms: float
    relevance_profile: str
    expanded_terms: dict[str, list[str]]
    query_explanation: dict
    did_you_mean: str | None = None
    related_queries: list[str]
    external_results: list[dict]
    external_error: str | None = None
    results: list[dict]


def create_app(
    redis_client: Redis | None = None,
    google_client: GoogleProgrammableSearchClient | None = None,
    yahoo_client: YahooSearchClient | None = None,
) -> FastAPI:
    client = redis_client or Redis.from_url(
        os.getenv("REDIS_URL", "redis://localhost:6379/0"),
        decode_responses=False,
    )
    engine = RedisSearchEngine(client)
    google_client = google_client or GoogleProgrammableSearchClient()
    yahoo_client = yahoo_client or YahooSearchClient()
    app = FastAPI(title="RV Search Engine", version="0.3.0")

    @app.get("/health")
    def health() -> dict[str, str]:
        client.ping()
        return {"status": "ok"}

    @app.get("/stats")
    def stats() -> dict[str, int]:
        return {
            "documents": engine.count_documents(),
            "images": engine.count_images(),
        }

    @app.post("/bootstrap-demo")
    def bootstrap_demo() -> dict[str, int]:
        engine.add_documents(DEMO_DOCUMENTS)
        for image in DEMO_IMAGES:
            engine.add_image(
                image["id"],
                image_url=image["image_url"],
                site_title=image.get("site_title"),
                site_url=image.get("site_url"),
                alt_tag=image.get("alt_tag"),
            )
        return {
            "documents": engine.count_documents(),
            "images": engine.count_images(),
        }

    @app.post("/documents")
    def index_document(document: DocumentIn) -> dict[str, int | str]:
        indexed_terms = engine.add_document(
            document.id,
            document.content,
            title=document.title,
            url=document.url,
            popularity=document.popularity,
            updated_at=document.updated_at or time(),
            latitude=document.latitude,
            longitude=document.longitude,
            aliases=document.aliases,
            synonyms=document.synonyms,
            description=document.description,
            first_few_words=document.first_few_words,
            load_time=document.load_time,
            back_links=document.back_links,
            back_link_keywords=document.back_link_keywords,
            url_keywords=document.url_keywords,
            main_keywords=document.main_keywords,
            headings=document.headings,
            favicon=document.favicon,
            og_image=document.og_image,
        )
        return {"id": document.id, "indexed_terms": indexed_terms}

    @app.post("/images")
    def index_image(image: ImageIn) -> dict[str, int | str]:
        indexed_terms = engine.add_image(
            image.id,
            image_url=image.image_url,
            site_title=image.site_title,
            site_url=image.site_url,
            alt_tag=image.alt_tag,
        )
        return {"id": image.id, "indexed_terms": indexed_terms}

    @app.get("/image-search")
    def image_search(q: str = Query(..., min_length=1), limit: int = Query(10, ge=1, le=50)) -> dict[str, list[dict]]:
        return {"results": engine.search_images(q, limit=limit)}

    @app.get("/suggest")
    def suggest(q: str = Query(..., min_length=1), limit: int = Query(6, ge=1, le=10)) -> dict[str, list[str]]:
        return {"suggestions": engine.suggest_queries(q, limit=limit)}

    @app.delete("/documents/{document_id}")
    def delete_document(document_id: str) -> dict[str, bool]:
        deleted = engine.remove_document(document_id)
        if not deleted:
            raise HTTPException(status_code=404, detail="Document not found")
        return {"deleted": True}

    @app.get("/documents/{document_id}")
    def get_document(document_id: str) -> dict:
        document = engine.get_document(document_id)
        if document is None:
            raise HTTPException(status_code=404, detail="Document not found")
        return document

    @app.get("/search", response_model=SearchResponseModel)
    def search(
        q: str = Query(..., min_length=1),
        offset: int = Query(0, ge=0),
        limit: int = Query(10, ge=1, le=50),
        typo_tolerance: bool = Query(True),
        relevance_profile: str = Query("balanced", pattern="^(balanced|precision|fresh|trending|local)$"),
        phrase_boost: float = Query(0.75, ge=0.0),
        proximity_boost: float = Query(0.45, ge=0.0),
        recency_weight: float = Query(0.25, ge=0.0),
        popularity_weight: float = Query(0.2, ge=0.0),
        latitude: float | None = Query(None),
        longitude: float | None = Query(None),
        radius_km: float | None = Query(None, gt=0.0),
        include_external: bool = Query(False),
        external_provider: str = Query("google", pattern="^(google|yahoo)$"),
        external_limit: int = Query(5, ge=1, le=10),
    ) -> SearchResponseModel:
        response = engine.search(
            q,
            offset=offset,
            limit=limit,
            typo_tolerance=typo_tolerance,
            relevance_profile=relevance_profile,
            phrase_boost=phrase_boost,
            proximity_boost=proximity_boost,
            recency_weight=recency_weight,
            popularity_weight=popularity_weight,
            latitude=latitude,
            longitude=longitude,
            radius_km=radius_km,
        )

        external_results: list[dict] = []
        external_error: str | None = None
        if include_external:
            provider = google_client if external_provider == "google" else yahoo_client
            try:
                provider_results = provider.search(q, limit=external_limit)
                external_results = [
                    {
                        "provider": result.provider,
                        "title": result.title,
                        "url": result.url,
                        "snippet": result.snippet,
                        "metadata": result.metadata,
                    }
                    for result in provider_results
                ]
            except ExternalSearchError as exc:
                external_error = str(exc)

        return SearchResponseModel(
            total=response.total,
            offset=response.offset,
            limit=response.limit,
            returned=len(response.results),
            took_ms=response.took_ms,
            relevance_profile=response.relevance_profile,
            expanded_terms=response.expanded_terms,
            query_explanation=response.query_explanation,
            did_you_mean=response.did_you_mean,
            related_queries=response.related_queries,
            external_results=external_results,
            external_error=external_error,
            results=[
                {
                    "id": result.document_id,
                    "score": result.score,
                    "document": result.document,
                    "explanation": result.explanation,
                }
                for result in response.results
            ],
        )

    return app


app = create_app()
