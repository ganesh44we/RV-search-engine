from __future__ import annotations

import json
import math
import time
import uuid
from collections import Counter
from dataclasses import dataclass
from difflib import get_close_matches
from hashlib import sha1
from typing import Any

from redis import Redis

from redis_search_engine.text import (
    ParsedQuery,
    contains_phrase,
    contains_proximity,
    expand_term_variants,
    parse_query,
    term_frequencies,
)

FIELD_WEIGHTS = {
    "title": 3.5,
    "content": 1.0,
    "description": 1.8,
    "first_words": 1.2,
    "headings": 2.2,
    "main_keywords": 2.5,
    "url_keywords": 1.7,
    "backlink_keywords": 1.4,
    "aliases": 2.25,
    "synonyms": 1.4,
}

RELEVANCE_BANK = {
    "balanced": {
        "tfidf_weight": 1.0,
        "phrase_boost": 0.75,
        "proximity_boost": 0.45,
        "recency_weight": 0.25,
        "popularity_weight": 0.2,
        "exact_title_boost": 0.9,
        "all_terms_present_boost": 0.35,
        "geo_decay_weight": 0.12,
        "backlink_weight": 0.015,
        "load_time_weight": 0.1,
    },
    "precision": {
        "tfidf_weight": 1.15,
        "phrase_boost": 1.05,
        "proximity_boost": 0.65,
        "recency_weight": 0.12,
        "popularity_weight": 0.1,
        "exact_title_boost": 1.2,
        "all_terms_present_boost": 0.55,
        "geo_decay_weight": 0.08,
        "backlink_weight": 0.012,
        "load_time_weight": 0.08,
    },
    "fresh": {
        "tfidf_weight": 0.95,
        "phrase_boost": 0.65,
        "proximity_boost": 0.35,
        "recency_weight": 0.9,
        "popularity_weight": 0.01,
        "exact_title_boost": 0.7,
        "all_terms_present_boost": 0.25,
        "geo_decay_weight": 0.1,
        "backlink_weight": 0.008,
        "load_time_weight": 0.05,
    },
    "trending": {
        "tfidf_weight": 0.9,
        "phrase_boost": 0.55,
        "proximity_boost": 0.3,
        "recency_weight": 0.3,
        "popularity_weight": 0.45,
        "exact_title_boost": 0.6,
        "all_terms_present_boost": 0.25,
        "geo_decay_weight": 0.08,
        "backlink_weight": 0.02,
        "load_time_weight": 0.04,
    },
    "local": {
        "tfidf_weight": 0.95,
        "phrase_boost": 0.6,
        "proximity_boost": 0.35,
        "recency_weight": 0.18,
        "popularity_weight": 0.15,
        "exact_title_boost": 0.6,
        "all_terms_present_boost": 0.3,
        "geo_decay_weight": 0.5,
        "backlink_weight": 0.01,
        "load_time_weight": 0.05,
    },
}


@dataclass(slots=True)
class SearchResult:
    document_id: str
    score: float
    document: dict[str, Any] | None
    explanation: dict[str, Any]


@dataclass(slots=True)
class SearchResponse:
    results: list[SearchResult]
    total: int
    offset: int
    limit: int
    took_ms: float
    expanded_terms: dict[str, list[str]]
    query_explanation: dict[str, Any]
    relevance_profile: str
    did_you_mean: str | None
    related_queries: list[str]


class RedisSearchEngine:
    def __init__(self, redis_client: Redis, prefix: str = "search:") -> None:
        self.redis = redis_client
        self.prefix = prefix
        self._lexicon_cache: set[str] | None = None

    def _indexed_key(self) -> str:
        return f"{self.prefix}indexed"

    def _indexed_images_key(self) -> str:
        return f"{self.prefix}images:indexed"

    def _document_key(self, document_id: str) -> str:
        return f"{self.prefix}doc:{document_id}"

    def _terms_key(self, document_id: str) -> str:
        return f"{self.prefix}terms:{document_id}"

    def _lexicon_key(self) -> str:
        return f"{self.prefix}lexicon"

    def _image_lexicon_key(self) -> str:
        return f"{self.prefix}image:lexicon"

    def _term_key(self, field: str, token: str) -> str:
        return f"{self.prefix}term:{field}:{token}"

    def _image_key(self, image_id: str) -> str:
        return f"{self.prefix}image:{image_id}"

    def _image_terms_key(self, image_id: str) -> str:
        return f"{self.prefix}image_terms:{image_id}"

    def _image_term_key(self, field: str, token: str) -> str:
        return f"{self.prefix}image_term:{field}:{token}"

    def add_document(
        self,
        document_id: str,
        content: str,
        *,
        title: str | None = None,
        url: str | None = None,
        popularity: float = 0.0,
        updated_at: float | None = None,
        latitude: float | None = None,
        longitude: float | None = None,
        aliases: list[str] | None = None,
        synonyms: list[str] | None = None,
        description: str | None = None,
        first_few_words: str | None = None,
        load_time: float | None = None,
        back_links: int | None = None,
        back_link_keywords: list[str] | None = None,
        url_keywords: list[str] | None = None,
        main_keywords: list[str] | None = None,
        headings: list[str] | None = None,
        favicon: str | None = None,
        og_image: str | None = None,
        **metadata: Any,
    ) -> int:
        existing_document = self._get_raw_document(document_id)
        normalized_popularity = float(popularity)
        normalized_updated_at = float(
            updated_at
            if updated_at is not None
            else existing_document.get("updated_at", time.time()) if existing_document is not None else time.time()
        )
        aliases = aliases or []
        synonyms = synonyms or []
        back_link_keywords = back_link_keywords or []
        url_keywords = url_keywords or []
        main_keywords = main_keywords or []
        headings = headings or []
        combined_text = " ".join(
            part for part in [title or "", description or "", first_few_words or "", content] if part
        )
        signature = self._document_signature(
            content=combined_text,
            title=title,
            url=url,
            popularity=normalized_popularity,
            updated_at=normalized_updated_at,
            metadata={
                **metadata,
                "aliases": aliases,
                "synonyms": synonyms,
                "description": description,
                "first_few_words": first_few_words,
                "load_time": load_time,
                "back_links": back_links,
                "back_link_keywords": back_link_keywords,
                "url_keywords": url_keywords,
                "main_keywords": main_keywords,
                "headings": headings,
                "favicon": favicon,
                "og_image": og_image,
                "latitude": latitude,
                "longitude": longitude,
            },
        )
        if existing_document is not None and existing_document.get("_signature") == signature:
            return int(existing_document.get("_indexed_terms", 0))

        self.remove_document(document_id)
        combined_tf, tokens, analysis = term_frequencies(combined_text)
        field_payloads = self._build_field_indices(
            title=title,
            content=content,
            description=description,
            first_few_words=first_few_words,
            back_link_keywords=back_link_keywords,
            url_keywords=url_keywords,
            main_keywords=main_keywords,
            headings=headings,
            aliases=aliases,
            synonyms=synonyms,
        )
        unique_terms = sorted({term for field_data in field_payloads.values() for term in field_data["tf"].keys()} | set(combined_tf.keys()))
        payload = {
            "id": document_id,
            "title": title,
            "content": content,
            "description": description,
            "first_few_words": first_few_words,
            "url": url,
            "popularity": normalized_popularity,
            "updated_at": normalized_updated_at,
            "load_time": load_time,
            "back_links": back_links or 0,
            "back_link_keywords": back_link_keywords,
            "url_keywords": url_keywords,
            "main_keywords": main_keywords,
            "headings": headings,
            "favicon": favicon,
            "og_image": og_image,
            "latitude": latitude,
            "longitude": longitude,
            "aliases": aliases,
            "synonyms": synonyms,
            "_tokens": tokens,
            "_terms": unique_terms,
            "_indexed_terms": len(unique_terms),
            "_signature": signature,
            "_alias_matches": analysis.alias_matches,
            "_synonym_matches": analysis.synonym_matches,
            "_field_terms": {field: sorted(data["tf"].keys()) for field, data in field_payloads.items()},
            **metadata,
        }

        pipe = self.redis.pipeline(transaction=False)
        pipe.sadd(self._indexed_key(), document_id)
        pipe.set(self._document_key(document_id), json.dumps(payload))
        if unique_terms:
            pipe.sadd(self._lexicon_key(), *unique_terms)
        term_members: list[str] = []
        for field, data in field_payloads.items():
            tf = data["tf"]
            if not tf:
                continue
            for token, value in tf.items():
                term_members.append(f"{field}|{token}")
                pipe.zadd(self._term_key(field, token), {document_id: value})
        if term_members:
            pipe.sadd(self._terms_key(document_id), *term_members)
        pipe.execute()
        self._invalidate_lexicon_cache()
        return len(unique_terms)

    def add_documents(self, documents: list[dict[str, Any]]) -> list[dict[str, int | str]]:
        results: list[dict[str, int | str]] = []
        for document in documents:
            document_id = str(document["id"])
            indexed_terms = self.add_document(
                document_id,
                str(document["content"]),
                title=document.get("title"),
                url=document.get("url"),
                popularity=float(document.get("popularity", 0.0)),
                updated_at=document.get("updated_at"),
                latitude=document.get("latitude"),
                longitude=document.get("longitude"),
                aliases=document.get("aliases"),
                synonyms=document.get("synonyms"),
                description=document.get("description"),
                first_few_words=document.get("first_few_words"),
                load_time=document.get("load_time"),
                back_links=document.get("back_links"),
                back_link_keywords=document.get("back_link_keywords"),
                url_keywords=document.get("url_keywords"),
                main_keywords=document.get("main_keywords"),
                headings=document.get("headings"),
                favicon=document.get("favicon"),
                og_image=document.get("og_image"),
                **{
                    key: value
                    for key, value in document.items()
                    if key
                    not in {
                        "id",
                        "content",
                        "title",
                        "url",
                        "popularity",
                        "updated_at",
                        "latitude",
                        "longitude",
                        "aliases",
                        "synonyms",
                        "description",
                        "first_few_words",
                        "load_time",
                        "back_links",
                        "back_link_keywords",
                        "url_keywords",
                        "main_keywords",
                        "headings",
                        "favicon",
                        "og_image",
                    }
                },
            )
            results.append({"id": document_id, "indexed_terms": indexed_terms})
        return results

    def add_image(
        self,
        image_id: str,
        *,
        image_url: str,
        site_title: str | None = None,
        site_url: str | None = None,
        alt_tag: str | None = None,
    ) -> int:
        payload = {
            "id": image_id,
            "image_url": image_url,
            "site_title": site_title,
            "site_url": site_url,
            "alt_tag": alt_tag,
        }
        field_payloads = {
            "site_title": term_frequencies(site_title or ""),
            "alt_tag": term_frequencies(alt_tag or ""),
        }
        pipe = self.redis.pipeline(transaction=False)
        pipe.sadd(self._indexed_images_key(), image_id)
        pipe.set(self._image_key(image_id), json.dumps(payload))
        image_terms: list[str] = []
        unique_terms: set[str] = set()
        for field, (tf, _tokens, _analysis) in field_payloads.items():
            for token, value in tf.items():
                unique_terms.add(token)
                image_terms.append(f"{field}|{token}")
                pipe.zadd(self._image_term_key(field, token), {image_id: value})
        if image_terms:
            pipe.sadd(self._image_terms_key(image_id), *image_terms)
        if unique_terms:
            pipe.sadd(self._image_lexicon_key(), *unique_terms)
        pipe.execute()
        return len(unique_terms)

    def search_images(self, query: str, *, limit: int = 10) -> list[dict[str, Any]]:
        parsed = parse_query(query)
        expanded_terms, _ = self._expand_terms(parsed.required_terms + parsed.optional_terms, typo_tolerance=True)
        positive_groups = self._positive_groups(parsed, expanded_terms)
        if not positive_groups:
            return []

        field_weights = {"site_title": 1.8, "alt_tag": 2.2}
        pipe = self.redis.pipeline(transaction=False)
        term_key_counts: Counter[str] = Counter()
        for group in positive_groups:
            for token in group:
                for field in field_weights:
                    key = self._image_term_key(field, token)
                    if key not in term_key_counts:
                        pipe.zcard(key)
                    term_key_counts[key] += 1
        frequencies = pipe.execute()
        weighted: dict[str, float] = {}
        for key, frequency in zip(term_key_counts.keys(), frequencies):
            if frequency:
                field = key.split(":")[-2]
                idf = max(math.log(max(len(term_key_counts), 1) / frequency, 2), 0.0) or 1e-9
                weighted[key] = idf * term_key_counts[key] * field_weights.get(field, 1.0)
        if not weighted:
            return []
        temp_key = f"{self.prefix}image_temp:{uuid.uuid4().hex}"
        try:
            self.redis.zunionstore(temp_key, weighted)
            rows = self.redis.zrevrange(temp_key, 0, limit - 1, withscores=False)
        finally:
            self.redis.delete(temp_key)

        image_ids = [self._decode(row) for row in rows]
        pipe = self.redis.pipeline(transaction=False)
        for image_id in image_ids:
            pipe.get(self._image_key(image_id))
        payloads = pipe.execute()
        return [json.loads(self._decode(payload)) for payload in payloads if payload is not None]

    def count_documents(self) -> int:
        return int(self.redis.scard(self._indexed_key()))

    def count_images(self) -> int:
        return int(self.redis.scard(self._indexed_images_key()))

    def suggest_queries(self, query: str, *, limit: int = 6) -> list[str]:
        parsed = parse_query(query)
        normalized = " ".join(parsed.required_terms + parsed.optional_terms).strip().lower()
        if not normalized:
            return []

        suggestions: list[str] = []
        seen: set[str] = set()
        for document in self._fetch_all_documents():
            candidates = [
                document.get("title"),
                *(document.get("headings") or []),
                *(document.get("main_keywords") or []),
            ]
            for candidate in candidates:
                if not candidate:
                    continue
                value = str(candidate).strip()
                if value.lower().startswith(normalized) and value.lower() not in seen:
                    seen.add(value.lower())
                    suggestions.append(value)
                    if len(suggestions) >= limit:
                        return suggestions

        for token in sorted(self._load_lexicon()):
            if token.startswith(normalized) and token not in seen:
                seen.add(token)
                suggestions.append(token)
                if len(suggestions) >= limit:
                    break
        return suggestions

    def remove_document(self, document_id: str) -> bool:
        terms_key = self._terms_key(document_id)
        terms = [self._decode(term) for term in self.redis.smembers(terms_key)]
        if not terms and not self.redis.exists(self._document_key(document_id)):
            return False

        pipe = self.redis.pipeline(transaction=False)
        pipe.srem(self._indexed_key(), document_id)
        pipe.delete(self._document_key(document_id))
        pipe.delete(terms_key)
        for member in terms:
            field, token = member.split("|", 1)
            pipe.zrem(self._term_key(field, token), document_id)
        pipe.execute()
        self._invalidate_lexicon_cache()
        return True

    def search(
        self,
        query: str,
        *,
        offset: int = 0,
        limit: int = 10,
        typo_tolerance: bool = True,
        relevance_profile: str = "balanced",
        phrase_boost: float = 0.75,
        proximity_boost: float = 0.45,
        recency_weight: float = 0.25,
        popularity_weight: float = 0.2,
        candidate_multiplier: int = 20,
        min_candidates: int = 100,
        latitude: float | None = None,
        longitude: float | None = None,
        radius_km: float | None = None,
    ) -> SearchResponse:
        started_at = time.perf_counter()
        ranking = self._resolve_ranking_config(
            relevance_profile=relevance_profile,
            phrase_boost=phrase_boost,
            proximity_boost=proximity_boost,
            recency_weight=recency_weight,
            popularity_weight=popularity_weight,
        )
        parsed = parse_query(query)
        did_you_mean = self._did_you_mean(query, {})
        related_queries = self._related_queries(parsed)
        positive_terms = parsed.required_terms + parsed.optional_terms
        has_structured_constraints = bool(
            parsed.optional_phrases
            or parsed.required_phrases
            or parsed.optional_proximity
            or parsed.required_proximity
        )
        if not positive_terms and not has_structured_constraints:
            return SearchResponse(
                [],
                0,
                offset,
                limit,
                0.0,
                {},
                self._query_explanation(parsed, {}, latitude, longitude, radius_km, ranking, relevance_profile),
                relevance_profile,
                did_you_mean,
                related_queries,
            )

        expanded_terms, spelling_corrections = self._expand_terms(positive_terms, typo_tolerance=typo_tolerance)
        did_you_mean = self._did_you_mean(query, spelling_corrections)
        positive_groups = self._positive_groups(parsed, expanded_terms)
        query_explanation = self._query_explanation(parsed, spelling_corrections, latitude, longitude, radius_km, ranking, relevance_profile)
        if not positive_groups:
            return SearchResponse([], 0, offset, limit, 0.0, expanded_terms, query_explanation, relevance_profile, did_you_mean, related_queries)

        candidate_rows = self._score_candidates(
            positive_groups,
            limit=limit,
            offset=offset,
            candidate_multiplier=candidate_multiplier,
            min_candidates=min_candidates,
        )
        if not candidate_rows:
            took_ms = (time.perf_counter() - started_at) * 1000
            return SearchResponse([], 0, offset, limit, round(took_ms, 3), expanded_terms, query_explanation, relevance_profile, did_you_mean, related_queries)

        document_ids = [self._decode(document_id) for document_id, _ in candidate_rows]
        documents = self._fetch_documents(document_ids)

        matched: list[SearchResult] = []
        for raw_document_id, base_score in candidate_rows:
            document_id = self._decode(raw_document_id)
            document = documents.get(document_id)
            if document is None:
                continue

            geo_distance_km = self._geo_distance_km(document, latitude, longitude)
            if not self._matches(document, parsed, expanded_terms, geo_distance_km=geo_distance_km, radius_km=radius_km):
                continue

            tfidf_score = float(base_score) * ranking["tfidf_weight"]
            phrase_score = 0.0
            phrase_groups = parsed.optional_phrases + parsed.required_phrases
            if phrase_groups and any(contains_phrase(document["_tokens"], phrase) for phrase in phrase_groups):
                phrase_score = ranking["phrase_boost"]
            proximity_score = 0.0
            proximity_groups = parsed.optional_proximity + parsed.required_proximity
            if proximity_groups and any(
                contains_proximity(document["_tokens"], terms, distance) for terms, distance in proximity_groups
            ):
                proximity_score = ranking["proximity_boost"]

            recency_score = self._recency_boost(document) * ranking["recency_weight"]
            popularity_score = float(document.get("popularity", 0.0)) * ranking["popularity_weight"]

            matched_terms = sorted(
                term for term in set(parsed.required_terms + parsed.optional_terms) if any(candidate in document.get("_terms", []) for candidate in expanded_terms.get(term, [term]))
            )
            exact_title_score = self._exact_title_boost(document, parsed, ranking)
            all_terms_score = self._all_terms_present_boost(document, expanded_terms, parsed, ranking)
            geo_score = self._geo_decay_boost(geo_distance_km, radius_km, ranking)
            backlink_score = self._backlink_boost(document, ranking)
            load_time_score = self._load_time_boost(document, ranking)
            score = (
                tfidf_score
                + phrase_score
                + proximity_score
                + recency_score
                + popularity_score
                + exact_title_score
                + all_terms_score
                + geo_score
                + backlink_score
                + load_time_score
            )
            explanation = {
                "tfidf_score": round(tfidf_score, 6),
                "phrase_boost": round(phrase_score, 6),
                "proximity_boost": round(proximity_score, 6),
                "recency_boost": round(recency_score, 6),
                "popularity_boost": round(popularity_score, 6),
                "exact_title_boost": round(exact_title_score, 6),
                "all_terms_present_boost": round(all_terms_score, 6),
                "geo_decay_boost": round(geo_score, 6),
                "backlink_boost": round(backlink_score, 6),
                "load_time_boost": round(load_time_score, 6),
                "matched_terms": matched_terms,
                "matched_fields": self._matched_fields(document, expanded_terms, parsed),
                "matched_phrases": [" ".join(phrase) for phrase in phrase_groups if contains_phrase(document["_tokens"], phrase)],
                "matched_proximity": [
                    {"terms": " ".join(terms), "distance": distance}
                    for terms, distance in proximity_groups
                    if contains_proximity(document["_tokens"], terms, distance)
                ],
                "spelling_corrections": spelling_corrections,
                "alias_matches": parsed.alias_matches,
                "synonym_matches": parsed.synonym_matches,
                "geo_distance_km": round(geo_distance_km, 3) if geo_distance_km is not None else None,
                "relevance_profile": relevance_profile,
                "snippet": self._build_snippet(document, expanded_terms, parsed),
            }
            matched.append(
                SearchResult(
                    document_id=document_id,
                    score=round(score, 6),
                    document=self._public_document(document),
                    explanation=explanation,
                )
            )

        matched.sort(key=lambda item: item.score, reverse=True)
        total = len(matched)
        page = matched[offset : offset + limit]
        took_ms = (time.perf_counter() - started_at) * 1000
        return SearchResponse(page, total, offset, limit, round(took_ms, 3), expanded_terms, query_explanation, relevance_profile, did_you_mean, related_queries)

    def get_document(self, document_id: str) -> dict[str, Any] | None:
        document = self._get_raw_document(document_id)
        if document is None:
            return None
        return self._public_document(document)

    def _score_candidates(
        self,
        positive_groups: list[list[str]],
        *,
        limit: int,
        offset: int,
        candidate_multiplier: int,
        min_candidates: int,
    ) -> list[tuple[str, float]]:
        total_docs = max(self.redis.scard(self._indexed_key()), 1)
        weighted_keys: dict[str, float] = {}
        pipe = self.redis.pipeline(transaction=False)
        term_key_counts: Counter[str] = Counter()
        for group in positive_groups:
            for token in group:
                for field, field_weight in FIELD_WEIGHTS.items():
                    term_key = self._term_key(field, token)
                    if term_key not in term_key_counts:
                        pipe.zcard(term_key)
                    term_key_counts[term_key] += 1
        frequencies = pipe.execute()

        for term_key, frequency in zip(term_key_counts.keys(), frequencies):
            if frequency:
                idf = max(math.log(total_docs / frequency, 2), 0.0) or 1e-9
                field = term_key.split(":")[-2]
                weighted_keys[term_key] = idf * term_key_counts[term_key] * FIELD_WEIGHTS.get(field, 1.0)

        if not weighted_keys:
            return []

        candidate_count = max(min_candidates, max(limit + offset, 1) * max(candidate_multiplier, 1))
        temp_key = f"{self.prefix}temp:{uuid.uuid4().hex}"
        try:
            self.redis.zunionstore(temp_key, weighted_keys)
            return [
                (self._decode(document_id), float(score))
                for document_id, score in self.redis.zrevrange(temp_key, 0, candidate_count - 1, withscores=True)
            ]
        finally:
            self.redis.delete(temp_key)

    def _positive_groups(self, parsed: ParsedQuery, expanded_terms: dict[str, list[str]]) -> list[list[str]]:
        groups: list[list[str]] = []
        for term in parsed.required_terms + parsed.optional_terms:
            group = expanded_terms.get(term, [])
            if group:
                groups.append(group)
        return groups

    def _expand_terms(self, terms: list[str], *, typo_tolerance: bool) -> tuple[dict[str, list[str]], dict[str, str]]:
        lexicon_set = self._load_lexicon()
        lexicon = sorted(lexicon_set)
        expanded: dict[str, list[str]] = {}
        corrections: dict[str, str] = {}
        for term in terms:
            semantic_variants = set(expand_term_variants(term))
            present_variants = sorted(variant for variant in semantic_variants if variant in lexicon_set)
            if term in lexicon_set:
                expanded[term] = sorted(set([term, *present_variants]))
                continue
            if present_variants:
                expanded[term] = present_variants
                continue
            if typo_tolerance and lexicon:
                matches = get_close_matches(term, lexicon, n=3, cutoff=0.75)
                if matches:
                    corrections[term] = matches[0]
                    corrected_variants = set(expand_term_variants(matches[0]))
                    expanded[term] = sorted({variant for variant in corrected_variants if variant in lexicon_set} or set(matches))
                    continue
            expanded[term] = [term]
        return expanded, corrections

    def _matches(
        self,
        document: dict[str, Any],
        parsed: ParsedQuery,
        expanded_terms: dict[str, list[str]],
        *,
        geo_distance_km: float | None,
        radius_km: float | None,
    ) -> bool:
        document_terms = set(document.get("_terms", []))
        document_tokens = document.get("_tokens", [])

        for term in parsed.required_terms:
            if not any(candidate in document_terms for candidate in expanded_terms.get(term, [term])):
                return False

        for term in parsed.excluded_terms:
            if term in document_terms:
                return False

        for phrase in parsed.required_phrases:
            if not contains_phrase(document_tokens, phrase):
                return False

        for phrase in parsed.excluded_phrases:
            if contains_phrase(document_tokens, phrase):
                return False

        for terms, distance in parsed.required_proximity:
            if not contains_proximity(document_tokens, terms, distance):
                return False

        for terms, distance in parsed.excluded_proximity:
            if contains_proximity(document_tokens, terms, distance):
                return False

        if radius_km is not None:
            if geo_distance_km is None or geo_distance_km > radius_km:
                return False

        return True

    def _fetch_documents(self, document_ids: list[str]) -> dict[str, dict[str, Any]]:
        if not document_ids:
            return {}
        pipe = self.redis.pipeline(transaction=False)
        for document_id in document_ids:
            pipe.get(self._document_key(document_id))
        rows = pipe.execute()
        documents: dict[str, dict[str, Any]] = {}
        for document_id, payload in zip(document_ids, rows):
            if payload is None:
                continue
            documents[document_id] = json.loads(self._decode(payload))
        return documents

    def _fetch_all_documents(self) -> list[dict[str, Any]]:
        document_ids = [self._decode(value) for value in self.redis.smembers(self._indexed_key())]
        return list(self._fetch_documents(document_ids).values())

    def _get_raw_document(self, document_id: str) -> dict[str, Any] | None:
        payload = self.redis.get(self._document_key(document_id))
        if payload is None:
            return None
        return json.loads(self._decode(payload))

    def _recency_boost(self, document: dict[str, Any]) -> float:
        updated_at = float(document.get("updated_at", 0.0) or 0.0)
        if updated_at <= 0:
            return 0.0
        age_seconds = max(time.time() - updated_at, 0.0)
        age_days = age_seconds / 86400
        return 1 / (1 + age_days)

    def _geo_distance_km(self, document: dict[str, Any], latitude: float | None, longitude: float | None) -> float | None:
        if latitude is None or longitude is None:
            return None
        doc_lat = document.get("latitude")
        doc_lon = document.get("longitude")
        if doc_lat is None or doc_lon is None:
            return None
        return self._haversine_km(latitude, longitude, float(doc_lat), float(doc_lon))

    def _query_explanation(
        self,
        parsed: ParsedQuery,
        spelling_corrections: dict[str, str],
        latitude: float | None,
        longitude: float | None,
        radius_km: float | None,
        ranking: dict[str, float],
        relevance_profile: str,
    ) -> dict[str, Any]:
        return {
            "relevance_profile": relevance_profile,
            "ranking_factors": ranking,
            "normalized_query": parsed.normalized_query,
            "field_weights": FIELD_WEIGHTS,
            "required_terms": parsed.required_terms,
            "optional_terms": parsed.optional_terms,
            "excluded_terms": parsed.excluded_terms,
            "required_phrases": [" ".join(phrase) for phrase in parsed.required_phrases],
            "optional_phrases": [" ".join(phrase) for phrase in parsed.optional_phrases],
            "excluded_phrases": [" ".join(phrase) for phrase in parsed.excluded_phrases],
            "required_proximity": [{"terms": " ".join(terms), "distance": distance} for terms, distance in parsed.required_proximity],
            "optional_proximity": [{"terms": " ".join(terms), "distance": distance} for terms, distance in parsed.optional_proximity],
            "excluded_proximity": [{"terms": " ".join(terms), "distance": distance} for terms, distance in parsed.excluded_proximity],
            "alias_matches": parsed.alias_matches,
            "synonym_matches": parsed.synonym_matches,
            "spelling_corrections": spelling_corrections,
            "geo_filter": (
                {"latitude": latitude, "longitude": longitude, "radius_km": radius_km}
                if latitude is not None and longitude is not None and radius_km is not None
                else None
            ),
        }

    def _did_you_mean(self, raw_query: str, spelling_corrections: dict[str, str]) -> str | None:
        if not spelling_corrections:
            return None
        rewritten = [spelling_corrections.get(token.lower(), token) for token in raw_query.split()]
        candidate = " ".join(rewritten).strip()
        return candidate if candidate and candidate.lower() != raw_query.strip().lower() else None

    def _related_queries(self, parsed: ParsedQuery, *, limit: int = 5) -> list[str]:
        queries: list[str] = []
        seen: set[str] = set()
        terms = parsed.required_terms + parsed.optional_terms
        for term in terms:
            for variant in expand_term_variants(term):
                if variant == term:
                    continue
                query = " ".join([variant if item == term else item for item in terms]).strip()
                if query and query not in seen:
                    seen.add(query)
                    queries.append(query)
                    if len(queries) >= limit:
                        return queries
        return queries

    def _resolve_ranking_config(
        self,
        *,
        relevance_profile: str,
        phrase_boost: float,
        proximity_boost: float,
        recency_weight: float,
        popularity_weight: float,
    ) -> dict[str, float]:
        base = RELEVANCE_BANK.get(relevance_profile, RELEVANCE_BANK["balanced"]).copy()
        base["phrase_boost"] = phrase_boost if phrase_boost != 0.75 else base["phrase_boost"]
        base["proximity_boost"] = proximity_boost if proximity_boost != 0.45 else base["proximity_boost"]
        base["recency_weight"] = recency_weight if recency_weight != 0.25 else base["recency_weight"]
        base["popularity_weight"] = popularity_weight if popularity_weight != 0.2 else base["popularity_weight"]
        return base

    def _exact_title_boost(self, document: dict[str, Any], parsed: ParsedQuery, ranking: dict[str, float]) -> float:
        title = (document.get("title") or "").lower()
        normalized_title = parse_query(title).normalized_query.replace('"', "")
        query_terms = parsed.required_terms + parsed.optional_terms
        if not normalized_title or not query_terms:
            return 0.0
        if all(term in normalized_title.split() for term in query_terms):
            return ranking["exact_title_boost"]
        return 0.0

    def _all_terms_present_boost(
        self,
        document: dict[str, Any],
        expanded_terms: dict[str, list[str]],
        parsed: ParsedQuery,
        ranking: dict[str, float],
    ) -> float:
        query_terms = parsed.required_terms + parsed.optional_terms
        if not query_terms:
            return 0.0
        document_terms = set(document.get("_terms", []))
        if all(any(candidate in document_terms for candidate in expanded_terms.get(term, [term])) for term in query_terms):
            return ranking["all_terms_present_boost"]
        return 0.0

    def _matched_fields(
        self,
        document: dict[str, Any],
        expanded_terms: dict[str, list[str]],
        parsed: ParsedQuery,
    ) -> list[str]:
        matched: list[str] = []
        field_terms = document.get("_field_terms", {})
        for field, terms in field_terms.items():
            term_set = set(terms)
            if any(any(candidate in term_set for candidate in expanded_terms.get(term, [term])) for term in parsed.required_terms + parsed.optional_terms):
                matched.append(field)
        return matched

    def _build_snippet(
        self,
        document: dict[str, Any],
        expanded_terms: dict[str, list[str]],
        parsed: ParsedQuery,
        *,
        max_length: int = 180,
    ) -> str:
        source = document.get("description") or document.get("content") or document.get("first_few_words") or ""
        if not source:
            return "No snippet available."
        lowered = source.lower()
        for term in parsed.required_terms + parsed.optional_terms:
            for candidate in expanded_terms.get(term, [term]):
                idx = lowered.find(candidate.lower())
                if idx != -1:
                    start = max(0, idx - 50)
                    end = min(len(source), idx + max_length - 50)
                    snippet = source[start:end].strip()
                    if start > 0:
                        snippet = "..." + snippet
                    if end < len(source):
                        snippet = snippet + "..."
                    return snippet
        return source[:max_length].strip() + ("..." if len(source) > max_length else "")

    def _geo_decay_boost(self, geo_distance_km: float | None, radius_km: float | None, ranking: dict[str, float]) -> float:
        if geo_distance_km is None:
            return 0.0
        if radius_km is not None and radius_km > 0:
            distance_ratio = min(geo_distance_km / radius_km, 1.0)
            return (1 - distance_ratio) * ranking["geo_decay_weight"]
        return (1 / (1 + geo_distance_km)) * ranking["geo_decay_weight"]

    def _backlink_boost(self, document: dict[str, Any], ranking: dict[str, float]) -> float:
        backlinks = max(float(document.get("back_links", 0) or 0), 0.0)
        return math.log1p(backlinks) * ranking["backlink_weight"]

    def _load_time_boost(self, document: dict[str, Any], ranking: dict[str, float]) -> float:
        load_time = document.get("load_time")
        if load_time is None:
            return 0.0
        return (1 / (1 + max(float(load_time), 0.0))) * ranking["load_time_weight"]

    def _public_document(self, document: dict[str, Any]) -> dict[str, Any]:
        return {key: value for key, value in document.items() if not key.startswith("_")}

    def _build_field_indices(
        self,
        *,
        title: str | None,
        content: str,
        description: str | None,
        first_few_words: str | None,
        back_link_keywords: list[str],
        url_keywords: list[str],
        main_keywords: list[str],
        headings: list[str],
        aliases: list[str],
        synonyms: list[str],
    ) -> dict[str, dict[str, Any]]:
        field_sources = {
            "title": title or "",
            "content": content,
            "description": description or "",
            "first_words": first_few_words or "",
            "backlink_keywords": " ".join(back_link_keywords),
            "url_keywords": " ".join(url_keywords),
            "main_keywords": " ".join(main_keywords),
            "headings": " ".join(headings),
            "aliases": " ".join(aliases),
            "synonyms": " ".join(synonyms),
        }
        field_payloads: dict[str, dict[str, Any]] = {}
        for field, value in field_sources.items():
            tf, tokens, analysis = term_frequencies(value)
            field_payloads[field] = {
                "tf": tf,
                "tokens": tokens,
                "analysis": analysis,
            }
        return field_payloads

    def _load_lexicon(self) -> set[str]:
        if self._lexicon_cache is None:
            self._lexicon_cache = {self._decode(token) for token in self.redis.smembers(self._lexicon_key())}
        return self._lexicon_cache

    def _invalidate_lexicon_cache(self) -> None:
        self._lexicon_cache = None

    @staticmethod
    def _document_signature(
        content: str,
        *,
        title: str | None,
        url: str | None,
        popularity: float,
        updated_at: float,
        metadata: dict[str, Any],
    ) -> str:
        normalized = json.dumps(
            {
                "content": content,
                "title": title,
                "url": url,
                "popularity": popularity,
                "updated_at": updated_at,
                "metadata": metadata,
            },
            sort_keys=True,
            separators=(",", ":"),
        )
        return sha1(normalized.encode("utf-8")).hexdigest()

    @staticmethod
    def _haversine_km(lat1: float, lon1: float, lat2: float, lon2: float) -> float:
        radius = 6371.0
        phi1 = math.radians(lat1)
        phi2 = math.radians(lat2)
        delta_phi = math.radians(lat2 - lat1)
        delta_lambda = math.radians(lon2 - lon1)
        a = (
            math.sin(delta_phi / 2) ** 2
            + math.cos(phi1) * math.cos(phi2) * math.sin(delta_lambda / 2) ** 2
        )
        c = 2 * math.atan2(math.sqrt(a), math.sqrt(1 - a))
        return radius * c

    @staticmethod
    def _decode(value: Any) -> Any:
        if isinstance(value, bytes):
            return value.decode("utf-8")
        return value
