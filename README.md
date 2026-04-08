# RV Search Engine

RV Search Engine is a Redis-backed search engine project with a FastAPI backend and a minimal Electron desktop UI.

It is designed to explore how a modern search engine can be built from core search concepts such as:

- field-aware indexing
- TF-IDF scoring
- query normalization
- stemming and lemmatization
- stop-word removal
- aliases and synonyms
- spelling correction
- phrase and proximity search
- geo-aware filtering
- ranking profiles and boost factors
- image indexing and image search

The project is intentionally practical: it gives you a working search engine you can run locally, inspect in code, and extend.

## Purpose

The purpose of RV Search Engine is to provide:

- a hands-on search engine implementation built on Redis
- a backend that demonstrates real search-engine ideas clearly
- a local desktop UI to query and inspect results quickly
- a foundation for ranking experiments, relevance tuning, and later training

This project is useful if you want to learn:

- how inverted indexes work
- how text normalization affects search quality
- how ranking factors combine into a final score
- how metadata such as backlinks, load time, popularity, and geo distance can influence ranking
- how to structure a search backend so it remains extensible

## What The Project Does

RV Search Engine supports:

- indexing website-like documents with metadata
- indexing images separately
- running web search queries against indexed pages
- running image search queries against indexed images
- auto-loading demo data for local exploration
- returning result explanations and ranking details
- optionally adding external Google results when configured

## Main Features

### Core Search Features

- TF-IDF ranking using Redis sorted sets
- field-aware indexing for title, content, description, headings, keywords, aliases, and synonyms
- query normalization
- stop-word removal
- lightweight stemming
- lightweight lemmatization
- alias normalization such as `js -> javascript`
- synonym expansion such as `automobile -> car`
- spelling correction such as `redsi -> redis`
- phrase search such as `"redis search"`
- proximity search such as `+"redis engine"~2`
- required and excluded query terms such as `+redis -python`

### Ranking Features

- field weighting
- popularity boost
- recency boost
- exact-title boost
- all-query-terms-present boost
- geo distance decay boost
- backlink boost
- load-time boost
- named relevance profiles:
  - `balanced`
  - `precision`
  - `fresh`
  - `trending`
  - `local`

### Metadata / Structured Search Features

- description indexing
- first-few-words indexing
- headings indexing
- main keyword indexing
- URL keyword indexing
- backlink keyword indexing
- geo filtering with `latitude`, `longitude`, and `radius_km`
- image indexing with image URL, site title, site URL, and alt tag

### UX Features

- minimal Electron desktop UI
- query suggestions
- “did you mean” support
- related query suggestions
- result snippets
- result explanation objects
- auto-bootstrap of demo search data

## Project Structure

```text
redis-search-engine/
├── README.md
├── pyproject.toml
├── package.json
├── package-lock.json
├── .gitignore
├── electron/
│   ├── main.js
│   └── preload.js
├── ui/
│   ├── index.html
│   ├── styles.css
│   └── renderer.js
├── redis_search_engine/
│   ├── __init__.py
│   ├── main.py
│   ├── engine.py
│   ├── text.py
│   └── external_search.py
└── tests/
    ├── test_api.py
    ├── test_engine.py
    └── test_text.py
```

## Backend Overview

### `redis_search_engine/main.py`

This is the FastAPI entrypoint.

It:

- creates the application
- connects Redis
- instantiates the search engine
- exposes the API routes
- defines request and response models
- provides demo bootstrap endpoints

### `redis_search_engine/engine.py`

This is the main search engine implementation.

It handles:

- indexing documents
- indexing images
- document removal
- web search
- image search
- Redis key layout
- field-aware ranking
- relevance profile selection
- result explanations
- query suggestion support

### `redis_search_engine/text.py`

This is the text processing pipeline.

It handles:

- tokenization
- normalization
- stop-word filtering
- stemming
- lemmatization
- alias resolution
- synonym expansion
- query parsing
- phrase and proximity parsing

### `redis_search_engine/external_search.py`

This module provides external search provider support.

Currently it contains:

- Google Programmable Search integration
- a Yahoo placeholder/error adapter

## UI Overview

### `electron/main.js`

This starts the desktop application and launches the Python backend process automatically.

### `electron/preload.js`

This safely exposes a minimal bridge between the Electron shell and the frontend.

### `ui/index.html`

This is the minimal desktop interface layout.

### `ui/styles.css`

This contains the styling for the Electron UI.

### `ui/renderer.js`

This controls:

- search requests
- suggestions
- rendering results
- demo data bootstrapping
- backend readiness behavior

## Installation

### Requirements

- macOS, Linux, or Windows with Python 3.11+
- Node.js 18+ recommended
- npm
- Redis server available locally

### 1. Go To The Project

```bash
cd "/Users/ganeshrayapati/Documents/New project/redis-search-engine"
```

### 2. Create And Activate A Virtual Environment

```bash
python3 -m venv .venv
. .venv/bin/activate
```

### 3. Install Python Dependencies

```bash
pip install -e ".[dev]"
```

### 4. Install Node / Electron Dependencies

```bash
npm install
```

### 5. Start Redis

If Redis is installed with Homebrew:

```bash
brew services start redis
```

Or run it manually:

```bash
/opt/homebrew/opt/redis/bin/redis-server /opt/homebrew/etc/redis.conf
```

## Running The Project

### Run The Desktop App

```bash
npm start
```

This launches:

- the Electron UI
- the Python FastAPI backend automatically

### Run Only The Backend

```bash
. .venv/bin/activate
uvicorn redis_search_engine.main:app --reload
```

Backend URL:

- `http://127.0.0.1:8000`

## Demo Data

The app can bootstrap demo data automatically.

You can also call it directly:

```bash
curl -X POST http://127.0.0.1:8000/bootstrap-demo
```

Check stats:

```bash
curl http://127.0.0.1:8000/stats
```

## API Documentation

### Health

```http
GET /health
```

Returns backend health.

### Stats

```http
GET /stats
```

Returns:

- number of indexed documents
- number of indexed images

### Bootstrap Demo

```http
POST /bootstrap-demo
```

Loads demo website and image data.

### Index A Document

```http
POST /documents
```

Example payload:

```json
{
  "id": "doc-1",
  "title": "Redis Search",
  "content": "Redis can power a search engine",
  "description": "Overview of Redis search",
  "url": "https://example.com/doc-1",
  "popularity": 5,
  "updated_at": 1710000000,
  "latitude": 12.9716,
  "longitude": 77.5946,
  "aliases": ["rv"],
  "synonyms": ["search engine"],
  "first_few_words": "Redis can power a search engine",
  "load_time": 0.4,
  "back_links": 10,
  "back_link_keywords": ["redis"],
  "url_keywords": ["redis-search"],
  "main_keywords": ["redis", "search"],
  "headings": ["Redis Search"],
  "favicon": "https://example.com/favicon.ico",
  "og_image": "https://example.com/og.png"
}
```

### Index An Image

```http
POST /images
```

Example payload:

```json
{
  "id": "img-1",
  "image_url": "https://example.com/logo.png",
  "site_title": "Redis Search",
  "site_url": "https://example.com",
  "alt_tag": "redis engine logo"
}
```

### Web Search

```http
GET /search?q=redis
```

Supported query params include:

- `q`
- `offset`
- `limit`
- `typo_tolerance`
- `relevance_profile`
- `phrase_boost`
- `proximity_boost`
- `recency_weight`
- `popularity_weight`
- `latitude`
- `longitude`
- `radius_km`
- `include_external`
- `external_provider`
- `external_limit`

### Image Search

```http
GET /image-search?q=logo
```

### Suggestions

```http
GET /suggest?q=red
```

## Query Syntax

### Standard Terms

```text
redis search
```

### Required Terms

```text
+redis search
```

### Excluded Terms

```text
redis -python
```

### Phrase Search

```text
"redis search"
```

### Required Phrase

```text
+"redis search"
```

### Proximity Search

```text
+"redis engine"~2
```

This means the terms must appear within a window of 2 positions.

## Relevance Profiles

RV Search Engine supports these built-in relevance banks:

- `balanced`
  - general-purpose profile
- `precision`
  - stricter phrase/title emphasis
- `fresh`
  - favors recent documents
- `trending`
  - favors popularity more heavily
- `local`
  - favors geo closeness

## Tools, Libraries, And Technologies Used

### Backend

- Python
- FastAPI
- Pydantic
- Redis
- `redis-py`
- `fakeredis` for tests
- `pytest`

### Desktop / Frontend

- Electron
- HTML
- CSS
- vanilla JavaScript

### External Search

- Google Programmable Search API

## Important Internal Functions

### In `engine.py`

- `add_document()`
  - indexes a document into Redis
- `add_documents()`
  - batch indexing
- `add_image()`
  - indexes an image
- `search()`
  - main web search function
- `search_images()`
  - image search function
- `suggest_queries()`
  - search suggestion generation
- `_score_candidates()`
  - candidate scoring using Redis sorted sets
- `_expand_terms()`
  - synonym/spelling expansion
- `_matches()`
  - post-filter matching logic
- `_query_explanation()`
  - builds search explanation payloads

### In `text.py`

- `tokenize()`
- `analyze_text()`
- `term_frequencies()`
- `parse_query()`
- `contains_phrase()`
- `contains_proximity()`
- `normalize_token()`
- `lemmatize_token()`
- `stem_token()`

## Result Explanation

Search results can include explanation details such as:

- TF-IDF contribution
- phrase boost
- proximity boost
- recency boost
- popularity boost
- exact title boost
- all-terms-present boost
- geo decay boost
- backlink boost
- load-time boost
- matched terms
- matched phrases
- matched proximity groups
- matched fields
- snippet

## External Search Support

Google search is supported if you provide:

- `GOOGLE_SEARCH_API_KEY`
- `GOOGLE_SEARCH_CX`

Yahoo backend web search is not currently enabled as a working REST provider in this project.

## Testing

Run all tests:

```bash
. .venv/bin/activate
python -m pytest
```

## Current Status

This project currently includes:

- working Redis-based backend
- working Electron desktop UI
- demo data bootstrap
- web and image indexing
- relevance profiles
- search suggestions
- “did you mean” support
- structured metadata ranking

## Next Possible Improvements

- crawler / URL ingestion pipeline
- configurable synonym dictionaries from JSON/YAML
- learned ranking weights
- result caching
- pagination in UI
- external provider blending
- snippet highlighting
- packaged app build
