from __future__ import annotations

import json
import os
from dataclasses import dataclass
from typing import Any
from urllib.error import HTTPError, URLError
from urllib.parse import urlencode
from urllib.request import urlopen


class ExternalSearchError(Exception):
    """Raised when an external provider cannot be queried."""


@dataclass(slots=True)
class ExternalSearchResult:
    provider: str
    title: str
    url: str
    snippet: str
    metadata: dict[str, Any]


class GoogleProgrammableSearchClient:
    def __init__(self, api_key: str | None = None, cx: str | None = None) -> None:
        self.api_key = api_key or os.getenv("GOOGLE_SEARCH_API_KEY")
        self.cx = cx or os.getenv("GOOGLE_SEARCH_CX")

    def is_configured(self) -> bool:
        return bool(self.api_key and self.cx)

    def search(self, query: str, *, limit: int = 5) -> list[ExternalSearchResult]:
        if not self.is_configured():
            raise ExternalSearchError("Google search is not configured. Set GOOGLE_SEARCH_API_KEY and GOOGLE_SEARCH_CX.")

        params = urlencode(
            {
                "key": self.api_key,
                "cx": self.cx,
                "q": query,
                "num": max(1, min(limit, 10)),
            }
        )
        endpoint = f"https://customsearch.googleapis.com/customsearch/v1?{params}"
        try:
            with urlopen(endpoint, timeout=10) as response:
                payload = json.loads(response.read().decode("utf-8"))
        except HTTPError as exc:
            raise ExternalSearchError(f"Google search failed with HTTP {exc.code}.") from exc
        except URLError as exc:
            raise ExternalSearchError(f"Google search network error: {exc.reason}.") from exc

        items = payload.get("items", [])
        return [
            ExternalSearchResult(
                provider="google",
                title=item.get("title", ""),
                url=item.get("link", ""),
                snippet=item.get("snippet", ""),
                metadata={
                    "display_link": item.get("displayLink"),
                    "formatted_url": item.get("formattedUrl"),
                },
            )
            for item in items
        ]


class YahooSearchClient:
    def search(self, query: str, *, limit: int = 5) -> list[ExternalSearchResult]:
        raise ExternalSearchError(
            "Yahoo backend search is not available here. The currently documented Yahoo Search offering is a mobile Search SDK, not a simple server-side web search REST API for this app."
        )
