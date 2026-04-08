from __future__ import annotations

import re
from collections import Counter
from dataclasses import dataclass

NON_WORDS = re.compile(r"[^a-z0-9' ]")
QUERY_PARTS = re.compile(r'[+-]?"[^"]+"(?:~\d+)?|\S+')
STOP_WORDS = {
    "a",
    "able",
    "about",
    "across",
    "after",
    "all",
    "almost",
    "also",
    "am",
    "among",
    "an",
    "and",
    "any",
    "are",
    "as",
    "at",
    "be",
    "because",
    "been",
    "but",
    "by",
    "can",
    "cannot",
    "could",
    "dear",
    "did",
    "do",
    "does",
    "either",
    "else",
    "ever",
    "every",
    "for",
    "from",
    "get",
    "got",
    "had",
    "has",
    "have",
    "he",
    "her",
    "hers",
    "him",
    "his",
    "how",
    "however",
    "i",
    "if",
    "in",
    "into",
    "is",
    "it",
    "its",
    "just",
    "least",
    "let",
    "like",
    "likely",
    "may",
    "me",
    "might",
    "most",
    "must",
    "my",
    "neither",
    "no",
    "nor",
    "not",
    "of",
    "off",
    "often",
    "on",
    "only",
    "or",
    "other",
    "our",
    "own",
    "rather",
    "said",
    "say",
    "says",
    "she",
    "should",
    "since",
    "so",
    "some",
    "than",
    "that",
    "the",
    "their",
    "them",
    "then",
    "there",
    "these",
    "they",
    "this",
    "tis",
    "to",
    "too",
    "twas",
    "us",
    "wants",
    "was",
    "we",
    "were",
    "what",
    "when",
    "where",
    "which",
    "while",
    "who",
    "whom",
    "why",
    "will",
    "with",
    "would",
    "yet",
    "you",
    "your",
}

LEMMA_EXCEPTIONS = {
    "children": "child",
    "mice": "mouse",
    "geese": "goose",
    "feet": "foot",
    "better": "good",
    "best": "good",
    "worse": "bad",
    "ran": "run",
    "running": "run",
    "ate": "eat",
    "cars": "car",
    "went": "go",
}

TERM_EQUIVALENTS = {
    "artificial intelligence": {"ai", "ml"},
    "new york city": {"nyc", "big apple"},
    "san francisco": {"sf", "bay area"},
    "united states": {"usa", "us", "america"},
    "javascript": {"js", "nodejs"},
    "python": {"py"},
    "database": {"db", "datastore"},
    "search": {"lookup", "retrieval", "find"},
    "car": {"automobile", "vehicle", "cars"},
    "hotel": {"stay", "lodging"},
}

CANONICAL_TERM_MAP: dict[str, str] = {}
RELATED_TERM_MAP: dict[str, list[str]] = {}
for canonical, variants in TERM_EQUIVALENTS.items():
    group = {canonical, *variants}
    normalized_group = set()
    for phrase in group:
        normalized_group.add(phrase)
        CANONICAL_TERM_MAP[phrase] = canonical
    related = sorted(normalized_group)
    for phrase in normalized_group:
        RELATED_TERM_MAP[phrase] = [item for item in related if item != phrase]


@dataclass(slots=True)
class ParsedQuery:
    optional_terms: list[str]
    required_terms: list[str]
    excluded_terms: list[str]
    optional_phrases: list[list[str]]
    required_phrases: list[list[str]]
    excluded_phrases: list[list[str]]
    optional_proximity: list[tuple[list[str], int]]
    required_proximity: list[tuple[list[str], int]]
    excluded_proximity: list[tuple[list[str], int]]
    normalized_query: str
    alias_matches: dict[str, str]
    synonym_matches: dict[str, list[str]]


@dataclass(slots=True)
class TokenAnalysis:
    normalized: list[str]
    original_to_normalized: dict[str, str]
    alias_matches: dict[str, str]
    synonym_matches: dict[str, list[str]]


def normalize_token(token: str) -> str | None:
    if not token or len(token) <= 1 or token in STOP_WORDS:
        return None
    token = lemmatize_token(token)
    token = stem_token(token)
    return CANONICAL_TERM_MAP.get(token, token)


def lemmatize_token(token: str) -> str:
    if token in LEMMA_EXCEPTIONS:
        return LEMMA_EXCEPTIONS[token]
    if token.endswith("ies") and len(token) > 4:
        return token[:-3] + "y"
    if token.endswith("ves") and len(token) > 4:
        return token[:-3] + "f"
    if token.endswith("men") and len(token) > 4:
        return token[:-3] + "man"
    if token.endswith("s") and len(token) > 4 and not token.endswith(("ss", "is", "us")):
        return token[:-1]
    return token


def stem_token(token: str) -> str:
    if len(token) <= 3:
        return token

    rules = (
        ("ingly", ""),
        ("edly", ""),
        ("ing", ""),
        ("ed", ""),
        ("sses", "ss"),
    )
    for suffix, replacement in rules:
        if token.endswith(suffix) and len(token) - len(suffix) + len(replacement) >= 3:
            stemmed = token[: len(token) - len(suffix)] + replacement
            if len(stemmed) >= 2 and stemmed[-1] == stemmed[-2]:
                stemmed = stemmed[:-1]
            return stemmed
    return token


def tokenize(content: str) -> list[str]:
    return analyze_text(content).normalized


def analyze_text(content: str) -> TokenAnalysis:
    words = NON_WORDS.sub(" ", content.lower()).split()
    cleaned = [word.strip("'") for word in words]

    normalized: list[str] = []
    original_to_normalized: dict[str, str] = {}
    alias_matches: dict[str, str] = {}
    synonym_matches: dict[str, list[str]] = {}

    for word in cleaned:
        if not word:
            continue
        normalized_word = normalize_token(word)
        if not normalized_word:
            continue
        normalized.append(normalized_word)
        original_to_normalized[word] = normalized_word
        if word != normalized_word:
            alias_matches[word] = normalized_word
        related = RELATED_TERM_MAP.get(normalized_word, [])
        if related:
            synonym_matches[normalized_word] = related

    return TokenAnalysis(
        normalized=normalized,
        original_to_normalized=original_to_normalized,
        alias_matches=alias_matches,
        synonym_matches=synonym_matches,
    )


def term_frequencies(content: str) -> tuple[dict[str, float], list[str], TokenAnalysis]:
    analysis = analyze_text(content)
    tokens = analysis.normalized
    if not tokens:
        return {}, [], analysis
    counts = Counter(tokens)
    total = len(tokens)
    return ({token: count / total for token, count in counts.items()}, tokens, analysis)


def expand_term_variants(term: str) -> list[str]:
    canonical = CANONICAL_TERM_MAP.get(term, term)
    variants = {canonical}
    variants.update(RELATED_TERM_MAP.get(canonical, []))
    return sorted(variants)


def parse_query(query: str) -> ParsedQuery:
    optional_terms: list[str] = []
    required_terms: list[str] = []
    excluded_terms: list[str] = []
    optional_phrases: list[list[str]] = []
    required_phrases: list[list[str]] = []
    excluded_phrases: list[list[str]] = []
    optional_proximity: list[tuple[list[str], int]] = []
    required_proximity: list[tuple[list[str], int]] = []
    excluded_proximity: list[tuple[list[str], int]] = []
    normalized_parts: list[str] = []
    alias_matches: dict[str, str] = {}
    synonym_matches: dict[str, list[str]] = {}

    for raw_part in QUERY_PARTS.findall(query):
        modifier = ""
        part = raw_part
        if raw_part[:1] in {"+", "-"}:
            modifier = raw_part[0]
            part = raw_part[1:]

        proximity_distance: int | None = None
        if part.startswith('"') and '"~' in part:
            quote_end = part.rfind('"')
            content = part[1:quote_end]
            proximity_distance = int(part[quote_end + 2 :])
        else:
            content = part[1:-1] if part.startswith('"') and part.endswith('"') else part
        analysis = analyze_text(content)
        if analysis.alias_matches:
            alias_matches.update(analysis.alias_matches)
        if analysis.synonym_matches:
            synonym_matches.update(analysis.synonym_matches)
        tokens = analysis.normalized
        if not tokens:
            continue

        if part.startswith('"'):
            suffix = f"~{proximity_distance}" if proximity_distance is not None else ""
            normalized_parts.append(f'{modifier}"{" ".join(tokens)}"{suffix}')
        else:
            normalized_parts.append(f"{modifier}{' '.join(tokens)}")

        if part.startswith('"'):
            if modifier == "+":
                target = required_terms
                if proximity_distance is None:
                    required_phrases.append(tokens)
                else:
                    required_proximity.append((tokens, proximity_distance))
            elif modifier == "-":
                target = excluded_terms
                if proximity_distance is None:
                    excluded_phrases.append(tokens)
                else:
                    excluded_proximity.append((tokens, proximity_distance))
            else:
                target = optional_terms
                if proximity_distance is None:
                    optional_phrases.append(tokens)
                else:
                    optional_proximity.append((tokens, proximity_distance))
            target.extend(tokens)
            continue

        target = optional_terms
        if modifier == "+":
            target = required_terms
        elif modifier == "-":
            target = excluded_terms
        target.extend(tokens)

    return ParsedQuery(
        optional_terms=_dedupe(optional_terms),
        required_terms=_dedupe(required_terms),
        excluded_terms=_dedupe(excluded_terms),
        optional_phrases=optional_phrases,
        required_phrases=required_phrases,
        excluded_phrases=excluded_phrases,
        optional_proximity=optional_proximity,
        required_proximity=required_proximity,
        excluded_proximity=excluded_proximity,
        normalized_query=" ".join(normalized_parts),
        alias_matches=alias_matches,
        synonym_matches=synonym_matches,
    )


def contains_phrase(tokens: list[str], phrase: list[str]) -> bool:
    if not phrase or len(phrase) > len(tokens):
        return False
    width = len(phrase)
    return any(tokens[index : index + width] == phrase for index in range(len(tokens) - width + 1))


def contains_proximity(tokens: list[str], terms: list[str], distance: int) -> bool:
    if not terms or len(terms) == 1:
        return bool(terms and terms[0] in tokens)

    positions: list[list[int]] = []
    for term in terms:
        hits = [index for index, token in enumerate(tokens) if token == term]
        if not hits:
            return False
        positions.append(hits)

    min_pos = min(position_list[0] for position_list in positions)
    max_pos = max(position_list[-1] for position_list in positions)
    if max_pos - min_pos <= distance:
        return True

    for start in positions[0]:
        current_min = start
        current_max = start
        valid = True
        for next_positions in positions[1:]:
            candidate = min(next_positions, key=lambda pos: abs(pos - current_max))
            current_min = min(current_min, candidate)
            current_max = max(current_max, candidate)
            if current_max - current_min > distance:
                valid = False
                break
        if valid:
            return True
    return False


def _dedupe(values: list[str]) -> list[str]:
    seen: set[str] = set()
    output: list[str] = []
    for value in values:
        if value not in seen:
            seen.add(value)
            output.append(value)
    return output
