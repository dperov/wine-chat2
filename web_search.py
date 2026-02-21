from __future__ import annotations

import os
import re
from typing import Any

from dotenv import find_dotenv, load_dotenv

try:
    from openai import OpenAI
except Exception:
    OpenAI = None

load_dotenv(find_dotenv())

WINE_MARKERS = (
    "вино",
    "wine",
    "vino",
    "vin",
    "цена",
    "price",
    "купить",
    "магазин",
    "catalog",
    "product",
    "shop",
    "wine.rbc.ru",
    "russianvine",
    "vinoteki",
    "inwine",
    "winestyle",
    "simplewine",
)
NON_WINE_MARKERS = (
    "wikipedia.org",
    "wiktionary",
    "merriam-webster",
    "dictionary",
    "vocabulary",
    "musicca",
    "britannica",
    "wordreference",
)


def _safe_text(value: Any) -> str:
    return " ".join(str(value or "").split()).strip()


def _to_dict(value: Any) -> Any:
    if isinstance(value, (dict, list, str, int, float, bool)) or value is None:
        return value
    if hasattr(value, "model_dump"):
        try:
            return value.model_dump()
        except Exception:
            pass
    if hasattr(value, "to_dict"):
        try:
            return value.to_dict()
        except Exception:
            pass
    data = getattr(value, "__dict__", None)
    if isinstance(data, dict):
        return {k: _to_dict(v) for k, v in data.items()}
    return str(value)


def _extract_message_text(resp_obj: Any, resp_dict: dict[str, Any]) -> str:
    text = _safe_text(getattr(resp_obj, "output_text", ""))
    if text:
        return text

    output = resp_dict.get("output") or []
    for item in output:
        if not isinstance(item, dict):
            continue
        if item.get("type") != "message":
            continue
        parts = item.get("content") or []
        chunk_texts: list[str] = []
        for part in parts:
            if not isinstance(part, dict):
                continue
            ptype = part.get("type")
            if ptype in {"output_text", "text"}:
                chunk_texts.append(_safe_text(part.get("text")))
        text = _safe_text(" ".join(chunk_texts))
        if text:
            return text
    return ""


def _extract_search_query(resp_dict: dict[str, Any], fallback: str) -> str:
    output = resp_dict.get("output") or []
    for item in output:
        if not isinstance(item, dict):
            continue
        if item.get("type") != "web_search_call":
            continue
        action = item.get("action") or {}
        if isinstance(action, dict):
            query = _safe_text(action.get("query"))
            if query:
                return query
            queries = action.get("queries") or []
            if isinstance(queries, list):
                for q in queries:
                    query = _safe_text(q)
                    if query:
                        return query
    return fallback


def _parse_source_item(src: dict[str, Any]) -> dict[str, str] | None:
    url = _safe_text(src.get("url") or src.get("link"))
    if not url:
        return None
    title = _safe_text(src.get("title") or src.get("name") or "Источник")
    snippet = _safe_text(
        src.get("snippet")
        or src.get("text")
        or src.get("description")
        or ""
    )
    return {"title": title, "url": url, "snippet": snippet}


def _extract_sources(resp_dict: dict[str, Any]) -> list[dict[str, str]]:
    collected: list[dict[str, str]] = []
    seen: set[str] = set()

    def add(item: dict[str, Any]) -> None:
        parsed = _parse_source_item(item)
        if not parsed:
            return
        url = parsed["url"]
        if url in seen:
            return
        seen.add(url)
        collected.append(parsed)

    output = resp_dict.get("output") or []

    # Preferred source: included web_search_call.action.sources.
    for item in output:
        if not isinstance(item, dict):
            continue
        if item.get("type") != "web_search_call":
            continue
        action = item.get("action") or {}
        if not isinstance(action, dict):
            continue
        sources = action.get("sources") or []
        if isinstance(sources, list):
            for src in sources:
                if isinstance(src, dict):
                    add(src)

    # Fallback source: URL citations inside assistant message annotations.
    for item in output:
        if not isinstance(item, dict):
            continue
        if item.get("type") != "message":
            continue
        content = item.get("content") or []
        for part in content:
            if not isinstance(part, dict):
                continue
            annotations = part.get("annotations") or []
            if not isinstance(annotations, list):
                continue
            for ann in annotations:
                if not isinstance(ann, dict):
                    continue
                add(ann)

    # Last resort: scan any top-level URL-like values.
    if not collected:
        def walk(node: Any) -> None:
            if isinstance(node, dict):
                if "url" in node and isinstance(node.get("url"), str):
                    add(node)
                for value in node.values():
                    walk(value)
            elif isinstance(node, list):
                for value in node:
                    walk(value)

        walk(resp_dict)

    return collected


def _extract_links_from_text(text: str) -> list[dict[str, str]]:
    links = re.findall(r"https?://[^\s\])>]+", text or "")
    out: list[dict[str, str]] = []
    seen: set[str] = set()
    for url in links:
        clean = _safe_text(url.rstrip(".,;!?:"))
        if not clean or clean in seen:
            continue
        seen.add(clean)
        out.append({"title": "Источник", "url": clean, "snippet": ""})
    return out


def _normalize_query_for_wine(query: str) -> str:
    q = _safe_text(query)
    if not q:
        return q
    low = q.lower()
    extras: list[str] = []

    if not any(x in low for x in ("вино", "wine", "vino", "вин")):
        extras.append("вино")
    if not any(x in low for x in ("цена", "стоит", "price", "купить", "налич")):
        extras.append("цена купить")
    if "козак" in low and "cosaque" not in low:
        extras.append("cosaque")
    if "cosaque" in low and "козак" not in low:
        extras.append("козак")
    if "магнум" in low and "1.5" not in low and "1,5" not in low:
        extras.append("1.5 л")

    if extras:
        return f"{q} {' '.join(extras)}"
    return q


def _tokenize(value: str) -> list[str]:
    return [t for t in re.findall(r"[0-9a-zа-яё]+", (value or "").lower()) if len(t) >= 3]


def _source_score(query: str, item: dict[str, str]) -> int:
    hay = " ".join(
        [
            _safe_text(item.get("title")),
            _safe_text(item.get("snippet")),
            _safe_text(item.get("url")),
        ]
    ).lower()
    score = 0
    for marker in WINE_MARKERS:
        if marker in hay:
            score += 3
    for marker in NON_WINE_MARKERS:
        if marker in hay:
            score -= 8
    for tok in _tokenize(query):
        if tok in hay:
            score += 1
    return score


def _rank_sources(query: str, items: list[dict[str, str]], limit: int) -> list[dict[str, str]]:
    if not items:
        return []
    scored = sorted(
        [(_source_score(query, item), item) for item in items],
        key=lambda t: t[0],
        reverse=True,
    )
    filtered = [item for score, item in scored if score >= 2][:limit]
    if filtered:
        return filtered
    return [item for score, item in scored if score > -8][:limit]


def search_wine_web(query: str, max_results: int = 5) -> dict[str, Any]:
    q = _safe_text(query)
    if not q:
        return {"ok": False, "error": "Пустой поисковый запрос."}

    if not OpenAI:
        return {
            "ok": False,
            "error": "Web-поиск недоступен: пакет `openai` не установлен.",
            "query": q,
            "search_query": q,
            "engine": "openai_web_search",
        }

    api_key = _safe_text(os.getenv("OPENAI_API_KEY"))
    if not api_key:
        return {
            "ok": False,
            "error": "Web-поиск недоступен: не задан OPENAI_API_KEY.",
            "query": q,
            "search_query": q,
            "engine": "openai_web_search",
        }

    max_results = max(1, min(int(max_results or 5), 10))
    web_model = _safe_text(os.getenv("OPENAI_WEB_MODEL") or os.getenv("OPENAI_MODEL") or "gpt-4.1")
    context_size = _safe_text(os.getenv("WEB_SEARCH_CONTEXT_SIZE") or "medium").lower()
    if context_size not in {"low", "medium", "high"}:
        context_size = "medium"

    country = _safe_text(os.getenv("WEB_SEARCH_COUNTRY") or "RU").upper()
    city = _safe_text(os.getenv("WEB_SEARCH_CITY") or "moscow")

    web_tool: dict[str, Any] = {
        "type": "web_search",
        "search_context_size": context_size,
    }
    if country:
        user_location: dict[str, Any] = {
            "type": "approximate",
            "country": country,
        }
        if city:
            user_location["city"] = city
        web_tool["user_location"] = user_location

    client = OpenAI(api_key=api_key)
    allowed_domains_raw = _safe_text(os.getenv("WEB_SEARCH_ALLOWED_DOMAINS"))
    filters: dict[str, Any] = {}
    if allowed_domains_raw:
        domains = [
            _safe_text(item)
            for item in allowed_domains_raw.split(",")
            if _safe_text(item)
        ]
        if domains:
            filters["allowed_domains"] = domains
    if filters:
        web_tool["filters"] = filters

    normalized_query = _normalize_query_for_wine(q)

    try:
        response = client.responses.create(
            model=web_model,
            input=[
                {
                    "role": "developer",
                    "content": [
                        {
                            "type": "input_text",
                            "text": (
                                "Ты выполняешь web-поиск только по теме вина. "
                                "Игнорируй словари, энциклопедии, переводчики и нерелевантные страницы. "
                                "Приоритет: цены, наличие в магазинах, карточки вина, винные каталоги."
                            ),
                        }
                    ],
                },
                {
                    "role": "user",
                    "content": [
                        {"type": "input_text", "text": normalized_query},
                    ],
                }
            ],
            tools=[web_tool],
            tool_choice="required",
            max_tool_calls=1,
            include=["web_search_call.action.sources"],
            temperature=0,
            max_output_tokens=1200,
        )
    except Exception as exc:
        return {
            "ok": False,
            "error": f"Ошибка OpenAI web_search: {exc}",
            "query": q,
            "search_query": q,
            "engine": "openai_web_search",
        }

    resp_dict_any = _to_dict(response)
    resp_dict = resp_dict_any if isinstance(resp_dict_any, dict) else {}
    search_query = _extract_search_query(resp_dict, q)
    answer_text = _extract_message_text(response, resp_dict)
    sources = _extract_sources(resp_dict)

    if not sources and answer_text:
        sources = _extract_links_from_text(answer_text)

    if not sources:
        return {
            "ok": False,
            "error": "OpenAI web_search не вернул источников. Уточните запрос.",
            "query": q,
            "search_query": search_query,
            "engine": "openai_web_search",
            "answer_text": answer_text,
        }

    results = _rank_sources(normalized_query, sources, max_results)
    if not results:
        return {
            "ok": False,
            "error": "Web-поиск вернул только нерелевантные источники. Уточните название вина/винтаж/объем.",
            "query": q,
            "search_query": search_query,
            "engine": "openai_web_search",
            "answer_text": answer_text,
        }

    return {
        "ok": True,
        "query": q,
        "search_query": search_query,
        "engine": "openai_web_search",
        "model": web_model,
        "results": results,
        "count": len(results),
        "answer_text": answer_text,
    }
