import os
import threading
from datetime import datetime, timezone
from pathlib import Path
from typing import Any

_LOCK = threading.Lock()
_BASE_DIR = Path(__file__).resolve().parent
_DEFAULT_LOG_DIR = _BASE_DIR / "logs"
_DEFAULT_LOG_PATH = _DEFAULT_LOG_DIR / "wine_chat_perf.log"


def _to_bool(value: str | None, default: bool = False) -> bool:
    raw = str(value or "").strip().lower()
    if not raw:
        return default
    return raw in {"1", "true", "yes", "on"}


def is_perf_log_enabled() -> bool:
    return _to_bool(os.getenv("WINE_PERF_LOG_ENABLED"), default=True)


def get_perf_log_path() -> Path:
    custom_path = str(os.getenv("WINE_PERF_LOG_PATH", "")).strip()
    if custom_path:
        return Path(custom_path).expanduser().resolve()
    log_dir = str(os.getenv("WINE_LOG_DIR", "")).strip()
    if log_dir:
        return Path(log_dir).expanduser().resolve() / _DEFAULT_LOG_PATH.name
    return _DEFAULT_LOG_PATH


def _format_value(value: Any) -> str:
    if value is None:
        return "-"
    if isinstance(value, bool):
        return "yes" if value else "no"
    if isinstance(value, float):
        return f"{value:.2f}"
    text = str(value).strip()
    text = " ".join(text.split())
    if not text:
        return "-"
    if any(ch.isspace() for ch in text):
        return f'"{text}"'
    return text


def _format_human_line(ts: str, event: str, fields: dict[str, Any]) -> str:
    priority = [
        "status",
        "method",
        "path",
        "public_user",
        "user_source",
        "selected_model",
        "request_ms",
        "total_ms",
        "llm_rounds",
        "llm_wait_ms_total",
        "db_tool_calls",
        "db_query_ms_total",
        "web_tool_calls",
        "web_query_ms_total",
        "fallback_web_calls",
        "fallback_web_ms_total",
        "rows",
        "sql_count",
        "web_count",
        "sid",
    ]
    parts: list[str] = [f"{ts}", f"event={event}"]
    seen: set[str] = set()
    for key in priority:
        if key in fields:
            parts.append(f"{key}={_format_value(fields[key])}")
            seen.add(key)
    for key in sorted(k for k in fields.keys() if k not in seen):
        parts.append(f"{key}={_format_value(fields[key])}")
    return " | ".join(parts)


def append_perf_log(event: str, **fields: Any) -> bool:
    if not is_perf_log_enabled():
        return False

    ts = datetime.now(timezone.utc).strftime("%Y-%m-%d %H:%M:%S UTC")
    event_name = str(event or "").strip() or "event"
    normalized_fields: dict[str, Any] = {}
    for key, value in fields.items():
        normalized_fields[str(key)] = value

    path = get_perf_log_path()
    try:
        path.parent.mkdir(parents=True, exist_ok=True)
        line = _format_human_line(ts=ts, event=event_name, fields=normalized_fields) + "\n"
        with _LOCK:
            with path.open("a", encoding="utf-8") as f:
                f.write(line)
        return True
    except Exception:
        return False


def tail_perf_log(lines: int = 100, max_bytes: int = 262_144) -> list[str]:
    path = get_perf_log_path()
    if not path.exists() or lines <= 0:
        return []

    limit_lines = max(1, min(int(lines), 1000))
    try:
        with path.open("rb") as f:
            f.seek(0, os.SEEK_END)
            file_size = f.tell()
            read_size = min(file_size, max_bytes)
            f.seek(-read_size, os.SEEK_END)
            data = f.read(read_size)
        text = data.decode("utf-8", errors="replace")
        result = [line for line in text.splitlines() if line.strip()]
        if len(result) > limit_lines:
            result = result[-limit_lines:]
        return result
    except Exception:
        return []
