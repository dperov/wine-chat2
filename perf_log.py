import json
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


def _safe_json(data: dict[str, Any]) -> str:
    return json.dumps(data, ensure_ascii=False, separators=(",", ":"))


def append_perf_log(event: str, **fields: Any) -> bool:
    if not is_perf_log_enabled():
        return False

    entry = {
        "ts": datetime.now(timezone.utc).isoformat(),
        "event": str(event or "").strip() or "event",
    }
    for key, value in fields.items():
        entry[str(key)] = value

    path = get_perf_log_path()
    try:
        path.parent.mkdir(parents=True, exist_ok=True)
        line = _safe_json(entry) + "\n"
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
