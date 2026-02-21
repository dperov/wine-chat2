import json
import os
import threading
import time
import uuid
from pathlib import Path

from dotenv import find_dotenv, load_dotenv
from flask import Flask, jsonify, render_template, request, session

from assistant import WineAssistant
from db import WineDB
from perf_log import append_perf_log, get_perf_log_path, is_perf_log_enabled, tail_perf_log
from public_records_db import PublicRecordError, PublicRecordsDB

load_dotenv(find_dotenv())

BASE_DIR = Path(__file__).resolve().parent
DEFAULT_DB_PATH = (BASE_DIR.parent / "wine_product.sqlite").resolve()
DEFAULT_USER_DB_PATH = (BASE_DIR.parent / "wine_social.sqlite").resolve()
DB_PATH = Path(os.getenv("WINE_DB_PATH", str(DEFAULT_DB_PATH))).resolve()
USER_DB_PATH = Path(os.getenv("WINE_USER_DB_PATH", str(DEFAULT_USER_DB_PATH))).resolve()
TABLE_NAME = os.getenv("WINE_TABLE", "wine_cards_wide")
EXTERNAL_USER_ID_HEADER = os.getenv("EXTERNAL_USER_ID_HEADER", "X-External-User-Id")
CAPABILITIES_FILE = BASE_DIR / "SYSTEM_CAPABILITIES.md"
WELCOME_MESSAGE = (
    "В базе собраны карточки российских вин: название, производитель, регион, "
    "урожай, рейтинг, характеристики и гастрономические рекомендации. "
    "Задайте любой запрос по этим данным. "
    "Если хотите, в начале диалога представьтесь по имени, и я буду обращаться к вам по имени."
)
APP_DEBUG = str(os.getenv("WINE_APP_DEBUG", "0")).strip().lower() in {"1", "true", "yes", "on"}

app = Flask(__name__)
app.secret_key = os.getenv("FLASK_SECRET_KEY", "wine-chat2-dev-secret")

db = WineDB(DB_PATH, table_name=TABLE_NAME)
records_db = PublicRecordsDB(USER_DB_PATH, wine_db=db)
assistant = WineAssistant(db=db, records_db=records_db)

CHAT_STORE: dict[str, list[dict[str, str]]] = {}
CONTEXT_STORE: dict[str, dict] = {}
LOCK = threading.Lock()
MAX_HISTORY_ITEMS = 24


def _session_id() -> str:
    sid = session.get("sid")
    if not sid:
        sid = uuid.uuid4().hex
        session["sid"] = sid
    return sid


def _get_history(sid: str) -> list[dict[str, str]]:
    with LOCK:
        return CHAT_STORE.setdefault(sid, [])


def _append_history(sid: str, role: str, content: str) -> None:
    with LOCK:
        items = CHAT_STORE.setdefault(sid, [])
        items.append({"role": role, "content": content})
        if len(items) > MAX_HISTORY_ITEMS:
            CHAT_STORE[sid] = items[-MAX_HISTORY_ITEMS:]


def _get_context_state(sid: str) -> dict:
    with LOCK:
        return CONTEXT_STORE.setdefault(
            sid,
            {
                "last_wine_candidates": [],
                "pending_record_action": None,
            },
        )


def _update_context_state_from_meta(sid: str, meta: dict) -> dict:
    with LOCK:
        state = CONTEXT_STORE.setdefault(
            sid,
            {
                "last_wine_candidates": [],
                "pending_record_action": None,
            },
        )
        candidates = meta.get("wine_context_candidates")
        if isinstance(candidates, list) and candidates:
            state["last_wine_candidates"] = candidates[:30]

        if meta.get("clear_pending_record_action"):
            state["pending_record_action"] = None

        pending = meta.get("set_pending_record_action")
        if isinstance(pending, dict):
            state["pending_record_action"] = pending

        return {
            "last_wine_candidates_count": len(state.get("last_wine_candidates") or []),
            "has_pending_record_action": bool(state.get("pending_record_action")),
        }


def _resolve_external_user_id(payload: dict) -> str:
    header_name = EXTERNAL_USER_ID_HEADER
    header_value = str(request.headers.get(header_name, "")).strip() if header_name else ""
    if header_value:
        return header_value

    query_value = str(request.args.get("external_user_id", "")).strip()
    if query_value:
        return query_value

    payload_value = str((payload or {}).get("external_user_id", "")).strip()
    if payload_value:
        return payload_value

    return ""


def _resolve_effective_user(payload: dict) -> tuple[str, str]:
    requested_user = str((payload or {}).get("user", "")).strip()
    if requested_user:
        return requested_user, "payload.user"

    external_user_id = _resolve_external_user_id(payload)
    if external_user_id:
        return f"ext:{external_user_id}", f"external_id({EXTERNAL_USER_ID_HEADER})"

    return "Гость", "guest"


@app.route("/")
def index():
    return render_template(
        "index.html",
        db_path=str(DB_PATH),
        table_name=TABLE_NAME,
        model=assistant.model,
        welcome_message=WELCOME_MESSAGE,
    )


@app.route("/health")
def health():
    try:
        db.ping()
        records_db.ping()
        cols = db.get_columns()
        return jsonify(
            {
                "ok": True,
                "db": str(DB_PATH),
                "table": TABLE_NAME,
                "columns": len(cols),
                "records_db": str(USER_DB_PATH),
                "external_user_id_header": EXTERNAL_USER_ID_HEADER,
                "perf_log_enabled": is_perf_log_enabled(),
                "perf_log_path": str(get_perf_log_path()),
                "app_debug": APP_DEBUG,
            }
        )
    except Exception as exc:
        return jsonify({"ok": False, "error": str(exc)}), 500


@app.route("/capabilities", methods=["GET"])
def capabilities():
    try:
        text = CAPABILITIES_FILE.read_text(encoding="utf-8").strip()
    except Exception as exc:
        return jsonify({"ok": False, "error": f"Не удалось прочитать capabilities: {exc}"}), 500
    return jsonify({"ok": True, "capabilities": text})


@app.route("/chat", methods=["POST"])
def chat():
    request_t0 = time.perf_counter()
    payload = request.get_json(silent=True) or {}
    message = str(payload.get("message", "")).strip()
    if not message:
        return jsonify({"response": "Пустой запрос.", "meta": {"sql": None, "rows": 0}}), 400
    if len(message) > 4000:
        return jsonify({"response": "Слишком длинный запрос.", "meta": {"sql": None, "rows": 0}}), 400

    sid = _session_id()
    history = list(_get_history(sid))
    context_state = dict(_get_context_state(sid))
    _append_history(sid, "user", message)

    public_user, user_source = _resolve_effective_user(payload)

    try:
        answer, meta = assistant.ask(
            message,
            history=history,
            public_user=public_user,
            record_context=context_state,
        )
        if isinstance(meta, dict):
            meta.setdefault("public_user", public_user)
            meta.setdefault("public_user_source", user_source)
            state_meta = _update_context_state_from_meta(sid, meta)
            meta.setdefault("context", state_meta)
    except Exception as exc:
        answer = f"Ошибка обработки запроса: {exc}"
        meta = {"sql": None, "rows": 0, "model": assistant.model}

    elapsed_ms = round((time.perf_counter() - request_t0) * 1000, 2)
    perf_meta = meta.get("perf") if isinstance(meta, dict) else None
    append_perf_log(
        "chat_request",
        sid=sid[:10],
        method=request.method,
        path=request.path,
        status="ok" if not str(answer).startswith("Ошибка обработки запроса:") else "error",
        user_source=user_source,
        public_user=public_user,
        message_len=len(message),
        response_len=len(str(answer or "")),
        request_ms=elapsed_ms,
        llm_rounds=(perf_meta or {}).get("llm_rounds"),
        selected_model=(perf_meta or {}).get("selected_model"),
        llm_wait_ms_total=(perf_meta or {}).get("llm_wait_ms_total"),
        db_tool_calls=(perf_meta or {}).get("db_tool_calls"),
        db_query_ms_total=(perf_meta or {}).get("db_query_ms_total"),
        web_tool_calls=(perf_meta or {}).get("web_tool_calls"),
        web_query_ms_total=(perf_meta or {}).get("web_query_ms_total"),
        fallback_web_calls=(perf_meta or {}).get("fallback_web_calls"),
        fallback_web_ms_total=(perf_meta or {}).get("fallback_web_ms_total"),
        total_ms=(perf_meta or {}).get("total_ms"),
        rows=(meta or {}).get("rows") if isinstance(meta, dict) else None,
        sql_count=len((meta or {}).get("sql_queries") or []) if isinstance(meta, dict) else 0,
        web_count=len((meta or {}).get("web_queries") or []) if isinstance(meta, dict) else 0,
    )

    _append_history(sid, "assistant", answer)
    return jsonify({"response": answer, "meta": meta})


@app.route("/debug/perf/tail", methods=["GET"])
def debug_perf_tail():
    lines_raw = str(request.args.get("lines", "100")).strip()
    try:
        lines = int(lines_raw)
    except Exception:
        lines = 100
    lines = max(1, min(lines, 500))

    raw_lines = tail_perf_log(lines=lines)
    parsed: list[dict] = []
    for item in raw_lines:
        try:
            parsed.append(json.loads(item))
        except Exception:
            parsed.append({"raw": item})

    return jsonify(
        {
            "ok": True,
            "enabled": is_perf_log_enabled(),
            "path": str(get_perf_log_path()),
            "count": len(parsed),
            "lines": parsed,
        }
    )


@app.route("/api/records", methods=["POST"])
def create_public_record():
    payload = request.get_json(silent=True) or {}

    wine_id = str(payload.get("wine_id", "")).strip()
    record_type = str(payload.get("record_type", "")).strip()
    content = payload.get("content")
    user, user_source = _resolve_effective_user(payload)

    try:
        record = records_db.add_record(
            user=user,
            record_type=record_type,
            content=str(content) if content is not None else None,
            wine_id=wine_id,
        )
    except PublicRecordError as exc:
        return jsonify({"ok": False, "error": str(exc)}), 400
    except Exception as exc:
        return jsonify({"ok": False, "error": f"Ошибка сохранения записи: {exc}"}), 500

    return jsonify(
        {
            "ok": True,
            "record": record,
            "user_source": user_source,
            "external_user_id_header": EXTERNAL_USER_ID_HEADER,
        }
    )


@app.route("/api/records", methods=["GET"])
def list_public_records():
    wine_id = str(request.args.get("wine_id", "")).strip() or None
    record_type = str(request.args.get("record_type", "")).strip() or None
    user = str(request.args.get("user", "")).strip() or None

    try:
        records = records_db.list_records(wine_id=wine_id, record_type=record_type, user=user)
    except PublicRecordError as exc:
        return jsonify({"ok": False, "error": str(exc)}), 400
    except Exception as exc:
        return jsonify({"ok": False, "error": f"Ошибка чтения записей: {exc}"}), 500

    return jsonify({"ok": True, "count": len(records), "records": records})


@app.route("/api/records/by-wine/<path:wine_id>", methods=["GET"])
def list_public_records_by_wine(wine_id: str):
    record_type = str(request.args.get("record_type", "")).strip() or None
    user = str(request.args.get("user", "")).strip() or None

    try:
        records = records_db.list_records(wine_id=wine_id, record_type=record_type, user=user)
        summary = records_db.get_wine_summary(wine_id)
    except PublicRecordError as exc:
        return jsonify({"ok": False, "error": str(exc)}), 400
    except Exception as exc:
        return jsonify({"ok": False, "error": f"Ошибка чтения записей: {exc}"}), 500

    return jsonify({"ok": True, "summary": summary, "count": len(records), "records": records})


if __name__ == "__main__":
    port = int(os.getenv("PORT", "5000"))
    app.run(host="0.0.0.0", port=port, debug=APP_DEBUG)
