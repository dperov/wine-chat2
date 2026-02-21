import os
import csv
from datetime import datetime
from pathlib import Path

from dotenv import find_dotenv, load_dotenv

from assistant import WineAssistant
from db import WineDB
from public_records_db import PublicRecordsDB

load_dotenv(find_dotenv())


def save_rows_to_csv(rows: list[dict], out_path: Path) -> Path | None:
    if not rows:
        return None

    columns: list[str] = list(rows[0].keys())
    for row in rows[1:]:
        for key in row.keys():
            if key not in columns:
                columns.append(key)

    out_path.parent.mkdir(parents=True, exist_ok=True)
    with out_path.open("w", newline="", encoding="utf-8-sig") as f:
        writer = csv.DictWriter(f, fieldnames=columns, extrasaction="ignore")
        writer.writeheader()
        for row in rows:
            writer.writerow(row)

    return out_path


def build_csv_filename(prefix: str = "query_result") -> str:
    ts = datetime.now().strftime("%Y%m%d_%H%M%S")
    return f"{prefix}_{ts}.csv"


def dedupe_keep_order(items: list[str]) -> list[str]:
    seen: set[str] = set()
    result: list[str] = []
    for item in items:
        key = str(item).strip()
        if not key or key in seen:
            continue
        seen.add(key)
        result.append(key)
    return result


def print_web_tool_logs(logs: list[dict]) -> None:
    if not logs:
        print("[weblog] Операции web tool не выполнялись.")
        return

    print(f"[weblog] Выполнено web-операций: {len(logs)}")
    for i, item in enumerate(logs, 1):
        source = str(item.get("source", "tool_call"))
        engine = str(item.get("engine") or "").strip()
        ok = bool(item.get("ok"))
        query = str(item.get("query") or "").strip()
        search_query = str(item.get("search_query") or "").strip()
        count = int(item.get("count") or 0)
        status = "ok" if ok else "error"
        print(
            f"[weblog#{i}] source={source} status={status} count={count} "
            f"engine={engine!r} query={query!r} search_query={search_query!r}"
        )
        error = str(item.get("error") or "").strip()
        if error:
            print(f"[weblog#{i}.error] {error}")

        provider_errors = item.get("providers_errors") or []
        if isinstance(provider_errors, list):
            for j, perr in enumerate(provider_errors, 1):
                perr_text = str(perr).strip()
                if perr_text:
                    print(f"[weblog#{i}.provider_error#{j}] {perr_text}")

        results = item.get("results") or []
        if isinstance(results, list):
            for j, res in enumerate(results, 1):
                if not isinstance(res, dict):
                    continue
                title = str(res.get("title") or "").strip()
                url = str(res.get("url") or "").strip()
                if url:
                    print(f"[weblog#{i}.result#{j}] {title} | {url}")


def main() -> None:
    base_dir = Path(__file__).resolve().parent
    default_db_path = (base_dir.parent / "wine_product.sqlite").resolve()
    default_user_db_path = (base_dir.parent / "wine_social.sqlite").resolve()
    db_path = Path(os.getenv("WINE_DB_PATH", str(default_db_path))).resolve()
    user_db_path = Path(os.getenv("WINE_USER_DB_PATH", str(default_user_db_path))).resolve()
    table_name = os.getenv("WINE_TABLE", "wine_cards_wide")

    print("Wine Chat 2 | Console mode")
    print(f"DB: {db_path}")
    print(f"Records DB: {user_db_path}")
    print(f"Table: {table_name}")
    print(
        "Команды: /exit, /quit, /clear, /sql on, /sql off, "
        "/weblog on, /weblog off, /csv on, /csv off, /csv dir <path>\n"
    )
    print(
        "В базе собраны карточки российских вин: название, производитель, регион, "
        "урожай, рейтинг, характеристики и рекомендации. "
        "Задайте запрос по этим данным.\n"
    )

    db = WineDB(db_path, table_name=table_name)
    records_db = PublicRecordsDB(user_db_path, wine_db=db)
    assistant = WineAssistant(db=db, records_db=records_db)

    history: list[dict[str, str]] = []
    context_state: dict = {
        "last_wine_candidates": [],
        "pending_record_action": None,
    }
    show_sql = True
    show_web_log = False
    csv_mode = False
    csv_dir = (base_dir / "exports").resolve()

    while True:
        try:
            user_text = input("Вы> ").strip()
        except (EOFError, KeyboardInterrupt):
            print("\nВыход.")
            break

        if not user_text:
            continue

        cmd = user_text.lower()
        if cmd in {"/exit", "/quit"}:
            print("Выход.")
            break
        if cmd == "/clear":
            history.clear()
            context_state["last_wine_candidates"] = []
            context_state["pending_record_action"] = None
            print("История очищена.")
            continue
        if cmd == "/sql on":
            show_sql = True
            print("Показ SQL включен.")
            continue
        if cmd == "/sql off":
            show_sql = False
            print("Показ SQL выключен.")
            continue
        if cmd == "/weblog on":
            show_web_log = True
            print("Подробный лог web tool включен.")
            continue
        if cmd == "/weblog off":
            show_web_log = False
            print("Подробный лог web tool выключен.")
            continue
        if cmd == "/csv on":
            csv_mode = True
            print(f"CSV-режим включен. Папка: {csv_dir}")
            continue
        if cmd == "/csv off":
            csv_mode = False
            print("CSV-режим выключен.")
            continue
        if cmd.startswith("/csv dir "):
            new_dir = user_text[len("/csv dir ") :].strip().strip('"').strip("'")
            if not new_dir:
                print("Укажите путь: /csv dir <path>")
                continue
            csv_dir = Path(new_dir).expanduser().resolve()
            print(f"Папка для CSV: {csv_dir}")
            continue

        answer, meta = assistant.ask(
            user_text,
            history=history,
            public_user="Гость",
            record_context=context_state,
        )
        candidates = meta.get("wine_context_candidates")
        if isinstance(candidates, list) and candidates:
            context_state["last_wine_candidates"] = candidates[:30]
        if meta.get("clear_pending_record_action"):
            context_state["pending_record_action"] = None
        pending = meta.get("set_pending_record_action")
        if isinstance(pending, dict):
            context_state["pending_record_action"] = pending

        history.append({"role": "user", "content": user_text})
        history.append({"role": "assistant", "content": answer})

        print(f"\nБот> {answer}")
        if show_sql:
            sql = meta.get("sql")
            sql_queries = meta.get("sql_queries") or ([] if not sql else [sql])
            sql_queries = dedupe_keep_order(sql_queries)
            web_queries = dedupe_keep_order(meta.get("web_queries") or [])
            rows = meta.get("rows")
            model = meta.get("model")
            pending_mark = "yes" if context_state.get("pending_record_action") else "no"
            candidate_count = len(context_state.get("last_wine_candidates") or [])
            print(f"[meta] model={model} rows={rows}")
            print(f"[context] candidates={candidate_count} pending_record_action={pending_mark}")
            if len(sql_queries) > 1:
                print(f"[sql] Выполнено запросов: {len(sql_queries)}")
                for i, item in enumerate(sql_queries, 1):
                    print(f"[sql#{i}] {item}")
            elif sql:
                print(f"[sql] {sql}")
            if web_queries:
                if len(web_queries) == 1:
                    print(f"[web] {web_queries[0]}")
                else:
                    print(f"[web] Выполнено web-поисков: {len(web_queries)}")
                    for i, item in enumerate(web_queries, 1):
                        print(f"[web#{i}] {item}")
        if show_web_log:
            print_web_tool_logs(meta.get("web_tool_logs") or [])

        sql_queries = meta.get("sql_queries") or ([] if not meta.get("sql") else [meta.get("sql")])
        sql_queries = dedupe_keep_order(sql_queries)
        if csv_mode and sql_queries:
            try:
                if len(sql_queries) > 1:
                    print(f"[csv] Найдено SQL-запросов для экспорта: {len(sql_queries)}")

                for i, sql_item in enumerate(sql_queries, 1):
                    _, rows_data = db.execute_safe_query(str(sql_item), max_rows=50000)
                    prefix = "query_result" if len(sql_queries) == 1 else f"query_result_q{i:02d}"
                    csv_path = csv_dir / build_csv_filename(prefix=prefix)
                    print(f"[csv] Начинаю запись результатов в файл: {csv_path}")
                    written = save_rows_to_csv(rows_data, csv_path)
                    if written:
                        print(f"[csv] Запись завершена. Строк: {len(rows_data)}. Файл: {written}")
                    else:
                        print("[csv] Запрос не вернул строк, файл не создан.")
            except Exception as exc:
                print(f"[csv] Ошибка сохранения CSV: {exc}")
        print("")


if __name__ == "__main__":
    main()
