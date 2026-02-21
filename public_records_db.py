import sqlite3
from pathlib import Path
from typing import Any

from db import WineDB


class PublicRecordError(Exception):
    pass


class PublicRecordsDB:
    def __init__(self, db_path: str | Path, wine_db: WineDB):
        self.db_path = Path(db_path).resolve()
        self.wine_db = wine_db
        self._ensure_parent_dir()
        self._ensure_schema()

    def _ensure_parent_dir(self) -> None:
        self.db_path.parent.mkdir(parents=True, exist_ok=True)

    def _connect_rw(self) -> sqlite3.Connection:
        conn = sqlite3.connect(self.db_path)
        conn.row_factory = sqlite3.Row
        return conn

    def _ensure_schema(self) -> None:
        schema_path = Path(__file__).resolve().parent / "public_records.sql"
        schema_sql = schema_path.read_text(encoding="utf-8")
        with self._connect_rw() as conn:
            conn.executescript(schema_sql)

    @staticmethod
    def _normalize_record_type(value: str) -> str:
        normalized = str(value or "").strip().lower()
        if normalized not in {"like", "note"}:
            raise PublicRecordError("record_type должен быть 'like' или 'note'.")
        return normalized

    @staticmethod
    def _normalize_user(value: str | None) -> str:
        user = str(value or "").strip()
        if not user:
            return "Гость"
        return user

    @staticmethod
    def _normalize_content(value: str | None) -> str:
        return str(value or "").strip()

    def _normalize_wine_id(self, value: str) -> str:
        wine_id = str(value or "").strip()
        if not wine_id:
            raise PublicRecordError("wine_id обязателен.")
        if not self.wine_db.wine_exists(wine_id):
            raise PublicRecordError("wine_id не найден в каталоге wine_cards_wide.")
        return wine_id

    @staticmethod
    def _row_to_dict(row: sqlite3.Row | None) -> dict[str, Any] | None:
        if row is None:
            return None
        return {k: row[k] for k in row.keys()}

    def ping(self) -> bool:
        with self._connect_rw() as conn:
            conn.execute("SELECT 1").fetchone()
        return True

    def add_record(
        self,
        user: str | None,
        record_type: str,
        content: str | None,
        wine_id: str,
    ) -> dict[str, Any]:
        normalized_user = self._normalize_user(user)
        normalized_type = self._normalize_record_type(record_type)
        normalized_content = self._normalize_content(content)
        normalized_wine_id = self._normalize_wine_id(wine_id)

        if normalized_type == "note" and not normalized_content:
            raise PublicRecordError("Для record_type='note' поле content обязательно.")
        if normalized_type == "like":
            normalized_content = normalized_content or "1"

        with self._connect_rw() as conn:
            cur = conn.execute(
                """
                INSERT INTO public_records (user, record_type, content, wine_id)
                VALUES (?, ?, ?, ?)
                """,
                (normalized_user, normalized_type, normalized_content, normalized_wine_id),
            )
            rec_id = int(cur.lastrowid)
            row = conn.execute(
                "SELECT * FROM public_records WHERE id = ?",
                (rec_id,),
            ).fetchone()
        result = self._row_to_dict(row)
        if not result:
            raise PublicRecordError("Не удалось прочитать созданную запись.")
        return result

    def list_records(
        self,
        wine_id: str | None = None,
        record_type: str | None = None,
        user: str | None = None,
    ) -> list[dict[str, Any]]:
        clauses: list[str] = []
        params: list[Any] = []

        if wine_id is not None and str(wine_id).strip():
            clauses.append("wine_id = ?")
            params.append(str(wine_id).strip())

        if record_type is not None and str(record_type).strip():
            clauses.append("record_type = ?")
            params.append(self._normalize_record_type(record_type))

        if user is not None and str(user).strip():
            clauses.append("user = ?")
            params.append(str(user).strip())

        where_sql = f"WHERE {' AND '.join(clauses)}" if clauses else ""
        sql = (
            "SELECT * FROM public_records "
            f"{where_sql} "
            "ORDER BY created_at DESC, id DESC"
        )
        with self._connect_rw() as conn:
            rows = conn.execute(sql, params).fetchall()
        return [{k: row[k] for k in row.keys()} for row in rows]

    def get_wine_summary(self, wine_id: str) -> dict[str, Any]:
        normalized_wine_id = self._normalize_wine_id(wine_id)
        with self._connect_rw() as conn:
            likes_row = conn.execute(
                """
                SELECT COUNT(*) AS like_count
                FROM public_records
                WHERE wine_id = ? AND record_type = 'like'
                """,
                (normalized_wine_id,),
            ).fetchone()
            notes_row = conn.execute(
                """
                SELECT COUNT(*) AS note_count
                FROM public_records
                WHERE wine_id = ? AND record_type = 'note'
                """,
                (normalized_wine_id,),
            ).fetchone()
        return {
            "wine_id": normalized_wine_id,
            "like_count": int(likes_row["like_count"]) if likes_row else 0,
            "note_count": int(notes_row["note_count"]) if notes_row else 0,
        }
