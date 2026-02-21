import json
import re
import sqlite3
from pathlib import Path

from sql_guard import build_safe_sql


class WineDB:
    def __init__(self, db_path: str | Path, table_name: str = "wine_cards_wide"):
        self.db_path = Path(db_path).resolve()
        self.table_name = table_name
        if not self.db_path.exists():
            raise FileNotFoundError(f"SQLite файл не найден: {self.db_path}")

    def _connect_ro(self) -> sqlite3.Connection:
        uri = f"file:{self.db_path.as_posix()}?mode=ro"
        conn = sqlite3.connect(uri, uri=True)
        conn.row_factory = sqlite3.Row
        return conn

    def ping(self) -> bool:
        with self._connect_ro() as conn:
            conn.execute("SELECT 1").fetchone()
        return True

    def get_columns(self) -> list[str]:
        with self._connect_ro() as conn:
            rows = conn.execute(f"PRAGMA table_info({self.table_name})").fetchall()
        return [row["name"] for row in rows]

    def get_schema_string(self) -> str:
        cols = self.get_columns()
        return f"Table: {self.table_name}\nColumns: {', '.join(cols)}"

    def get_distinct_values(self, column: str) -> list[str]:
        with self._connect_ro() as conn:
            rows = conn.execute(
                f"""
                SELECT DISTINCT {column}
                FROM {self.table_name}
                WHERE {column} IS NOT NULL
                  AND TRIM({column}) <> ''
                ORDER BY {column}
                """
            ).fetchall()
        return [str(r[0]) for r in rows]

    def get_reference_values(self) -> dict[str, list[str]]:
        refs = {
            "wine_color": self.get_distinct_values("wine_color"),
            "sugar_style": self.get_distinct_values("sugar_style"),
            "rating_status": self.get_distinct_values("rating_status"),
            "region": self.get_distinct_values("region"),
            "price_quality": self.get_distinct_values("price_quality"),
        }

        # recommendations в wide-колонке строка через запятую, но внутри терминов
        # возможны запятые ("..., барбекю"). Берем исходный raw из row_json и
        # разбираем по первичному разделителю ";".
        terms: set[str] = set()
        with self._connect_ro() as conn:
            rows = conn.execute(
                f"""
                SELECT row_json, recommendations
                FROM {self.table_name}
                """
            ).fetchall()
        for row in rows:
            parsed = False
            try:
                raw = json.loads(row["row_json"])
                raw_rec = str(raw.get("recommendations", "")).strip()
                if raw_rec:
                    for item in raw_rec.split(";"):
                        value = item.strip()
                        if value:
                            terms.add(value)
                    parsed = True
            except Exception:
                parsed = False

            if not parsed:
                # Fallback: если raw недоступен, используем колонку как есть.
                rec = str(row["recommendations"] or "").strip()
                if rec:
                    terms.add(rec)
        refs["recommendations"] = sorted(terms)
        return refs

    def execute_safe_query(
        self,
        raw_sql: str,
        max_rows: int = 200,
    ) -> tuple[str, list[dict]]:
        safe_sql = build_safe_sql(raw_sql, max_rows=max_rows)
        with self._connect_ro() as conn:
            cursor = conn.execute(safe_sql)
            rows = cursor.fetchall()

        result = [dict(row) for row in rows]
        return safe_sql, result

    def wine_exists(self, wine_id: str) -> bool:
        value = str(wine_id or "").strip()
        if not value:
            return False
        with self._connect_ro() as conn:
            row = conn.execute(
                f"""
                SELECT 1
                FROM {self.table_name}
                WHERE CAST(card_key AS TEXT) = ?
                   OR url = ?
                LIMIT 1
                """,
                (value, value),
            ).fetchone()
        return row is not None

    @staticmethod
    def _tokenize_reference(value: str) -> list[str]:
        return [t for t in re.findall(r"[0-9a-zа-яё]+", str(value or "").lower()) if len(t) >= 2]

    def search_wines_by_text(self, reference: str, limit: int = 10) -> list[dict]:
        ref = str(reference or "").strip()
        if not ref:
            return []

        tokens = self._tokenize_reference(ref)
        if not tokens:
            tokens = [ref.lower()]

        where_parts = []
        params: list[str | int] = []
        for tok in tokens[:8]:
            where_parts.append(
                """
                LOWER(
                    COALESCE(wine_name, '') || ' ' ||
                    COALESCE(producer, '') || ' ' ||
                    COALESCE(title, '')
                ) LIKE ?
                """
            )
            params.append(f"%{tok}%")

        params.append(max(1, min(int(limit), 50)))
        where_sql = " AND ".join(where_parts) if where_parts else "1=1"
        query = f"""
            SELECT
                card_key, wine_name, producer, harvest_year, region, rating_year, rating_points, url
            FROM {self.table_name}
            WHERE {where_sql}
            ORDER BY rating_year DESC, rating_points DESC, harvest_year DESC
            LIMIT ?
        """
        with self._connect_ro() as conn:
            rows = conn.execute(query, params).fetchall()
        return [dict(r) for r in rows]

    def get_wine_brief(self, wine_id: str) -> dict | None:
        value = str(wine_id or "").strip()
        if not value:
            return None
        with self._connect_ro() as conn:
            row = conn.execute(
                f"""
                SELECT
                    card_key, wine_name, producer, harvest_year, region, rating_year, rating_points, url
                FROM {self.table_name}
                WHERE CAST(card_key AS TEXT) = ?
                   OR url = ?
                ORDER BY rating_year DESC, rating_points DESC, harvest_year DESC
                LIMIT 1
                """,
                (value, value),
            ).fetchone()
        return dict(row) if row is not None else None

    def resolve_wine_id_from_fields(
        self,
        wine_name: str | None,
        producer: str | None = None,
        harvest_year: int | str | None = None,
    ) -> str | None:
        name = str(wine_name or "").strip()
        if not name:
            return None
        prod = str(producer or "").strip()
        year = str(harvest_year or "").strip()

        clauses = ["LOWER(COALESCE(wine_name,'')) = LOWER(?)"]
        params: list[str] = [name]
        if prod:
            clauses.append("LOWER(COALESCE(producer,'')) = LOWER(?)")
            params.append(prod)
        if year and year.isdigit():
            clauses.append("CAST(COALESCE(harvest_year,'') AS TEXT) = ?")
            params.append(year)

        where_sql = " AND ".join(clauses)
        query = f"""
            SELECT card_key
            FROM {self.table_name}
            WHERE {where_sql}
            ORDER BY rating_year DESC, rating_points DESC
            LIMIT 1
        """
        with self._connect_ro() as conn:
            row = conn.execute(query, params).fetchone()
        if row is None:
            return None
        return str(row["card_key"])
