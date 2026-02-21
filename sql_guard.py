import re


class SQLValidationError(ValueError):
    pass


FORBIDDEN_KEYWORDS = (
    "insert",
    "update",
    "delete",
    "drop",
    "alter",
    "create",
    "attach",
    "detach",
    "pragma",
    "vacuum",
    "reindex",
    "analyze",
    "replace",
    "truncate",
)


def _strip_comments(sql: str) -> str:
    sql = re.sub(r"/\*.*?\*/", "", sql, flags=re.S)
    sql = re.sub(r"--[^\n]*", "", sql)
    return sql


def _normalize_sql(sql: str) -> str:
    sql = _strip_comments(sql).strip()
    if not sql:
        raise SQLValidationError("Пустой SQL-запрос.")
    sql = sql.rstrip(";").strip()
    if ";" in sql:
        raise SQLValidationError("Разрешен только один SQL-запрос.")
    return sql


def validate_read_only_sql(sql: str) -> str:
    normalized = _normalize_sql(sql)
    lower = normalized.lower()

    if not (lower.startswith("select ") or lower.startswith("with ")):
        raise SQLValidationError("Разрешены только SELECT/CTE-запросы.")

    for keyword in FORBIDDEN_KEYWORDS:
        if re.search(rf"\b{keyword}\b", lower):
            raise SQLValidationError(f"Запрещенное ключевое слово в SQL: {keyword}")

    return normalized


def enforce_limit(sql: str, max_rows: int) -> str:
    max_rows = max(1, int(max_rows))
    # Всегда ограничиваем результат внешним SELECT.
    return f"SELECT * FROM ({sql}) AS _result LIMIT {max_rows}"


def build_safe_sql(sql: str, max_rows: int) -> str:
    validated = validate_read_only_sql(sql)
    return enforce_limit(validated, max_rows)

