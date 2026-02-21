import json
import os
import re
import time
import unicodedata
from pathlib import Path
from typing import Any

from dotenv import find_dotenv, load_dotenv
try:
    from openai import OpenAI
except Exception:
    OpenAI = None

from db import WineDB
from public_records_db import PublicRecordError, PublicRecordsDB
from sql_guard import SQLValidationError
from web_search import search_wine_web

load_dotenv(find_dotenv())


class WineAssistant:
    def __init__(
        self,
        db: WineDB,
        records_db: PublicRecordsDB | None = None,
        capabilities_path: str | Path | None = None,
        model: str | None = None,
        max_history_messages: int | None = None,
        max_sql_rows: int = 200,
        max_rows_to_model: int = 80,
        max_completion_tokens: int | None = None,
    ):
        self.db = db
        self.records_db = records_db
        self.model_fast = (
            model
            or os.getenv("OPENAI_MODEL_FAST")
            or os.getenv("OPENAI_MODEL")
            or "gpt-4.1-mini"
        )
        self.model_complex = os.getenv("OPENAI_MODEL_COMPLEX", "gpt-4.1")
        self.model = self.model_fast
        if max_history_messages is None:
            self.max_history_messages = int(os.getenv("OPENAI_MAX_HISTORY_MESSAGES", "8"))
        else:
            self.max_history_messages = int(max_history_messages)
        self.max_sql_rows = max_sql_rows
        self.max_rows_to_model = max_rows_to_model
        self.max_completion_tokens = max_completion_tokens or int(
            os.getenv("OPENAI_MAX_COMPLETION_TOKENS", "1200")
        )
        self.capabilities_text = self._load_capabilities_text(capabilities_path)

        api_key = os.getenv("OPENAI_API_KEY")
        self.client = OpenAI(api_key=api_key) if (OpenAI and api_key) else None
        self.system_prompt = self._build_system_prompt()

        self.tools = [
            {
                "type": "function",
                "function": {
                    "name": "execute_sql",
                    "description": "Выполняет безопасный SELECT-запрос в SQLite и возвращает строки.",
                    "parameters": {
                        "type": "object",
                        "properties": {
                            "query": {
                                "type": "string",
                                "description": "SQL SELECT/CTE запрос к таблице wine_cards_wide",
                            }
                        },
                        "required": ["query"],
                    },
                },
            },
            {
                "type": "function",
                "function": {
                    "name": "search_web",
                    "description": (
                        "Ищет информацию в интернете по винной теме: наличие в продаже, цены, магазины,"
                        " обзоры, новости."
                    ),
                    "parameters": {
                        "type": "object",
                        "properties": {
                            "query": {
                                "type": "string",
                                "description": "Поисковый запрос",
                            },
                            "max_results": {
                                "type": "integer",
                                "description": "Максимум результатов (1..10)",
                            },
                        },
                        "required": ["query"],
                    },
                },
            },
        ]

        if self.records_db is not None:
            self.tools.extend(
                [
                    {
                        "type": "function",
                        "function": {
                            "name": "add_public_record",
                            "description": (
                                "Добавляет публичную пользовательскую запись по вину: лайк или заметку."
                            ),
                            "parameters": {
                                "type": "object",
                                "properties": {
                                    "wine_id": {
                                        "type": "string",
                                        "description": "Идентификатор вина: card_key или url из wine_cards_wide",
                                    },
                                    "record_type": {
                                        "type": "string",
                                        "description": "Тип записи: like или note",
                                    },
                                    "content": {
                                        "type": "string",
                                        "description": "Содержимое заметки (для like можно не передавать)",
                                    },
                                    "user": {
                                        "type": "string",
                                        "description": "Имя пользователя (опционально)",
                                    },
                                },
                                "required": ["wine_id", "record_type"],
                            },
                        },
                    },
                    {
                        "type": "function",
                        "function": {
                            "name": "list_public_records",
                            "description": "Читает публичные записи пользователей (лайки/заметки).",
                            "parameters": {
                                "type": "object",
                                "properties": {
                                    "wine_id": {
                                        "type": "string",
                                        "description": "Фильтр по вину (card_key или url)",
                                    },
                                    "record_type": {
                                        "type": "string",
                                        "description": "Фильтр: like или note",
                                    },
                                    "user": {
                                        "type": "string",
                                        "description": "Фильтр по имени пользователя",
                                    },
                                },
                            },
                        },
                    },
                    {
                        "type": "function",
                        "function": {
                            "name": "get_wine_public_summary",
                            "description": "Возвращает агрегат по публичным записям вина: число лайков и заметок.",
                            "parameters": {
                                "type": "object",
                                "properties": {
                                    "wine_id": {
                                        "type": "string",
                                        "description": "Идентификатор вина: card_key или url из wine_cards_wide",
                                    }
                                },
                                "required": ["wine_id"],
                            },
                        },
                    },
                ]
            )

    @staticmethod
    def _load_capabilities_text(capabilities_path: str | Path | None) -> str:
        default_path = Path(__file__).resolve().parent / "SYSTEM_CAPABILITIES.md"
        path = Path(capabilities_path).resolve() if capabilities_path else default_path
        try:
            text = path.read_text(encoding="utf-8").strip()
            if text:
                return text
        except Exception:
            pass
        return (
            "Система умеет: SQL-поиск по каталогу вин, web-поиск по винной теме, "
            "публичные лайки и заметки по винам."
        )

    def _build_system_prompt(self) -> str:
        schema = self.db.get_schema_string()
        refs = self.db.get_reference_values()

        def fmt(name: str) -> str:
            values = refs.get(name, [])
            if not values:
                return f"{name}: []"
            return f"{name}: " + ", ".join(values)

        return (
            "Ты винный ассистент. Поддерживай разговор на темы вина.\n\n"
            f"{schema}\n\n"
            "Справочники (используй только эти значения в фильтрах):\n"
            f"- {fmt('wine_color')}\n"
            f"- {fmt('sugar_style')}\n"
            f"- {fmt('rating_status')}\n"
            f"- {fmt('region')}\n"
            f"- {fmt('price_quality')}\n"
            f"- recommendations: {', '.join(refs.get('recommendations', []))}\n\n"
            "Правила работы:\n"
            "1) Только SELECT или WITH.\n"
            "2) Для данных из локальной базы используй execute_sql и таблицу wine_cards_wide.\n"
            "3) Для строк-списков (grapes, recommendations, available_vintages) используй LIKE.\n"
            "4) Не используй DDL/DML.\n"
            "5) alcohol_pct уже целое число процента.\n"
            "6) Если пользователь просит топ/список, сортируй явно и ограничивай выдачу.\n"
            "7) Если пользователь просит полный список (полностью/весь список/без сокращений), "
            "нельзя сокращать ответ и писать '... и еще N'.\n"
            "8) Если пользователь спрашивает о наличии в продаже, цене на полке, магазинах или другой "
            "внешней информации, используй search_web.\n"
            "9) В ответе пользователю запрещено указывать URL, названия сайтов и любые веб-источники.\n"
            "10) Для цены и наличия указывай, что это рыночные данные, "
            "которые могут отличаться по регионам/магазинам.\n"
            "11) Если в локальной базе данных не найдено совпадений по названию вина, "
            "используй search_web, чтобы дать практичный ответ без ссылок.\n"
            "12) Если пользователь просит поставить лайк/добавить заметку/показать заметки и лайки, "
            "используй инструменты публичных записей.\n"
            "13) Если пользователь спрашивает о возможностях системы, выдай краткую сводку.\n"
            "14) При поиске производителя учитывай возможные русские/латинские написания "
            "и делай фильтр с OR по вариантам.\n"
            "15) Если вопрос не о вине — вежливо откажись и предложи винную тему.\n\n"
            "Соответствия написаний производителей:\n"
            "- шато ле гранд восток <-> Chateau le Grand Vostock\n"
            "- абрау-дюрсо <-> Abrau-Durso\n"
            "- эссе <-> Esse\n\n"
            "Сводка возможностей системы:\n"
            f"{self.capabilities_text}"
        )

    @staticmethod
    def _is_full_list_request(text: str) -> bool:
        q = (text or "").strip().lower()
        q = unicodedata.normalize("NFKC", q).replace("ё", "е")
        markers = (
            "полностью",
            "полный список",
            "весь список",
            "этот список",
            "все строки",
            "все записи",
            "без сокращ",
            "покажи всё",
            "покажи все",
            "представь этот список полностью",
        )
        normalized_markers = [unicodedata.normalize("NFKC", m).replace("ё", "е") for m in markers]
        return any(m in q for m in normalized_markers)

    @staticmethod
    def _is_capabilities_request(text: str) -> bool:
        q = (text or "").strip().lower()
        q = unicodedata.normalize("NFKC", q).replace("ё", "е")
        markers = (
            "что ты умеешь",
            "что умеет система",
            "возможности",
            "справка",
            "help",
            "шаблон",
            "пример команд",
            "покажи возможности",
        )
        normalized = [unicodedata.normalize("NFKC", m).replace("ё", "е") for m in markers]
        return any(m in q for m in normalized)

    @staticmethod
    def _is_complex_query(text: str) -> bool:
        q = WineAssistant._normalize_text(text)
        if not q:
            return False
        if len(q) >= 180:
            return True

        complex_markers = (
            "сравни",
            "сравнение",
            "проанализ",
            "обоснуй",
            "почему",
            "подробно",
            "сценар",
            "стратег",
            "подбери",
            "рекоменд",
            "пошагов",
            "разлож",
            "критер",
            "несколько вариантов",
        )
        if any(m in q for m in complex_markers):
            return True

        separators = q.count(" и ") + q.count(" или ") + q.count(",")
        if separators >= 4:
            return True

        if q.count("?") >= 2:
            return True
        return False

    def _select_model_for_query(self, user_text: str) -> str:
        if self.model_complex == self.model_fast:
            return self.model_fast
        if self._is_complex_query(user_text):
            return self.model_complex
        return self.model_fast

    @staticmethod
    def _pretty_key(name: str) -> str:
        labels = {
            "wine_name": "Вино",
            "producer": "Производитель",
            "harvest_year": "Урожай",
            "rating_points": "Рейтинг",
            "rating_year": "Год оценки",
            "region": "Регион",
            "url": "Ссылка",
            "wine_color": "Цвет",
            "sugar_style": "Сахарность",
            "alcohol_pct": "Алкоголь (%)",
            "price_quality": "Цена/качество",
        }
        return labels.get(name, name)

    def _format_full_list_answer(self, rows: list[dict[str, Any]]) -> str:
        if not rows:
            return "Ничего не найдено."

        lines = [f"Найдено записей: {len(rows)}. Полный список:"]
        for idx, row in enumerate(rows, 1):
            parts: list[str] = []
            for key, value in row.items():
                if str(key).lower() == "url":
                    continue
                if value is None:
                    continue
                text = str(value).strip()
                if not text:
                    continue
                parts.append(f"{self._pretty_key(key)}: {text}")
            if not parts:
                parts = ["(пустая строка)"]
            lines.append(f"{idx}. " + " | ".join(parts))
        return "\n".join(lines)

    @staticmethod
    def _is_price_or_availability_request(text: str) -> bool:
        q = (text or "").lower()
        markers = (
            "цена",
            "сколько стоит",
            "стоит",
            "налич",
            "продается",
            "продаётся",
            "где купить",
            "купить",
            "на полке",
            "в магазине",
        )
        return any(m in q for m in markers)

    @staticmethod
    def _looks_like_wine_name_or_topic(text: str) -> bool:
        q = (text or "").strip().lower()
        if not q:
            return False
        if any(m in q for m in ("вино", "вин", "сорт", "винтаж", "магнум", "игрист")):
            return True
        tokens = re.findall(r"[0-9a-zа-яё]+", q, flags=re.IGNORECASE)
        return 1 <= len(tokens) <= 8 and len(q) <= 90

    @staticmethod
    def _dedupe_web_results(results: list[dict[str, Any]]) -> list[dict[str, Any]]:
        seen: set[str] = set()
        out: list[dict[str, Any]] = []
        for item in results:
            url = str(item.get("url", "")).strip()
            if not url or url in seen:
                continue
            seen.add(url)
            out.append(item)
        return out

    @staticmethod
    def _sanitize_public_answer(text: str) -> str:
        cleaned = str(text or "")

        # Remove direct links and domain-like mentions.
        cleaned = re.sub(r"https?://\S+", "", cleaned, flags=re.IGNORECASE)
        cleaned = re.sub(r"\bwww\.[^\s]+", "", cleaned, flags=re.IGNORECASE)
        cleaned = re.sub(
            r"\b(?:[a-z0-9-]+\.)+(?:ru|com|net|org|info|io|рф)\b(?:/[^\s]*)?",
            "",
            cleaned,
            flags=re.IGNORECASE,
        )

        blocked_line_markers = (
            "источники web-поиска",
            "web-поиск",
            "web search",
            "источники поиска",
            "источники:",
        )

        lines: list[str] = []
        for raw_line in cleaned.splitlines():
            line = raw_line.strip()
            low = line.lower()
            if any(marker in low for marker in blocked_line_markers):
                continue
            line = re.sub(r"\bссылк[а-я]*\b", "", line, flags=re.IGNORECASE).strip()
            line = re.sub(r"\s{2,}", " ", line)
            if line:
                lines.append(line)
            elif lines and lines[-1] != "":
                lines.append("")

        cleaned = "\n".join(lines).strip()
        cleaned = re.sub(r"\n{3,}", "\n\n", cleaned)

        if not cleaned:
            return "Готов ответить по данным базы российских вин. Сформулируйте запрос."
        return cleaned

    @staticmethod
    def _normalize_text(text: str) -> str:
        q = (text or "").strip().lower()
        return unicodedata.normalize("NFKC", q).replace("ё", "е")

    @staticmethod
    def _ordinal_word_to_int(word: str) -> int | None:
        w = str(word or "").lower().strip()
        if not w:
            return None
        w = re.sub(r"[^а-яё]", "", w).replace("ё", "е")
        stems = [
            ("одиннадцат", 11),
            ("двенадцат", 12),
            ("тринадцат", 13),
            ("четырнадцат", 14),
            ("пятнадцат", 15),
            ("шестнадцат", 16),
            ("семнадцат", 17),
            ("восемнадцат", 18),
            ("девятнадцат", 19),
            ("двадцат", 20),
            ("десят", 10),
            ("девят", 9),
            ("восьм", 8),
            ("седьм", 7),
            ("шест", 6),
            ("пят", 5),
            ("четверт", 4),
            ("трет", 3),
            ("втор", 2),
            ("перв", 1),
        ]
        for stem, value in stems:
            if w.startswith(stem):
                return value
        return None

    @staticmethod
    def _count_word_to_int(word: str) -> int | None:
        w = str(word or "").lower().strip()
        if not w:
            return None
        w = re.sub(r"[^а-яё]", "", w).replace("ё", "е")
        stems = [
            ("одиннадцат", 11),
            ("одиннадц", 11),
            ("двенадцат", 12),
            ("двенадц", 12),
            ("тринадцат", 13),
            ("тринадц", 13),
            ("четырнадцат", 14),
            ("четырнадц", 14),
            ("пятнадцат", 15),
            ("пятнадц", 15),
            ("шестнадцат", 16),
            ("шестнадц", 16),
            ("семнадцат", 17),
            ("семнадц", 17),
            ("восемнадцат", 18),
            ("восемнадц", 18),
            ("девятнадцат", 19),
            ("девятнадц", 19),
            ("двадцат", 20),
            ("десят", 10),
            ("девят", 9),
            ("восем", 8),
            ("сем", 7),
            ("шест", 6),
            ("пят", 5),
            ("четыр", 4),
            ("три", 3),
            ("два", 2),
            ("один", 1),
        ]
        for stem, value in stems:
            if w.startswith(stem):
                return value
        return None

    def _position_token_to_int(self, token: str) -> int | None:
        t = self._normalize_text(token)
        if not t:
            return None
        t = t.strip().strip(".,;:()[]{}")
        if not t:
            return None
        m = re.match(r"^(\d+)", t)
        if m:
            return int(m.group(1))
        return self._ordinal_word_to_int(t)

    def _count_token_to_int(self, token: str) -> int | None:
        t = self._normalize_text(token)
        if not t:
            return None
        m = re.match(r"^(\d+)", t)
        if m:
            return int(m.group(1))
        return self._count_word_to_int(t)

    @staticmethod
    def _is_all_positions_phrase(text: str) -> bool:
        q = str(text or "")
        markers = (
            "все позиции",
            "всех позиций",
            "все из списка",
            "всех из списка",
            "все позиции из списка",
            "всех позиций из списка",
            "все из результатов",
            "все строки",
            "всех строк",
            "все пункты",
            "всех пунктов",
            "все варианты",
            "всех вариантов",
            "все вина из списка",
            "все найденные",
            "все найденные вина",
            "все из них",
            "все они",
            "всем из списка",
            "для всех позиций",
            "для всех пунктов",
            "по всем позициям",
        )
        if any(m in q for m in markers):
            return True
        if q.strip() in {"все", "все их", "все они", "всем", "всех"}:
            return True
        if re.search(r"\bвсе\b", q) and any(
            x in q for x in ("спис", "результат", "позиц", "пункт", "строк", "вариант")
        ):
            return True
        return False

    @staticmethod
    def _has_list_reference_phrase(text: str) -> bool:
        q = str(text or "")
        markers = (
            "позици",
            "номер",
            "из списка",
            "из результатов",
            "вариант",
            "пункт",
            "строк",
            "вино 1",
            "вина 1",
        )
        return any(m in q for m in markers)

    @staticmethod
    def _expand_range(start: int, end: int) -> list[int]:
        if start <= 0 or end <= 0:
            return []
        if start <= end:
            return list(range(start, end + 1))
        return list(range(start, end - 1, -1))

    def _extract_position_refs(self, text: str, max_count: int | None = None) -> list[int]:
        q = self._normalize_text(text)
        if not q:
            return []
        max_n = int(max_count) if (max_count or 0) > 0 else None

        if self._is_all_positions_phrase(q):
            if max_n:
                return list(range(1, max_n + 1))
            return []

        # "первые 3", "первые три", "последние 2"
        if max_n:
            m_first = re.search(r"\bперв(?:ые|ых|ую|ой)?\s+([0-9а-яё-]+)\b", q)
            if m_first:
                n = self._count_token_to_int(m_first.group(1))
                if n and n > 0:
                    n = min(n, max_n)
                    return list(range(1, n + 1))
            m_last = re.search(r"\bпоследн(?:ие|их|юю|ей)?\s+([0-9а-яё-]+)\b", q)
            if m_last:
                n = self._count_token_to_int(m_last.group(1))
                if n and n > 0:
                    n = min(n, max_n)
                    return list(range(max_n - n + 1, max_n + 1))

        list_ref = self._has_list_reference_phrase(q)
        compact_range = bool(
            re.fullmatch(r"\s*(?:с|от)?\s*[0-9а-яё-]+\s*(?:по|до|-|–|—|\.\.)\s*[0-9а-яё-]+\s*", q)
        )
        allow_list_parsing = bool(max_n) or list_ref or compact_range

        # Numeric ranges: "3-5", "3..5", "с 3 по 5", "от 3 до 5"
        if allow_list_parsing:
            nums: list[int] = []
            for a, b in re.findall(r"(?:^|[\s,;])(?:с|от)?\s*(\d+)\s*(?:по|до)\s*(\d+)", q):
                nums.extend(self._expand_range(int(a), int(b)))
            for a, b in re.findall(r"\b(\d+)\s*(?:-|–|—|\.\.)\s*(\d+)\b", q):
                nums.extend(self._expand_range(int(a), int(b)))
            if nums:
                nums = self._dedupe_ints(nums)
                if max_n:
                    nums = [n for n in nums if 1 <= n <= max_n]
                if nums:
                    return nums

        # Word ranges: "с третьей по пятую"
        range_words = re.findall(r"(?:с|от)\s+([0-9а-яё-]+)\s+(?:по|до)\s+([0-9а-яё-]+)", q)
        if allow_list_parsing and range_words:
            nums: list[int] = []
            for wa, wb in range_words:
                a = self._position_token_to_int(wa)
                b = self._position_token_to_int(wb)
                if a and b:
                    nums.extend(self._expand_range(a, b))
            nums = self._dedupe_ints(nums)
            if max_n:
                nums = [n for n in nums if 1 <= n <= max_n]
            if nums:
                return nums

        # "1", "1 и 2", "1,2"
        if re.fullmatch(r"[\d,\s;#№и\-]+", q):
            nums = [int(x) for x in re.findall(r"\d+", q)]
            nums = self._dedupe_ints(nums)
            if max_n:
                nums = [n for n in nums if 1 <= n <= max_n]
            return nums

        if allow_list_parsing and (list_ref or max_n):
            nums = [int(x) for x in re.findall(r"\d+", q)]
            words = re.findall(r"[а-яё-]+", q)
            nums.extend(n for n in (self._ordinal_word_to_int(w) for w in words) if n)
            nums = self._dedupe_ints(nums)
            if max_n:
                nums = [n for n in nums if 1 <= n <= max_n]
            if nums:
                return nums

        if max_n and re.fullmatch(r"[а-яё,\s\-]+", q):
            words = re.findall(r"[а-яё-]+", q)
            nums = [n for n in (self._ordinal_word_to_int(w) for w in words) if n]
            nums = self._dedupe_ints(nums)
            nums = [n for n in nums if 1 <= n <= max_n]
            if nums:
                return nums

        if max_n and any(x in q for x in ("выбираю", "беру", "выбери", "выберу")):
            m_num = re.search(r"\b(\d+)\s*(?:я|й)?\b", q)
            if m_num:
                n = int(m_num.group(1))
                if 1 <= n <= max_n:
                    return [n]
            words = re.findall(r"[а-яё-]+", q)
            for w in words:
                n = self._ordinal_word_to_int(w)
                if n and 1 <= n <= max_n:
                    return [n]
            return []

        if self._has_list_reference_phrase(q):
            nums = [int(x) for x in re.findall(r"\d+", q)]
            nums = self._dedupe_ints(nums)
            if max_n:
                nums = [n for n in nums if 1 <= n <= max_n]
            return nums

        m = re.search(r"\b(\d+)\s*(?:я|й)?\b", q)
        if m and any(x in q for x in ("выбираю", "беру")):
            n = int(m.group(1))
            if max_n and not (1 <= n <= max_n):
                return []
            return [n]

        return []

    @staticmethod
    def _dedupe_ints(values: list[int]) -> list[int]:
        seen: set[int] = set()
        out: list[int] = []
        for v in values:
            if v in seen:
                continue
            seen.add(v)
            out.append(v)
        return out

    def _extract_record_intent(self, text: str) -> str | None:
        q = self._normalize_text(text)
        if "заметк" in q:
            return "note"
        if any(m in q for m in ("лайк", "нравится", "понравил", "отметь", "отметк", "отметка")):
            return "like"
        return None

    @staticmethod
    def _is_explicit_record_action(text: str) -> bool:
        q = WineAssistant._normalize_text(text)
        action_verbs = (
            "постав",
            "добав",
            "сдела",
            "созда",
            "сохрани",
            "запиши",
            "отметь",
            "лайкни",
        )
        record_words = ("лайк", "заметк", "отметк")
        return any(v in q for v in action_verbs) and any(w in q for w in record_words)

    def _extract_note_content(self, text: str) -> str | None:
        raw = str(text or "").strip()
        if ":" in raw:
            tail = raw.split(":", 1)[1].strip()
            if tail:
                return tail
        m = re.search(r"(?:текст заметки|заметка)\s+(.+)$", raw, flags=re.IGNORECASE)
        if m:
            content = m.group(1).strip()
            if content:
                return content
        return None

    def _extract_wine_reference(self, text: str) -> str | None:
        raw = str(text or "").strip()
        before_colon = raw.split(":", 1)[0].strip()
        patterns = [
            r"(?:для|к)\s*вину?\s+(.+)$",
            r"вину?\s+(.+)$",
            r"вина\s+(.+)$",
        ]
        for p in patterns:
            m = re.search(p, before_colon, flags=re.IGNORECASE)
            if m:
                ref = m.group(1).strip().strip('"').strip("'")
                if ref:
                    return ref
        return None

    @staticmethod
    def _format_wine_label(item: dict[str, Any]) -> str:
        name = str(item.get("wine_name") or item.get("title") or "").strip()
        producer = str(item.get("producer") or "").strip()
        year = str(item.get("harvest_year") or "").strip()
        parts = [p for p in [name, producer, year] if p]
        return ", ".join(parts) if parts else "Без названия"

    def _normalize_candidate(self, row: dict[str, Any]) -> dict[str, Any] | None:
        wine_id = str(row.get("wine_id") or row.get("card_key") or "").strip()
        url = str(row.get("url") or "").strip()
        wine_name = str(row.get("wine_name") or row.get("title") or "").strip()
        producer = str(row.get("producer") or "").strip()
        harvest_year = row.get("harvest_year")

        if not wine_id and url:
            wine_id = url

        if not wine_id and wine_name:
            resolved = self.db.resolve_wine_id_from_fields(
                wine_name=wine_name,
                producer=producer or None,
                harvest_year=harvest_year,
            )
            if resolved:
                wine_id = resolved

        if not wine_id:
            return None

        brief = self.db.get_wine_brief(wine_id)
        if brief is not None:
            wine_id = str(brief.get("card_key") or wine_id)
            if not wine_name:
                wine_name = str(brief.get("wine_name") or "").strip()
            if not producer:
                producer = str(brief.get("producer") or "").strip()
            if not harvest_year:
                harvest_year = brief.get("harvest_year")
            if not url:
                url = str(brief.get("url") or "").strip()
            if not row.get("region"):
                row["region"] = brief.get("region")

        return {
            "wine_id": wine_id,
            "wine_name": wine_name or None,
            "producer": producer or None,
            "harvest_year": harvest_year,
            "region": row.get("region"),
            "url": url or None,
        }

    def _extract_wine_candidates_from_rows(self, rows: list[dict[str, Any]]) -> list[dict[str, Any]]:
        out: list[dict[str, Any]] = []
        for row in rows[:200]:
            if not isinstance(row, dict):
                continue
            normalized = self._normalize_candidate(row)
            if not normalized:
                continue
            out.append(normalized)
            if len(out) >= 30:
                break
        return out

    def _search_wine_candidates(self, reference: str, limit: int = 7) -> list[dict[str, Any]]:
        rows = self.db.search_wines_by_text(reference=reference, limit=limit)
        out: list[dict[str, Any]] = []
        seen: set[str] = set()
        for row in rows:
            normalized = self._normalize_candidate(row)
            if not normalized:
                continue
            key = str(normalized.get("wine_id") or "")
            if not key or key in seen:
                continue
            seen.add(key)
            out.append(normalized)
        return out

    def _format_candidates_prompt(
        self,
        candidates: list[dict[str, Any]],
        record_type: str,
        note_content: str | None = None,
    ) -> str:
        action = "заметки" if record_type == "note" else "лайка"
        lines = [f"Найдено несколько вариантов для {action}. Выберите номер позиции:"]
        for idx, item in enumerate(candidates, 1):
            label = self._format_wine_label(item)
            lines.append(f"{idx}. {label}")
        if record_type == "note" and note_content:
            lines.append("Текст заметки уже сохранен в контексте и будет применен после выбора позиции.")
        lines.append("Ответьте числом, например: 1")
        return "\n".join(lines)

    def _build_record_saved_answer(
        self,
        record_type: str,
        candidate: dict[str, Any],
        content: str | None = None,
    ) -> str:
        label = self._format_wine_label(candidate)
        if record_type == "like":
            return f"Лайк сохранен для вина: {label}."
        return f"Заметка сохранена для вина: {label}.\nТекст заметки: {content or ''}"

    @staticmethod
    def _is_my_records_request(text: str) -> bool:
        q = WineAssistant._normalize_text(text)
        markers = (
            "мои отмет",
            "мои лайк",
            "мои замет",
            "мои запис",
            "покажи мои отмет",
            "покажи мои лайк",
            "покажи мои замет",
            "покажи мои запис",
        )
        if any(m in q for m in markers):
            return True
        return bool(
            re.search(r"\bмои\b", q)
            and any(x in q for x in ("отмет", "лайк", "замет", "запис"))
        )

    @staticmethod
    def _extract_records_filter_type(text: str) -> str | None:
        q = WineAssistant._normalize_text(text)
        has_like = "лайк" in q
        has_note = "замет" in q
        if has_like and not has_note:
            return "like"
        if has_note and not has_like:
            return "note"
        return None

    def _format_public_records_answer(
        self,
        rows: list[dict[str, Any]],
        user: str,
        record_type: str | None = None,
    ) -> str:
        if not rows:
            if record_type == "like":
                return "У вас пока нет лайков."
            if record_type == "note":
                return "У вас пока нет заметок."
            return "У вас пока нет публичных записей (лайков/заметок)."

        type_label = "записей"
        if record_type == "like":
            type_label = "лайков"
        elif record_type == "note":
            type_label = "заметок"
        lines = [f"Найдено {len(rows)} ваших {type_label} (пользователь: {user})."]

        brief_cache: dict[str, dict[str, Any] | None] = {}
        limit = 30
        for idx, row in enumerate(rows[:limit], 1):
            wine_id = str(row.get("wine_id") or "").strip()
            if wine_id not in brief_cache:
                brief_cache[wine_id] = self.db.get_wine_brief(wine_id) if wine_id else None
            brief = brief_cache[wine_id] or {}
            label = self._format_wine_label(
                {
                    "wine_name": brief.get("wine_name") or f"wine_id={wine_id}",
                    "producer": brief.get("producer"),
                    "harvest_year": brief.get("harvest_year"),
                }
            )
            rec_type = str(row.get("record_type") or "").strip()
            kind = "лайк" if rec_type == "like" else "заметка"
            created = str(row.get("created_at") or "").strip()
            content = str(row.get("content") or "").strip()
            line = f"{idx}. [{kind}] {label}"
            if created:
                line += f" | {created}"
            if rec_type == "note" and content:
                line += f" | {content}"
            lines.append(line)
        if len(rows) > limit:
            lines.append(f"... и еще {len(rows) - limit}")
        return "\n".join(lines)

    def _handle_my_records_request(
        self,
        user_text: str,
        public_user: str | None,
    ) -> tuple[str, dict[str, Any]] | None:
        if self.records_db is None:
            return None
        if not self._is_my_records_request(user_text):
            return None

        user = str(public_user or "").strip() or "Гость"
        record_type = self._extract_records_filter_type(user_text)
        rows = self.records_db.list_records(user=user, record_type=record_type)
        answer = self._format_public_records_answer(rows=rows, user=user, record_type=record_type)
        return (
            answer,
            {
                "sql": None,
                "rows": 0,
                "model": self.model,
                "public_record_ops": [
                    {
                        "op": "direct_list_my_records",
                        "ok": True,
                        "count": len(rows),
                        "user": user,
                        "record_type": record_type,
                    }
                ],
            },
        )

    def _handle_contextual_record_intent(
        self,
        user_text: str,
        public_user: str | None,
        record_context: dict[str, Any] | None,
    ) -> tuple[str, dict[str, Any]] | None:
        if self.records_db is None:
            return None

        context = record_context or {}
        pending = context.get("pending_record_action")
        pending = pending if isinstance(pending, dict) else None

        intent = self._extract_record_intent(user_text)
        if not intent and not pending:
            return None
        if intent and not pending and not self._is_explicit_record_action(user_text):
            return None

        record_type = intent or str(pending.get("record_type") or "").strip()
        if record_type not in {"like", "note"}:
            return None

        source_candidates = []
        if pending and isinstance(pending.get("candidates"), list):
            source_candidates = pending.get("candidates") or []
        elif isinstance(context.get("last_wine_candidates"), list):
            source_candidates = context.get("last_wine_candidates") or []

        source_candidates = [c for c in source_candidates if isinstance(c, dict)]
        pos_list = self._extract_position_refs(
            user_text,
            max_count=len(source_candidates) if source_candidates else None,
        )
        has_list_ref = self._has_list_reference_phrase(self._normalize_text(user_text))
        is_all_ref = self._is_all_positions_phrase(self._normalize_text(user_text))
        note_content = self._extract_note_content(user_text)
        if not note_content and pending and record_type == "note":
            note_content = str(pending.get("content") or "").strip() or None

        selected: dict[str, Any] | None = None
        selected_list: list[dict[str, Any]] = []
        candidates: list[dict[str, Any]] = []

        if pos_list or has_list_ref or is_all_ref:
            candidates = source_candidates
            if not candidates:
                return (
                    "Не найден контекст списка вин. Сначала запросите список вин, затем укажите номер позиции.",
                    {
                        "sql": None,
                        "rows": 0,
                        "model": self.model,
                        "public_record_ops": [],
                    },
                )
            if not pos_list and is_all_ref:
                pos_list = list(range(1, len(candidates) + 1))
            if not pos_list and has_list_ref:
                return (
                    "Не удалось определить позиции из запроса. Укажите номера явно, например: 1,2 или с 3 по 5.",
                    {
                        "sql": None,
                        "rows": 0,
                        "model": self.model,
                        "public_record_ops": [],
                    },
                )
            bad = [p for p in pos_list if p < 1 or p > len(candidates)]
            if bad:
                return (
                    f"Позиции {', '.join(str(x) for x in bad)} вне диапазона 1..{len(candidates)}. "
                    "Укажите корректные номера.",
                    {
                        "sql": None,
                        "rows": 0,
                        "model": self.model,
                        "public_record_ops": [],
                    },
                )
            for p in pos_list:
                normalized = self._normalize_candidate(candidates[p - 1])
                if normalized:
                    selected_list.append(normalized)
            if selected_list:
                selected = selected_list[0]
        else:
            reference = self._extract_wine_reference(user_text)
            if not reference and pending and str(pending.get("reference") or "").strip():
                reference = str(pending.get("reference") or "").strip()

            if reference:
                candidates = self._search_wine_candidates(reference, limit=7)
                if len(candidates) == 1:
                    selected = candidates[0]
                    selected_list = [candidates[0]]
                elif len(candidates) > 1:
                    return (
                        self._format_candidates_prompt(
                            candidates=candidates,
                            record_type=record_type,
                            note_content=note_content,
                        ),
                        {
                            "sql": None,
                            "rows": 0,
                            "model": self.model,
                            "public_record_ops": [],
                            "set_pending_record_action": {
                                "record_type": record_type,
                                "content": note_content,
                                "reference": reference,
                                "candidates": candidates,
                            },
                        },
                    )
                else:
                    return (
                        "Не удалось однозначно найти вино по названию. Уточните название или сначала получите список вин.",
                        {
                            "sql": None,
                            "rows": 0,
                            "model": self.model,
                            "public_record_ops": [],
                        },
                    )

        if not selected_list and selected:
            selected_list = [selected]

        if not selected_list and pending and isinstance(pending.get("selected_list"), list):
            selected_list = [
                c for c in pending.get("selected_list") or [] if isinstance(c, dict)
            ]

        if not selected and pending and isinstance(pending.get("selected"), dict):
            selected = self._normalize_candidate(pending.get("selected") or {})
        if not selected and selected_list:
            selected = selected_list[0]

        if not selected:
            return None

        bad_candidates = [c for c in selected_list if not str(c.get("wine_id") or "").strip()]
        if bad_candidates:
            return (
                "Не удалось определить идентификатор одного из выбранных вин. Уточните запрос.",
                {"sql": None, "rows": 0, "model": self.model, "public_record_ops": []},
            )

        unique_selected_list: list[dict[str, Any]] = []
        seen_ids: set[str] = set()
        duplicate_selected_count = 0
        for cand in selected_list:
            wine_id = str(cand.get("wine_id") or "").strip()
            if not wine_id:
                continue
            if wine_id in seen_ids:
                duplicate_selected_count += 1
                continue
            seen_ids.add(wine_id)
            unique_selected_list.append(cand)
        if unique_selected_list:
            selected_list = unique_selected_list
            selected = selected_list[0]

        if record_type == "note" and not note_content:
            if len(selected_list) == 1:
                label = self._format_wine_label(selected)
            else:
                label = ", ".join(self._format_wine_label(c) for c in selected_list[:3])
                if len(selected_list) > 3:
                    label += f" и ещё {len(selected_list) - 3}"
            return (
                f"Определены вина: {label}. Теперь укажите текст заметки в формате:\n"
                "заметка для выбранного вина: <текст>",
                {
                    "sql": None,
                    "rows": 0,
                    "model": self.model,
                    "public_record_ops": [],
                    "set_pending_record_action": {
                        "record_type": "note",
                        "selected": selected,
                        "selected_list": selected_list,
                        "content": None,
                    },
                },
            )

        saved_records: list[dict[str, Any]] = []
        errors: list[str] = []
        for cand in selected_list:
            wine_id = str(cand.get("wine_id") or "").strip()
            try:
                record = self.records_db.add_record(
                    user=public_user,
                    record_type=record_type,
                    content=note_content if record_type == "note" else None,
                    wine_id=wine_id,
                )
                saved_records.append(record)
            except PublicRecordError as exc:
                errors.append(f"{self._format_wine_label(cand)}: {exc}")

        if not saved_records:
            return (
                "Не удалось сохранить отметки. " + ("; ".join(errors[:3]) if errors else ""),
                {"sql": None, "rows": 0, "model": self.model, "public_record_ops": []},
            )

        if len(saved_records) == 1:
            answer = self._build_record_saved_answer(record_type, selected_list[0], content=note_content)
        else:
            action = "Лайк сохранён" if record_type == "like" else "Заметка сохранена"
            lines = [f"{action} для {len(saved_records)} вин:"]
            for idx, cand in enumerate(selected_list[:5], 1):
                lines.append(f"{idx}. {self._format_wine_label(cand)}")
            if len(selected_list) > 5:
                lines.append(f"... и ещё {len(selected_list) - 5}")
            if record_type == "note":
                lines.append(f"Текст заметки: {note_content or ''}")
            if errors:
                lines.append("Часть записей не сохранена: " + "; ".join(errors[:2]))
            answer = "\n".join(lines)
        if duplicate_selected_count > 0:
            answer += (
                "\nПримечание: среди выбранных позиций были дубликаты одной и той же карточки, "
                "сохранены только уникальные записи."
            )

        return (
            answer,
            {
                "sql": None,
                "rows": 0,
                "model": self.model,
                "public_record_ops": [
                    {
                        "op": "contextual_add_public_record",
                        "ok": True,
                        "records": saved_records,
                        "errors": errors,
                    }
                ],
                "clear_pending_record_action": True,
            },
        )

    def _build_messages(self, user_text: str, history: list[dict[str, str]]) -> list[dict[str, Any]]:
        msgs: list[dict[str, Any]] = [{"role": "system", "content": self.system_prompt}]
        if history:
            tail = history[-self.max_history_messages :]
            for item in tail:
                role = item.get("role")
                content = item.get("content", "")
                if role in {"user", "assistant"} and content:
                    msgs.append({"role": role, "content": content})
        msgs.append({"role": "user", "content": user_text})
        return msgs

    def _tool_response(self, tool_call_args: str, include_full_rows: bool = False) -> dict[str, Any]:
        try:
            args = json.loads(tool_call_args or "{}")
        except json.JSONDecodeError:
            return {"ok": False, "error": "Невалидный JSON аргументов инструмента."}

        raw_query = str(args.get("query", "")).strip()
        if not raw_query:
            return {"ok": False, "error": "Пустой SQL query."}

        t0 = time.perf_counter()
        try:
            safe_sql, rows = self.db.execute_safe_query(raw_query, max_rows=self.max_sql_rows)
            limited_rows = rows[: self.max_rows_to_model]
            result = {
                "ok": True,
                "safe_sql": safe_sql,
                "row_count": len(rows),
                "rows": limited_rows,
                "truncated_for_model": len(rows) > len(limited_rows),
                "elapsed_ms": round((time.perf_counter() - t0) * 1000, 2),
            }
            if include_full_rows:
                result["rows_full"] = rows
            return result
        except SQLValidationError as exc:
            return {
                "ok": False,
                "error": f"SQL отклонен: {exc}",
                "elapsed_ms": round((time.perf_counter() - t0) * 1000, 2),
            }
        except Exception as exc:
            return {
                "ok": False,
                "error": f"Ошибка выполнения SQL: {exc}",
                "elapsed_ms": round((time.perf_counter() - t0) * 1000, 2),
            }

    def _tool_web_response(self, tool_call_args: str) -> dict[str, Any]:
        try:
            args = json.loads(tool_call_args or "{}")
        except json.JSONDecodeError:
            return {"ok": False, "error": "Невалидный JSON аргументов инструмента."}

        query = str(args.get("query", "")).strip()
        max_results = int(args.get("max_results", 5) or 5)
        t0 = time.perf_counter()
        result = search_wine_web(query=query, max_results=max_results)
        if isinstance(result, dict):
            result = dict(result)
            result["elapsed_ms"] = round((time.perf_counter() - t0) * 1000, 2)
        return result

    def _tool_public_add_response(
        self,
        tool_call_args: str,
        default_user: str | None = None,
    ) -> dict[str, Any]:
        if self.records_db is None:
            return {"ok": False, "error": "Public records DB не подключена."}

        try:
            args = json.loads(tool_call_args or "{}")
        except json.JSONDecodeError:
            return {"ok": False, "error": "Невалидный JSON аргументов инструмента."}

        wine_id = str(args.get("wine_id", "")).strip()
        record_type = str(args.get("record_type", "")).strip()
        content = args.get("content")
        user = str(args.get("user", "")).strip() or (str(default_user or "").strip() or None)

        try:
            record = self.records_db.add_record(
                user=user,
                record_type=record_type,
                content=str(content) if content is not None else None,
                wine_id=wine_id,
            )
            return {"ok": True, "record": record}
        except PublicRecordError as exc:
            return {"ok": False, "error": str(exc)}
        except Exception as exc:
            return {"ok": False, "error": f"Ошибка add_public_record: {exc}"}

    def _tool_public_list_response(self, tool_call_args: str) -> dict[str, Any]:
        if self.records_db is None:
            return {"ok": False, "error": "Public records DB не подключена."}

        try:
            args = json.loads(tool_call_args or "{}")
        except json.JSONDecodeError:
            return {"ok": False, "error": "Невалидный JSON аргументов инструмента."}

        wine_id = str(args.get("wine_id", "")).strip() or None
        record_type = str(args.get("record_type", "")).strip() or None
        user = str(args.get("user", "")).strip() or None

        try:
            rows = self.records_db.list_records(
                wine_id=wine_id,
                record_type=record_type,
                user=user,
            )
            return {"ok": True, "count": len(rows), "rows": rows}
        except PublicRecordError as exc:
            return {"ok": False, "error": str(exc)}
        except Exception as exc:
            return {"ok": False, "error": f"Ошибка list_public_records: {exc}"}

    def _tool_public_summary_response(self, tool_call_args: str) -> dict[str, Any]:
        if self.records_db is None:
            return {"ok": False, "error": "Public records DB не подключена."}

        try:
            args = json.loads(tool_call_args or "{}")
        except json.JSONDecodeError:
            return {"ok": False, "error": "Невалидный JSON аргументов инструмента."}

        wine_id = str(args.get("wine_id", "")).strip()
        if not wine_id:
            return {"ok": False, "error": "Пустой wine_id."}

        try:
            summary = self.records_db.get_wine_summary(wine_id=wine_id)
            return {"ok": True, "summary": summary}
        except PublicRecordError as exc:
            return {"ok": False, "error": str(exc)}
        except Exception as exc:
            return {"ok": False, "error": f"Ошибка get_wine_public_summary: {exc}"}

    def ask(
        self,
        user_text: str,
        history: list[dict[str, str]] | None = None,
        public_user: str | None = None,
        record_context: dict[str, Any] | None = None,
    ) -> tuple[str, dict[str, Any]]:
        started_at = time.perf_counter()
        selected_model = self._select_model_for_query(user_text)
        perf = {
            "selected_model": selected_model,
            "llm_rounds": 0,
            "llm_wait_ms_total": 0.0,
            "tool_calls_total": 0,
            "db_tool_calls": 0,
            "db_query_ms_total": 0.0,
            "web_tool_calls": 0,
            "web_query_ms_total": 0.0,
            "fallback_web_calls": 0,
            "fallback_web_ms_total": 0.0,
        }

        def attach_perf(meta: dict[str, Any]) -> dict[str, Any]:
            out = dict(meta or {})
            perf_total = dict(perf)
            perf_total["total_ms"] = round((time.perf_counter() - started_at) * 1000, 2)
            if perf_total["llm_rounds"] > 0:
                perf_total["llm_wait_ms_avg"] = round(
                    perf_total["llm_wait_ms_total"] / perf_total["llm_rounds"],
                    2,
                )
            out["perf"] = perf_total
            return out

        if self._is_capabilities_request(user_text):
            return (
                self.capabilities_text,
                attach_perf({
                    "sql": None,
                    "sql_queries": [],
                    "web_queries": [],
                    "web_results": [],
                    "web_tool_logs": [],
                    "rows": 0,
                    "model": selected_model,
                    "info_source": "SYSTEM_CAPABILITIES.md",
                }),
            )

        contextual_record = self._handle_contextual_record_intent(
            user_text=user_text,
            public_user=public_user,
            record_context=record_context,
        )
        if contextual_record is not None:
            answer, meta = contextual_record
            return self._sanitize_public_answer(answer), attach_perf(meta)

        my_records = self._handle_my_records_request(
            user_text=user_text,
            public_user=public_user,
        )
        if my_records is not None:
            answer, meta = my_records
            return self._sanitize_public_answer(answer), attach_perf(meta)

        if not self.client:
            return (
                "OpenAI недоступен: проверьте установку пакета `openai` и переменную OPENAI_API_KEY.",
                attach_perf({
                    "sql": None,
                    "sql_queries": [],
                    "web_queries": [],
                    "web_results": [],
                    "web_tool_logs": [],
                    "public_record_ops": [],
                    "rows": 0,
                    "model": selected_model,
                }),
            )

        messages = self._build_messages(user_text, history or [])
        last_sql = None
        last_rows = 0
        force_full = self._is_full_list_request(user_text)
        sql_queries: list[str] = []
        web_queries: list[str] = []
        web_results: list[dict[str, Any]] = []
        web_tool_logs: list[dict[str, Any]] = []
        public_record_ops: list[dict[str, Any]] = []
        latest_wine_candidates: list[dict[str, Any]] = []

        for _ in range(3):
            llm_t0 = time.perf_counter()
            completion = self.client.chat.completions.create(
                model=selected_model,
                messages=messages,
                tools=self.tools,
                temperature=0,
                max_completion_tokens=self.max_completion_tokens,
            )
            perf["llm_rounds"] += 1
            perf["llm_wait_ms_total"] += (time.perf_counter() - llm_t0) * 1000
            msg = completion.choices[0].message

            if not msg.tool_calls:
                answer = msg.content or "Не удалось сформировать ответ."
                if not web_results and (
                    self._is_price_or_availability_request(user_text)
                    or (last_rows == 0 and self._looks_like_wine_name_or_topic(user_text))
                ):
                    fallback_t0 = time.perf_counter()
                    fallback = search_wine_web(query=user_text, max_results=5)
                    perf["fallback_web_calls"] += 1
                    perf["fallback_web_ms_total"] += (time.perf_counter() - fallback_t0) * 1000
                    fallback_results = fallback.get("results") or []
                    fallback_log = {
                        "source": "fallback",
                        "ok": bool(fallback.get("ok")),
                        "engine": fallback.get("engine"),
                        "query": fallback.get("query") or user_text,
                        "search_query": fallback.get("search_query"),
                        "count": int(fallback.get("count") or 0),
                        "error": fallback.get("error"),
                        "providers_errors": fallback.get("providers_errors") or [],
                        "results": [
                            {
                                "title": item.get("title"),
                                "url": item.get("url"),
                            }
                            for item in (fallback_results if isinstance(fallback_results, list) else [])
                            if isinstance(item, dict)
                        ][:5],
                    }
                    web_tool_logs.append(fallback_log)
                    if fallback.get("ok"):
                        q = fallback.get("search_query") or fallback.get("query")
                        if q:
                            web_queries.append(str(q))
                        if isinstance(fallback_results, list):
                            web_results.extend([x for x in fallback_results if isinstance(x, dict)])

                answer = self._sanitize_public_answer(answer)
                return answer, attach_perf({
                    "sql": last_sql,
                    "sql_queries": sql_queries,
                    "web_queries": web_queries,
                    "web_results": self._dedupe_web_results(web_results)[:10],
                    "web_tool_logs": web_tool_logs,
                    "public_record_ops": public_record_ops,
                    "wine_context_candidates": latest_wine_candidates,
                    "rows": last_rows,
                    "model": selected_model,
                })

            messages.append(msg)
            for tool_call in msg.tool_calls:
                perf["tool_calls_total"] += 1
                if tool_call.function.name == "execute_sql":
                    tool_result = self._tool_response(
                        tool_call.function.arguments,
                        include_full_rows=force_full,
                    )
                    perf["db_tool_calls"] += 1
                    perf["db_query_ms_total"] += float(tool_result.get("elapsed_ms") or 0.0)
                    if tool_result.get("ok"):
                        last_sql = tool_result.get("safe_sql")
                        last_rows = int(tool_result.get("row_count", 0))
                        source_rows = tool_result.get("rows_full") if force_full else tool_result.get("rows")
                        if isinstance(source_rows, list):
                            extracted = self._extract_wine_candidates_from_rows(
                                [r for r in source_rows if isinstance(r, dict)]
                            )
                            if extracted:
                                latest_wine_candidates = extracted
                        if last_sql:
                            sql_queries.append(str(last_sql))
                        if force_full:
                            rows_full = tool_result.get("rows_full", [])
                            answer = self._format_full_list_answer(rows_full)
                            answer = self._sanitize_public_answer(answer)
                            return answer, attach_perf({
                                "sql": last_sql,
                                "sql_queries": sql_queries,
                                "web_queries": web_queries,
                                "web_results": self._dedupe_web_results(web_results)[:10],
                                "web_tool_logs": web_tool_logs,
                                "public_record_ops": public_record_ops,
                                "wine_context_candidates": latest_wine_candidates,
                                "rows": last_rows,
                                "model": selected_model,
                            })
                elif tool_call.function.name == "search_web":
                    tool_result = self._tool_web_response(tool_call.function.arguments)
                    perf["web_tool_calls"] += 1
                    perf["web_query_ms_total"] += float(tool_result.get("elapsed_ms") or 0.0)
                    q = None
                    tool_results = tool_result.get("results") or []
                    web_tool_logs.append(
                        {
                            "source": "tool_call",
                            "ok": bool(tool_result.get("ok")),
                            "engine": tool_result.get("engine"),
                            "query": tool_result.get("query"),
                            "search_query": tool_result.get("search_query"),
                            "count": int(tool_result.get("count") or 0),
                            "error": tool_result.get("error"),
                            "providers_errors": tool_result.get("providers_errors") or [],
                            "results": [
                                {
                                    "title": item.get("title"),
                                    "url": item.get("url"),
                                }
                                for item in (tool_results if isinstance(tool_results, list) else [])
                                if isinstance(item, dict)
                            ][:5],
                        }
                    )
                    if tool_result.get("ok"):
                        q = tool_result.get("search_query") or tool_result.get("query")
                        if isinstance(tool_results, list):
                            web_results.extend([x for x in tool_results if isinstance(x, dict)])
                    if q:
                        web_queries.append(str(q))
                elif tool_call.function.name == "add_public_record":
                    tool_result = self._tool_public_add_response(
                        tool_call.function.arguments,
                        default_user=public_user,
                    )
                    public_record_ops.append(
                        {
                            "op": "add_public_record",
                            "ok": bool(tool_result.get("ok")),
                            "error": tool_result.get("error"),
                            "record": tool_result.get("record"),
                        }
                    )
                elif tool_call.function.name == "list_public_records":
                    tool_result = self._tool_public_list_response(tool_call.function.arguments)
                    public_record_ops.append(
                        {
                            "op": "list_public_records",
                            "ok": bool(tool_result.get("ok")),
                            "error": tool_result.get("error"),
                            "count": int(tool_result.get("count") or 0),
                        }
                    )
                elif tool_call.function.name == "get_wine_public_summary":
                    tool_result = self._tool_public_summary_response(tool_call.function.arguments)
                    public_record_ops.append(
                        {
                            "op": "get_wine_public_summary",
                            "ok": bool(tool_result.get("ok")),
                            "error": tool_result.get("error"),
                            "summary": tool_result.get("summary"),
                        }
                    )
                else:
                    tool_result = {"ok": False, "error": f"Неизвестный инструмент: {tool_call.function.name}"}
                messages.append(
                    {
                        "role": "tool",
                        "tool_call_id": tool_call.id,
                        "content": json.dumps(tool_result, ensure_ascii=False),
                    }
                )

        return (
            "Не удалось завершить обработку запроса за допустимое число шагов.",
            attach_perf({
                "sql": last_sql,
                "sql_queries": sql_queries,
                "web_queries": web_queries,
                "web_results": self._dedupe_web_results(web_results)[:10],
                "web_tool_logs": web_tool_logs,
                "public_record_ops": public_record_ops,
                "wine_context_candidates": latest_wine_candidates,
                "rows": last_rows,
                "model": selected_model,
            }),
        )
