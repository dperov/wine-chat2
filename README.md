# wine-chat2

Новая версия приложения для чата по российским винам на базе текущей wide-схемы:
- SQLite: `wine_product.sqlite`
- Table: `wine_cards_wide`

## Возможности
- Tool-calling через OpenAI для генерации SQL.
- Tool-calling web-поиска по винной теме (наличие в продаже, цены на полке, магазины, новости).
- Безопасное исполнение SQL:
  - только `SELECT`/`WITH`
  - запрет DDL/DML/PRAGMA
  - ограничение выдачи `LIMIT` сверху
  - `LIKE` выполняется регистронезависимо для кириллицы и латиницы (через внутренний `RU_LIKE`)
- Сессионная история диалога (по пользователю).
- UI с санитизацией Markdown (DOMPurify).
- В prompt передаются схема и справочники из БД.

## Установка
```powershell
cd wine-chat2
python -m venv .venv
.\.venv\Scripts\activate
pip install -r requirements.txt
```

## Настройка `.env`
```properties
OPENAI_API_KEY=your_key
OPENAI_MODEL_FAST=gpt-4.1-mini
OPENAI_MODEL_COMPLEX=gpt-4.1
OPENAI_MODEL=
OPENAI_WEB_MODEL=gpt-4.1
OPENAI_MAX_HISTORY_MESSAGES=8
OPENAI_MAX_COMPLETION_TOKENS=1200
WINE_DB_PATH=..\wine_product.sqlite
WINE_TABLE=wine_cards_wide
WINE_USER_DB_PATH=..\wine_social.sqlite
EXTERNAL_USER_ID_HEADER=X-External-User-Id
FLASK_SECRET_KEY=change-me
PORT=5000
WEB_SEARCH_CONTEXT_SIZE=medium
WEB_SEARCH_COUNTRY=RU
WEB_SEARCH_CITY=moscow
WEB_SEARCH_ALLOWED_DOMAINS=
WINE_WEB_TOOL_ENABLED=0
WINE_APP_DEBUG=0
WINE_PERF_LOG_ENABLED=1
WINE_LOG_DIR=logs
WINE_PERF_LOG_PATH=
```

`WINE_DB_PATH` по умолчанию указывает на `../wine_product.sqlite`.
`WINE_USER_DB_PATH` по умолчанию указывает на `../wine_social.sqlite`.
`WINE_APP_DEBUG` по умолчанию `0` (debug-режим Flask выключен).
`WINE_PERF_LOG_ENABLED` по умолчанию `1` (рабочий perf-лог включен, человекочитаемый текстовый формат).
`WINE_WEB_TOOL_ENABLED` по умолчанию `0` (web tool отключен).
По умолчанию ассистент использует быструю модель `gpt-4.1-mini`, а для сложных запросов переключается на `OPENAI_MODEL_COMPLEX` (`gpt-4.1`).
История для LLM по умолчанию ограничена `8` сообщениями, а `OPENAI_MAX_COMPLETION_TOKENS` по умолчанию `1200`.

## Запуск
```powershell
python app.py
```

Откройте `http://127.0.0.1:5000`.

## Health-check
`GET /health` возвращает состояние подключения к БД и базовую информацию о схеме.
`GET /capabilities` возвращает краткую сводку возможностей системы (из `SYSTEM_CAPABILITIES.md`).
`GET /debug/perf/tail?lines=100` возвращает tail performance-лога в `text/plain`.
`GET /debug/perf/tail?lines=100&format=json` возвращает тот же tail в JSON.

## Публичные записи пользователей
Добавлена write-база с таблицей `public_records` (лайки/заметки).
Таблица создается автоматически при старте приложения (`wine-chat2/public_records.sql`).

Формат записи:
- `user` — имя пользователя (или `ext:<id>` если имя не передано, но пришел внешний `user_id`)
- `record_type` — `like` или `note`
- `content` — содержание отметки (`note`: обязательный текст, `like`: по умолчанию `1`)
- `wine_id` — идентификатор вина (`card_key` или `url` из `wine_cards_wide`)

Важно: в чате пользователю не нужно знать `wine_id`/`url`.
Можно указывать название вина или позицию из последнего списка (`позиция 2`, `номер 3`, `1`).
Если найдено несколько совпадений, ассистент покажет список кандидатов и попросит выбрать номер.

### API: создать запись
`POST /api/records`

Пример body:
```json
{
  "user": "Алексей",
  "record_type": "note",
  "content": "Понравилось, мягкие танины",
  "wine_id": "683"
}
```

Внешний `user_id` можно передать:
- через заголовок `X-External-User-Id` (или другой, если изменен `EXTERNAL_USER_ID_HEADER`)
- либо query/body `external_user_id`

Если `user` не передан, будет использовано:
1) `ext:<external_user_id>` — если внешний id есть
2) `Гость` — иначе

### API: список записей
`GET /api/records?wine_id=683&record_type=note&user=Алексей`

Параметры фильтрации опциональны:
- `wine_id`
- `record_type`
- `user`

### API: записи и summary по вину
`GET /api/records/by-wine/<wine_id>`

Возвращает:
- `summary` (`like_count`, `note_count`)
- полный список записей по вину

Шаблоны пользовательских команд (через чат) и краткая сводка функций лежат в `wine-chat2/SYSTEM_CAPABILITIES.md`.
Пользователь может запросить в чате: `Покажи возможности`.

## Консольный режим (для тестирования)
```powershell
python console_chat.py
```

Команды в консоли:
- `/exit` или `/quit` — выход
- `/clear` — очистить историю диалога
- `/sql on` / `/sql off` — включить/выключить показ SQL и meta
- `/csv on` / `/csv off` — включить/выключить автосохранение результатов SQL в CSV
- `/csv dir <path>` — задать папку для CSV (по умолчанию `wine-chat2/exports`)

При включенном CSV-режиме после каждого запроса, где есть SQL, результат сохраняется в отдельный файл
`query_result_YYYYMMDD_HHMMSS.csv` в кодировке `utf-8-sig` (подходит для Excel).
Если ассистент выполнил несколько SQL-запросов, сохраняется отдельный CSV для каждого:
`query_result_q01_...csv`, `query_result_q02_...csv`, и т.д.

## Примечание
Если не задан `OPENAI_API_KEY`, приложение запускается, но чат вернет сообщение о необходимости ключа.
Для web-поиска нужен доступ в интернет.
Web-поиск выполняется через встроенный инструмент OpenAI `web_search` (Responses API).
Если источники не найдены, ассистент попросит уточнить запрос (например, добавить точное название вина, регион или магазин).
Опционально можно ограничить web-поиск списком доменов через `WEB_SEARCH_ALLOWED_DOMAINS` (через запятую).
