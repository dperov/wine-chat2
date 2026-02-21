CREATE TABLE IF NOT EXISTS public_records (
    id INTEGER PRIMARY KEY AUTOINCREMENT,
    user TEXT,
    record_type TEXT NOT NULL CHECK (record_type IN ('like', 'note')),
    content TEXT,
    wine_id TEXT NOT NULL,
    created_at TEXT NOT NULL DEFAULT (datetime('now')),
    updated_at TEXT NOT NULL DEFAULT (datetime('now'))
);

CREATE INDEX IF NOT EXISTS idx_public_records_wine_id
    ON public_records (wine_id);

CREATE INDEX IF NOT EXISTS idx_public_records_record_type
    ON public_records (record_type);

CREATE INDEX IF NOT EXISTS idx_public_records_user
    ON public_records (user);
