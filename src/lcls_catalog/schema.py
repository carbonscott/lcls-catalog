"""SQLite schema definitions for the LCLS data catalog."""

SCHEMA_VERSION = 1

CREATE_FILES_TABLE = """
CREATE TABLE IF NOT EXISTS files (
    id INTEGER PRIMARY KEY,
    path TEXT NOT NULL,
    parent_path TEXT NOT NULL,
    filename TEXT NOT NULL,
    size INTEGER,
    mtime INTEGER,
    owner TEXT,
    group_name TEXT,
    permissions INTEGER,
    checksum TEXT,
    archive_uri TEXT,
    experiment TEXT,
    run INTEGER,
    purge_date TEXT,
    UNIQUE(path, purge_date)
)
"""

CREATE_INDEXES = [
    "CREATE INDEX IF NOT EXISTS idx_parent ON files(parent_path)",
    "CREATE INDEX IF NOT EXISTS idx_experiment ON files(experiment)",
    "CREATE INDEX IF NOT EXISTS idx_filename ON files(filename)",
    "CREATE INDEX IF NOT EXISTS idx_mtime ON files(mtime)",
    "CREATE INDEX IF NOT EXISTS idx_size ON files(size)",
]

CREATE_METADATA_TABLE = """
CREATE TABLE IF NOT EXISTS metadata (
    key TEXT PRIMARY KEY,
    value TEXT
)
"""


def init_schema(conn):
    """Initialize the database schema."""
    cursor = conn.cursor()
    cursor.execute(CREATE_FILES_TABLE)
    for index_sql in CREATE_INDEXES:
        cursor.execute(index_sql)
    cursor.execute(CREATE_METADATA_TABLE)
    cursor.execute(
        "INSERT OR REPLACE INTO metadata (key, value) VALUES (?, ?)",
        ("schema_version", str(SCHEMA_VERSION)),
    )
    conn.commit()
