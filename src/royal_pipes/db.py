import sqlite3
from pathlib import Path

WORD_COUNTS_TABLE = """
    CREATE TABLE IF NOT EXISTS word_counts (
        year INTEGER NOT NULL,
        word TEXT NOT NULL,
        count INTEGER NOT NULL,
        PRIMARY KEY (year, word)
    )
"""


def get_connection(db_path: str | Path) -> sqlite3.Connection:
    """Get a connection to the database.

    Args:
        db_path: Path to the SQLite database file

    Returns:
        SQLite connection
    """
    path = Path(db_path)
    path.parent.mkdir(parents=True, exist_ok=True)
    return sqlite3.connect(path)


def ensure_word_counts_table(db_path: str | Path) -> None:
    """Ensure the word_counts table exists with the correct schema.

    Args:
        db_path: Path to the SQLite database file

    Table schema:
        year INTEGER - The year of the speech
        word TEXT - The word (lowercased, cleaned)
        count INTEGER - Number of occurrences in that year's speech

    Primary key: (year, word)
    """
    with get_connection(db_path) as conn:
        conn.execute(WORD_COUNTS_TABLE)
        conn.commit()


def replace_word_counts(
    db_path: str | Path, word_counts: list[tuple[int, str, int]]
) -> None:
    """Replace all word counts in the database.

    Args:
        db_path: Path to the SQLite database file
        word_counts: List of (year, word, count) tuples

    This atomically replaces the entire table contents.
    """
    ensure_word_counts_table(db_path)

    with get_connection(db_path) as conn:
        # Atomic replacement: delete all, then insert
        conn.execute("DELETE FROM word_counts")
        conn.executemany(
            "INSERT INTO word_counts (year, word, count) VALUES (?, ?, ?)",
            word_counts,
        )
        conn.commit()
