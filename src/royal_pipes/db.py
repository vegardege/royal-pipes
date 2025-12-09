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

ODDS_TABLE = """
    CREATE TABLE IF NOT EXISTS odds (
        word TEXT PRIMARY KEY NOT NULL,
        odds REAL NOT NULL
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


def ensure_odds_table(db_path: str | Path) -> None:
    """Ensure the odds table exists with the correct schema.

    Args:
        db_path: Path to the SQLite database file

    Table schema:
        word TEXT - The word/phrase being bet on
        odds REAL - The betting odds for this word

    Primary key: word
    """
    with get_connection(db_path) as conn:
        conn.execute(ODDS_TABLE)
        conn.commit()


def replace_odds(db_path: str | Path, odds: dict[str, float]) -> None:
    """Replace all betting odds in the database.

    Args:
        db_path: Path to the SQLite database file
        odds: Dictionary mapping words to their odds

    This atomically replaces the entire table contents.
    """
    ensure_odds_table(db_path)

    with get_connection(db_path) as conn:
        # Atomic replacement: delete all, then insert
        conn.execute("DELETE FROM odds")
        conn.executemany(
            "INSERT INTO odds (word, odds) VALUES (?, ?)",
            odds.items(),
        )
        conn.commit()
