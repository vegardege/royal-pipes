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

ODDS_COUNT_TABLE = """
    CREATE TABLE IF NOT EXISTS odds_count (
        year INTEGER NOT NULL,
        word TEXT NOT NULL,
        count INTEGER NOT NULL,
        PRIMARY KEY (year, word)
    )
"""

SPEECHES_TABLE = """
    CREATE TABLE IF NOT EXISTS speeches (
        year INTEGER PRIMARY KEY NOT NULL,
        word_count INTEGER NOT NULL,
        monarch TEXT NOT NULL
    )
"""

CORPUS_TABLE = """
    CREATE TABLE IF NOT EXISTS corpus (
        word TEXT PRIMARY KEY NOT NULL,
        count INTEGER NOT NULL
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


def ensure_odds_count_table(db_path: str | Path) -> None:
    """Ensure the odds_count table exists with the correct schema.

    Args:
        db_path: Path to the SQLite database file

    Table schema:
        year INTEGER - The year of the speech
        word TEXT - The odds word/phrase being counted
        count INTEGER - Number of occurrences in that year's speech

    Primary key: (year, word)
    """
    with get_connection(db_path) as conn:
        conn.execute(ODDS_COUNT_TABLE)
        conn.commit()


def replace_odds_counts(
    db_path: str | Path, odds_counts: list[tuple[int, str, int]]
) -> None:
    """Replace all odds counts in the database.

    Args:
        db_path: Path to the SQLite database file
        odds_counts: List of (year, word, count) tuples

    This atomically replaces the entire table contents.
    """
    ensure_odds_count_table(db_path)

    with get_connection(db_path) as conn:
        # Atomic replacement: delete all, then insert
        conn.execute("DELETE FROM odds_count")
        conn.executemany(
            "INSERT INTO odds_count (year, word, count) VALUES (?, ?, ?)",
            odds_counts,
        )
        conn.commit()


def ensure_speeches_table(db_path: str | Path) -> None:
    """Ensure the speeches table exists with the correct schema.

    Args:
        db_path: Path to the SQLite database file

    Table schema:
        year INTEGER - The year of the speech
        word_count INTEGER - Number of words in the speech
        monarch TEXT - Name of the monarch who delivered the speech

    Primary key: year
    """
    with get_connection(db_path) as conn:
        conn.execute(SPEECHES_TABLE)
        conn.commit()


def replace_speeches(
    db_path: str | Path, speeches: list[tuple[int, int, str]]
) -> None:
    """Replace all speeches in the database.

    Args:
        db_path: Path to the SQLite database file
        speeches: List of (year, word_count, monarch) tuples

    This atomically replaces the entire table contents.
    """
    ensure_speeches_table(db_path)

    with get_connection(db_path) as conn:
        # Atomic replacement: delete all, then insert
        conn.execute("DELETE FROM speeches")
        conn.executemany(
            "INSERT INTO speeches (year, word_count, monarch) VALUES (?, ?, ?)",
            speeches,
        )
        conn.commit()


def ensure_corpus_table(db_path: str | Path) -> None:
    """Ensure the corpus table exists with the correct schema.

    Args:
        db_path: Path to the SQLite database file

    Table schema:
        word TEXT - The word from the corpus
        count INTEGER - Frequency count of the word in the corpus

    Primary key: word
    """
    with get_connection(db_path) as conn:
        conn.execute(CORPUS_TABLE)
        conn.commit()


def replace_corpus(db_path: str | Path, corpus: list[tuple[str, int]]) -> None:
    """Replace all corpus data in the database.

    Args:
        db_path: Path to the SQLite database file
        corpus: List of (word, count) tuples from the Leipzig corpus

    This atomically replaces the entire table contents.
    """
    ensure_corpus_table(db_path)

    with get_connection(db_path) as conn:
        # Atomic replacement: delete all, then insert
        conn.execute("DELETE FROM corpus")
        conn.executemany(
            "INSERT INTO corpus (word, count) VALUES (?, ?)",
            corpus,
        )
        conn.commit()
