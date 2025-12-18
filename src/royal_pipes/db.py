import sqlite3
from pathlib import Path

SPEECH_TABLE = """
    CREATE TABLE IF NOT EXISTS speech (
        year INTEGER PRIMARY KEY NOT NULL,
        monarch TEXT NOT NULL
    )
"""

WORD_COUNT_TABLE = """
    CREATE TABLE IF NOT EXISTS word_count (
        year INTEGER NOT NULL,
        word TEXT NOT NULL,
        count INTEGER NOT NULL,
        is_stopword BOOLEAN NOT NULL,
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

CORPUS_TABLE = """
    CREATE TABLE IF NOT EXISTS corpus (
        word TEXT PRIMARY KEY NOT NULL,
        count INTEGER NOT NULL,
        frequency REAL NOT NULL
    )
"""

WLO_COMPARISONS_TABLE = """
    CREATE TABLE IF NOT EXISTS wlo_comparisons (
        comparison_id TEXT PRIMARY KEY NOT NULL,
        comparison_type TEXT NOT NULL,
        focal_value TEXT NOT NULL,
        background_type TEXT NOT NULL,
        alpha REAL NOT NULL,
        focal_corpus_size INTEGER NOT NULL,
        background_corpus_size INTEGER NOT NULL
    )
"""

WLO_WORDS_TABLE = """
    CREATE TABLE IF NOT EXISTS wlo_words (
        comparison_id TEXT NOT NULL,
        rank INTEGER NOT NULL,
        word TEXT NOT NULL,
        wlo_score REAL NOT NULL,
        focal_count INTEGER NOT NULL,
        background_count INTEGER NOT NULL,
        focal_rate REAL NOT NULL,
        background_rate REAL NOT NULL,
        z_score REAL NOT NULL,
        PRIMARY KEY (comparison_id, rank),
        FOREIGN KEY (comparison_id) REFERENCES wlo_comparisons(comparison_id)
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


def ensure_word_count_table(db_path: str | Path) -> None:
    """Ensure the word_count table exists with the correct schema.

    Args:
        db_path: Path to the SQLite database file
    """
    with get_connection(db_path) as conn:
        conn.execute(WORD_COUNT_TABLE)
        conn.commit()


def replace_word_count(
    db_path: str | Path, word_counts: list[tuple[int, str, int, bool]]
) -> None:
    """Replace all word counts in the database.

    Args:
        db_path: Path to the SQLite database file
        word_counts: List of (year, word, count) tuples

    This atomically replaces the entire table contents.
    """
    ensure_word_count_table(db_path)

    with get_connection(db_path) as conn:
        conn.execute("DELETE FROM word_count")
        conn.executemany(
            "INSERT INTO word_count (year, word, count, is_stopword) VALUES (?, ?, ?, ?)",
            word_counts,
        )
        conn.commit()


def ensure_odds_table(db_path: str | Path) -> None:
    """Ensure the odds table exists with the correct schema.

    Args:
        db_path: Path to the SQLite database file
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
        conn.execute("DELETE FROM odds_count")
        conn.executemany(
            "INSERT INTO odds_count (year, word, count) VALUES (?, ?, ?)",
            odds_counts,
        )
        conn.commit()


def ensure_speech_table(db_path: str | Path) -> None:
    """Ensure the speech table exists with the correct schema.

    Args:
        db_path: Path to the SQLite database file
    """
    with get_connection(db_path) as conn:
        conn.execute(SPEECH_TABLE)
        conn.commit()


def replace_speech(db_path: str | Path, speeches: list[tuple[int, str]]) -> None:
    """Replace all speeches in the database.

    Args:
        db_path: Path to the SQLite database file
        speeches: List of (year, monarch) tuples

    This atomically replaces the entire table contents.
    """
    ensure_speech_table(db_path)

    with get_connection(db_path) as conn:
        conn.execute("DELETE FROM speech")
        conn.executemany(
            "INSERT INTO speech (year, monarch) VALUES (?, ?)",
            speeches,
        )
        conn.commit()


def ensure_corpus_table(db_path: str | Path) -> None:
    """Ensure the corpus table exists with the correct schema.

    Args:
        db_path: Path to the SQLite database file
    """
    with get_connection(db_path) as conn:
        conn.execute(CORPUS_TABLE)
        conn.commit()


def replace_corpus(db_path: str | Path, corpus: list[tuple[str, int, float]]) -> None:
    """Replace all corpus data in the database.

    Args:
        db_path: Path to the SQLite database file
        corpus: List of (word, count, frequency) tuples from the Leipzig corpus

    This atomically replaces the entire table contents.
    """
    ensure_corpus_table(db_path)

    with get_connection(db_path) as conn:
        conn.execute("DELETE FROM corpus")
        conn.executemany(
            "INSERT INTO corpus (word, count, frequency) VALUES (?, ?, ?)",
            corpus,
        )
        conn.commit()


def ensure_wlo_tables(db_path: str | Path) -> None:
    """Ensure the WLO comparison tables exist with the correct schema.

    Args:
        db_path: Path to the SQLite database file
    """
    with get_connection(db_path) as conn:
        conn.execute(WLO_COMPARISONS_TABLE)
        conn.execute(WLO_WORDS_TABLE)
        conn.commit()


def replace_wlo_comparisons(
    db_path: str | Path,
    comparisons: list[tuple[str, str, str, str, float, int, int]],
    words: list[tuple[str, int, str, float, int, int, float, float, float]],
) -> None:
    """Replace all WLO comparison data in the database.

    Args:
        db_path: Path to the SQLite database file
        comparisons: List of (comparison_id, comparison_type, focal_value,
                     background_type, alpha, focal_corpus_size, background_corpus_size) tuples
        words: List of (comparison_id, rank, word, wlo_score, focal_count,
               background_count, focal_rate, background_rate, z_score) tuples

    This atomically replaces the entire table contents. The comparison_id should be
    a human-readable string like "monarch:Margrethe" or "decade:1990s".
    """
    ensure_wlo_tables(db_path)

    with get_connection(db_path) as conn:
        # Clear existing data
        conn.execute("DELETE FROM wlo_words")
        conn.execute("DELETE FROM wlo_comparisons")

        # Insert comparisons
        conn.executemany(
            """INSERT INTO wlo_comparisons
               (comparison_id, comparison_type, focal_value, background_type, alpha,
                focal_corpus_size, background_corpus_size)
               VALUES (?, ?, ?, ?, ?, ?, ?)""",
            comparisons,
        )

        # Insert words
        conn.executemany(
            """INSERT INTO wlo_words
               (comparison_id, rank, word, wlo_score, focal_count,
                background_count, focal_rate, background_rate, z_score)
               VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?)""",
            words,
        )

        conn.commit()
