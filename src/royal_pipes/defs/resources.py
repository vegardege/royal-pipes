import dagster as dg

from royal_pipes import db


class AnalyticsDB(dg.ConfigurableResource):
    """SQLite database resource for analytics data.

    Wraps pure Python db module with Dagster resource pattern.
    Defaults to XDG_DATA_HOME/royal-pipes/analytics.db.
    """

    db_path: str

    def get_connection(self):
        """Get a connection to the analytics database."""
        return db.get_connection(self.db_path)

    def ensure_word_counts_table(self) -> None:
        """Ensure the word_counts table exists."""
        db.ensure_word_counts_table(self.db_path)

    def replace_word_counts(self, word_counts: list[tuple[int, str, int]]) -> None:
        """Replace all word counts in the database."""
        db.replace_word_counts(self.db_path, word_counts)

    def ensure_odds_table(self) -> None:
        """Ensure the odds table exists."""
        db.ensure_odds_table(self.db_path)

    def replace_odds(self, odds: dict[str, float]) -> None:
        """Replace all betting odds in the database."""
        db.replace_odds(self.db_path, odds)

    def ensure_odds_count_table(self) -> None:
        """Ensure the odds_count table exists."""
        db.ensure_odds_count_table(self.db_path)

    def replace_odds_counts(self, odds_counts: list[tuple[int, str, int]]) -> None:
        """Replace all odds counts in the database."""
        db.replace_odds_counts(self.db_path, odds_counts)
