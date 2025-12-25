import dagster as dg

from royal_pipes import db
from royal_pipes.models import (
    ComparisonResult,
    CorpusWordWithFrequency,
    OddsCount,
    Speech,
    SpeechNer,
    WordCount,
)


class AnalyticsDB(dg.ConfigurableResource):
    """SQLite database resource for analytics data.

    Wraps pure Python db module with Dagster resource pattern.
    Defaults to XDG_DATA_HOME/royal-pipes/analytics.db.
    """

    db_path: str

    def get_connection(self):
        """Get a connection to the analytics database."""
        return db.get_connection(self.db_path)

    def ensure_word_count_table(self) -> None:
        """Ensure the word_count table exists."""
        db.ensure_word_count_table(self.db_path)

    def replace_word_count(self, word_counts: list[WordCount]) -> None:
        """Replace all word counts in the database."""
        db.replace_word_count(self.db_path, word_counts)

    def ensure_odds_table(self) -> None:
        """Ensure the odds table exists."""
        db.ensure_odds_table(self.db_path)

    def replace_odds(self, odds: dict[str, float]) -> None:
        """Replace all betting odds in the database."""
        db.replace_odds(self.db_path, odds)

    def ensure_odds_count_table(self) -> None:
        """Ensure the odds_count table exists."""
        db.ensure_odds_count_table(self.db_path)

    def replace_odds_counts(self, odds_counts: list[OddsCount]) -> None:
        """Replace all odds counts in the database."""
        db.replace_odds_counts(self.db_path, odds_counts)

    def ensure_speech_table(self) -> None:
        """Ensure the speech table exists."""
        db.ensure_speech_table(self.db_path)

    def replace_speech(self, speeches: list[Speech]) -> None:
        """Replace all speeches in the database."""
        db.replace_speech(self.db_path, speeches)

    def ensure_corpus_table(self) -> None:
        """Ensure the corpus table exists."""
        db.ensure_corpus_table(self.db_path)

    def replace_corpus(self, corpus: list[CorpusWordWithFrequency]) -> None:
        """Replace all corpus data in the database."""
        db.replace_corpus(self.db_path, corpus)

    def ensure_wlo_tables(self) -> None:
        """Ensure the WLO comparison tables exist."""
        db.ensure_wlo_tables(self.db_path)

    def replace_wlo_comparisons(
        self,
        comparison_results: list[ComparisonResult],
    ) -> None:
        """Replace all WLO comparison data in the database."""
        db.replace_wlo_comparisons(self.db_path, comparison_results)

    def ensure_ner_tables(self) -> None:
        """Ensure all NER tables exist."""
        db.ensure_ner_tables(self.db_path)

    def replace_ner_data(self, ner_results: list[SpeechNer]) -> None:
        """Replace all NER data in the database."""
        db.replace_ner_data(self.db_path, ner_results)
