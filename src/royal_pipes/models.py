"""Data models for the Royal Pipes project.

This module contains dataclasses representing the core domain objects.
"""

from dataclasses import dataclass


@dataclass
class Monarch:
    """A Danish monarch with their reign period.

    Attributes:
        name: The monarch's name (e.g., "Margrethe II")
        start_year: Year the reign began
        end_year: Year the reign ended (None if currently reigning)
    """
    name: str
    start_year: int
    end_year: int | None


@dataclass
class CorpusWord:
    """A word from a text corpus with its count.

    Attributes:
        word: The word (lowercase)
        count: Number of occurrences in the corpus
    """
    word: str
    count: int


@dataclass
class CorpusWordWithFrequency:
    """A word from a text corpus with count and frequency.

    Attributes:
        word: The word (lowercase)
        count: Number of occurrences in the corpus
        frequency: Relative frequency (count / total_words)
    """
    word: str
    count: int
    frequency: float


@dataclass
class WordCount:
    """Word count for a specific year's speech.

    Attributes:
        year: The speech year
        word: The word (lowercase)
        count: Number of occurrences in that year's speech
        is_stopword: Whether this is a stopword
    """
    year: int
    word: str
    count: int
    is_stopword: bool


@dataclass
class OddsCount:
    """Count of a betting odds word in a specific year's speech.

    Attributes:
        year: The speech year
        word: The odds word (original form from betting site)
        count: Number of occurrences (including variants)
    """
    year: int
    word: str
    count: int


@dataclass
class Speech:
    """Speech metadata linking a year to a monarch.

    Attributes:
        year: The speech year
        monarch: The monarch who gave the speech
    """
    year: int
    monarch: str


@dataclass
class WeightedLogOddsResult:
    """Result of a weighted log-odds comparison for a single word.

    Attributes:
        word: The word being compared
        wlo_score: The weighted log-odds score (higher = more distinctive to focal)
        focal_count: Raw count of word in focal corpus
        background_count: Raw count of word in background corpus
        focal_rate: Usage rate in focal corpus (count / total_words)
        background_rate: Usage rate in background corpus
        z_score: Z-score of the weighted log-odds (for significance testing)
    """
    word: str
    wlo_score: float
    focal_count: int
    background_count: int
    focal_rate: float
    background_rate: float
    z_score: float


@dataclass
class WLOComparison:
    """Metadata for a weighted log-odds comparison.

    Attributes:
        comparison_type: Type of comparison ('monarch', 'decade')
        focal_value: The focal group being compared (e.g., 'Margrethe', '2000s')
        background_type: What the focal is compared against (e.g., 'other_monarchs')
        alpha: Dirichlet prior strength parameter used
        focal_corpus_size: Total word count in focal corpus
        background_corpus_size: Total word count in background corpus
    """
    comparison_type: str
    focal_value: str
    background_type: str
    alpha: float
    focal_corpus_size: int
    background_corpus_size: int


@dataclass
class ComparisonResult:
    """Result of a weighted log-odds comparison.

    Combines comparison metadata with the list of top distinctive words.

    Attributes:
        comparison: Metadata about the comparison
        top_words: List of top distinctive words with their scores
    """
    comparison: WLOComparison
    top_words: list[WeightedLogOddsResult]
