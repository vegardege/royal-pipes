"""Statistical functions for text analysis.

This module provides reusable statistical functions for comparing text corpora,
with no project-specific dependencies.
"""

import math
from dataclasses import dataclass


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


def weighted_log_odds(
    focal_counts: dict[str, int],
    background_counts: dict[str, int],
    alpha: float = 0.01,
    min_count: int = 0,
) -> list[WeightedLogOddsResult]:
    """Calculate weighted log-odds with informative Dirichlet prior.

    Implements the method from Monroe, Colaresi, and Quinn (2008):
    "Fightin' Words: Lexical Feature Selection and Evaluation for Identifying
    the Content of Political Conflict"

    This method compares word usage between two corpora (focal vs background)
    while accounting for overall word frequencies. It prevents rare words from
    dominating the results by using a Dirichlet prior based on overall usage.

    Args:
        focal_counts: Word counts in the focal corpus {word: count}
        background_counts: Word counts in the background corpus {word: count}
        alpha: Prior strength parameter (typically 0.01 for small corpora)
        min_count: Minimum total count across both corpora to include a word

    Returns:
        List of WeightedLogOddsResult objects, one per word that meets min_count
        threshold. Results are NOT sorted - caller should sort by wlo_score.

    Example:
        >>> focal = {"king": 50, "queen": 30, "the": 200}
        >>> background = {"king": 10, "queen": 5, "the": 300}
        >>> results = weighted_log_odds(focal, background, alpha=0.01)
        >>> sorted_results = sorted(results, key=lambda r: r.wlo_score, reverse=True)
        >>> print(sorted_results[0].word)  # Most distinctive to focal
        'queen'
    """
    # Get all words appearing in either corpus
    all_words = set(focal_counts.keys()) | set(background_counts.keys())

    # Calculate total words in each corpus
    n_focal = sum(focal_counts.values())
    n_background = sum(background_counts.values())
    n_total = n_focal + n_background

    if n_total == 0:
        return []

    results = []

    for word in all_words:
        y_focal = focal_counts.get(word, 0)
        y_background = background_counts.get(word, 0)

        # Apply minimum count filter
        if y_focal + y_background < min_count:
            continue

        # Calculate overall frequency (pi_w in the paper)
        overall_freq = (y_focal + y_background) / n_total

        # Dirichlet prior for this word
        alpha_w = alpha * overall_freq

        # Adjusted counts with prior
        adjusted_focal = y_focal + alpha_w
        adjusted_background = y_background + alpha_w

        # Total adjusted counts (for "not this word")
        total_adjusted_focal = n_focal + alpha - adjusted_focal
        total_adjusted_background = n_background + alpha - adjusted_background

        # Weighted log-odds score (equation 9 in Monroe et al.)
        if total_adjusted_focal > 0 and total_adjusted_background > 0:
            wlo = (
                math.log(adjusted_focal / total_adjusted_focal) -
                math.log(adjusted_background / total_adjusted_background)
            )
        else:
            wlo = 0.0

        # Calculate variance for z-score (equation 10 in Monroe et al.)
        variance = (1 / adjusted_focal) + (1 / adjusted_background)
        z_score = wlo / math.sqrt(variance) if variance > 0 else 0.0

        # Calculate usage rates for interpretability
        focal_rate = y_focal / n_focal if n_focal > 0 else 0.0
        background_rate = y_background / n_background if n_background > 0 else 0.0

        results.append(WeightedLogOddsResult(
            word=word,
            wlo_score=wlo,
            focal_count=y_focal,
            background_count=y_background,
            focal_rate=focal_rate,
            background_rate=background_rate,
            z_score=z_score,
        ))

    return results
