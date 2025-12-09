import re
from collections import Counter
from pathlib import Path


def expand_odds_word(word: str) -> list[str]:
    """Expand an odds word into searchable variants.

    Handles two slash patterns:
    1. "/-" (optional suffix): "Politi/-et" → ["politi", "politiet"]
    2. "/" (two complete words): "jøder/jødisk" → ["jøder", "jødisk"]

    For multi-word phrases, returns as-is.

    Args:
        word: The odds word (e.g., "Politi/-et", "jøder/jødisk", "AI", "Søens Folk")

    Returns:
        List of lowercase searchable variants

    Examples:
        >>> expand_odds_word("Politi/-et")
        ['politi', 'politiet']
        >>> expand_odds_word("jøder/jødisk")
        ['jøder', 'jødisk']
        >>> expand_odds_word("AI")
        ['ai']
        >>> expand_odds_word("Søens Folk")
        ['søens folk']
    """
    word_lower = word.lower()

    # Check for /- pattern (optional suffix)
    if "/-" in word_lower:
        parts = word_lower.split("/-")
        if len(parts) == 2:
            base = parts[0]  # e.g., "politi"
            suffix = parts[1]  # e.g., "et"
            return [base, base + suffix]

    # Check for / pattern (two complete words)
    # Must not have /- and must have exactly one /
    if "/" in word_lower and "/-" not in word_lower:
        parts = word_lower.split("/")
        if len(parts) == 2:
            # Two separate complete words
            return [parts[0].strip(), parts[1].strip()]

    # No pattern, return as-is
    return [word_lower]


def compute_word_counts(speeches_dir: str | Path) -> list[tuple[int, str, int]]:
    """Compute word counts across all speech files.

    Args:
        speeches_dir: Directory containing YYYY.txt speech files

    Returns:
        List of (year, word, count) tuples with lowercase cleaned words
    """
    speeches_path = Path(speeches_dir)
    word_counts: list[tuple[int, str, int]] = []

    for speech_file in sorted(speeches_path.glob("*.txt")):
        year = int(speech_file.stem)

        text = speech_file.read_text(encoding="utf-8")
        text_lower = text.lower()

        words = re.findall(r"\b[\w]+\b", text_lower)
        word_counter = Counter(words)

        for word, count in word_counter.items():
            word_counts.append((year, word, count))

    return word_counts


def compute_odds_counts(
    speeches_dir: str | Path, odds_words: list[str]
) -> list[tuple[int, str, int]]:
    """Count occurrences of betting odds words in historical speeches.

    Args:
        speeches_dir: Directory containing YYYY.txt speech files
        odds_words: List of odds words to count (e.g., ["Politi/-et", "AI", "Søens Folk"])

    Returns:
        List of (year, odds_word, count) tuples where count is the total
        occurrences of all variants of the odds word

    Examples:
        For "Politi/-et", counts both "politi" and "politiet" and sums them.
        For "Søens Folk", counts exact phrase "søens folk".
    """
    speeches_path = Path(speeches_dir)
    odds_counts: list[tuple[int, str, int]] = []

    for speech_file in sorted(speeches_path.glob("*.txt")):
        year = int(speech_file.stem)
        text = speech_file.read_text(encoding="utf-8")
        text_lower = text.lower()

        for odds_word in odds_words:
            # Expand the odds word into searchable variants
            variants = expand_odds_word(odds_word)

            total_count = 0
            for variant in variants:
                # For single words, count word boundaries
                if " " not in variant:
                    # Use word boundary regex
                    pattern = rf"\b{re.escape(variant)}\b"
                    matches = re.findall(pattern, text_lower)
                    total_count += len(matches)
                else:
                    # For multi-word phrases, count exact matches
                    total_count += text_lower.count(variant)

            odds_counts.append((year, odds_word, total_count))

    return odds_counts
