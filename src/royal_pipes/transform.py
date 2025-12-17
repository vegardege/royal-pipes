import re
from collections import Counter
from datetime import datetime
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
            return [parts[0].strip(), parts[1].strip()]

    # No pattern, return as-is
    return [word_lower]


def normalize_text(text: str) -> str:
    """Normalize text for word counting.

    Applies transformations:
    1. Remove acute (´) and grave (`) accents: "én" → "en", "è" → "e"
    2. Preserves Danish letters: å, æ, ø remain unchanged
    3. Convert to lowercase

    This function can be extended with more normalization rules as needed.

    Args:
        text: Raw text to normalize

    Returns:
        Normalized text

    Examples:
        >>> normalize_text("Én gang i 80'erne på Amalienborg")
        "en gang i 80'erne på amalienborg"
    """
    # Only remove acute and grave accents, preserve Nordic characters
    # Map specific accented characters to their base forms
    accent_map = {
        'á': 'a', 'à': 'a', 'é': 'e', 'è': 'e', 'í': 'i', 'ì': 'i',
        'ó': 'o', 'ò': 'o', 'ú': 'u', 'ù': 'u', 'ý': 'y', 'ỳ': 'y',
        'Á': 'a', 'À': 'a', 'É': 'e', 'È': 'e', 'Í': 'i', 'Ì': 'i',
        'Ó': 'o', 'Ò': 'o', 'Ú': 'u', 'Ù': 'u', 'Ý': 'y', 'Ỳ': 'y',
    }

    # Replace accented characters
    for accented, base in accent_map.items():
        text = text.replace(accented, base)

    return text.lower()


def read_stopwords() -> set[str]:
    """Read all active stopwords from the included txt file.

    Returns:
        A set of string, each string being a stopword
    """
    package_dir = Path(__file__).parent
    stopwords_path = package_dir / "data" / "stopwords.txt"
    with open(stopwords_path, "r") as f:
        return set(line.strip().lower() for line in f if line.strip())


def compute_word_counts(speeches_dir: str | Path) -> list[tuple[int, str, int, bool]]:
    """Compute word counts across all speech files.

    Args:
        speeches_dir: Directory containing YYYY.txt speech files

    Returns:
        List of (year, word, count, is_stopword) tuples with lowercase cleaned words
    """
    speeches_path = Path(speeches_dir)
    word_counts: list[tuple[int, str, int, bool]] = []

    stopwords = read_stopwords()

    for speech_file in sorted(speeches_path.glob("*.txt")):
        year = int(speech_file.stem)

        text = speech_file.read_text(encoding="utf-8")
        normalized = normalize_text(text)

        # Extract words: allow letters, digits, underscores, and apostrophes
        # This captures words like "80'erne" as a single token
        words = re.findall(r"[\w']+", normalized)
        word_counter = Counter(words)

        for word, count in word_counter.items():
            is_stopword = word in stopwords
            word_counts.append((year, word, count, is_stopword))

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


def compute_speeches(
    years: list[int],
    monarchs: list[tuple[str, int, int | None]],
) -> list[tuple[int, str]]:
    """Combine years with monarch data.

    Args:
        years: List of years for which speeches exist
        monarchs: List of (name, start_year, end_year) tuples where end_year
                  is None if still reigning

    Returns:
        List of (year, monarch_name) tuples for speeches

    Examples:
        >>> years = [1940, 1950]
        >>> monarchs = [("Christian X", 1912, 1947), ("Frederick IX", 1948, 1971)]
        >>> compute_speeches(years, monarchs)
        [(1940, "Christian X"), (1950, "Frederick IX")]
    """
    # Build a mapping of year -> monarch name for years they reigned on Dec 31
    year_to_monarch: dict[int, str] = {}
    current_year = datetime.now().year

    for name, start_year, end_year in monarchs:
        if end_year is None:
            end_year = current_year

        for year in range(start_year, end_year + 1):
            year_to_monarch[year] = name

    # Combine years with monarch data
    speeches: list[tuple[int, str]] = []
    for year in years:
        monarch = year_to_monarch.get(year)
        if monarch is not None:
            speeches.append((year, monarch))

    return speeches
