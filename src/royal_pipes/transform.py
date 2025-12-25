import json
import re
from collections import Counter
from datetime import datetime
from pathlib import Path

from royal_pipes.models import (
    ComparisonResult,
    EventMention,
    Monarch,
    OddsCount,
    PersonCount,
    PlaceCount,
    Speech,
    SpeechNer,
    WLOComparison,
    WordCount,
)
from royal_pipes.statistics import weighted_log_odds


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
        "á": "a",
        "à": "a",
        "é": "e",
        "è": "e",
        "í": "i",
        "ì": "i",
        "ó": "o",
        "ò": "o",
        "ú": "u",
        "ù": "u",
        "ý": "y",
        "ỳ": "y",
        "Á": "a",
        "À": "a",
        "É": "e",
        "È": "e",
        "Í": "i",
        "Ì": "i",
        "Ó": "o",
        "Ò": "o",
        "Ú": "u",
        "Ù": "u",
        "Ý": "y",
        "Ỳ": "y",
    }

    # Replace accented characters
    for accented, base in accent_map.items():
        text = text.replace(accented, base)

    return text.lower()


def normalize_for_wlo(word: str) -> str:
    """Normalize a word for weighted log-odds comparisons.

    Applies additional normalization beyond standard text normalization,
    specifically for comparative analysis where we want to treat historical
    spelling variants as the same word.

    Applies transformations:
    1. Normalize 'aa' to 'å' (historical Danish spelling: "gaar" → "går")
    2. Convert to lowercase

    Args:
        word: The word to normalize

    Returns:
        Normalized word

    Examples:
        >>> normalize_for_wlo("Gaar")
        "går"
        >>> normalize_for_wlo("Danmark")
        "danmark"

    Note:
        This is applied AFTER word extraction, so it doesn't affect proper nouns
        like "Aarhus" that were already extracted as complete tokens.
    """
    # Normalize 'aa' to 'å' before lowercasing to handle 'Aa' correctly
    word = word.replace("Aa", "Å").replace("AA", "Å").replace("aa", "å")
    return word.lower()


def read_stopwords() -> set[str]:
    """Read all active stopwords from the included txt file.

    Returns:
        A set of string, each string being a stopword
    """
    package_dir = Path(__file__).parent
    stopwords_path = package_dir / "data" / "stopwords.txt"
    with open(stopwords_path, "r") as f:
        return set(line.strip().lower() for line in f if line.strip())


def compute_word_counts(speeches_dir: str | Path) -> list[WordCount]:
    """Compute word counts across all speech files.

    Args:
        speeches_dir: Directory containing YYYY.txt speech files

    Returns:
        List of WordCount objects with lowercase cleaned words
    """
    speeches_path = Path(speeches_dir)
    word_counts: list[WordCount] = []

    stopwords = read_stopwords()

    for speech_file in sorted(speeches_path.glob("*.txt")):
        year = int(speech_file.stem)

        text = speech_file.read_text(encoding="utf-8")
        normalized = normalize_text(text)

        # Extract words: allow letters, digits, underscores, and apostrophes
        # This captures words like "80'erne" as a single token
        # Apostrophes must be surrounded by word characters (no standalone ')
        words = re.findall(r"\w+(?:'\w+)*", normalized)
        word_counter = Counter(words)

        for word, count in word_counter.items():
            is_stopword = word in stopwords
            word_counts.append(
                WordCount(year=year, word=word, count=count, is_stopword=is_stopword)
            )

    return word_counts


def compute_odds_counts(
    speeches_dir: str | Path, odds_words: list[str]
) -> list[OddsCount]:
    """Count occurrences of betting odds words in historical speeches.

    Args:
        speeches_dir: Directory containing YYYY.txt speech files
        odds_words: List of odds words to count (e.g., ["Politi/-et", "AI", "Søens Folk"])

    Returns:
        List of OddsCount objects where count is the total
        occurrences of all variants of the odds word

    Examples:
        For "Politi/-et", counts both "politi" and "politiet" and sums them.
        For "Søens Folk", counts exact phrase "søens folk".
    """
    speeches_path = Path(speeches_dir)
    odds_counts: list[OddsCount] = []

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

            odds_counts.append(OddsCount(year=year, word=odds_word, count=total_count))

    return odds_counts


def compute_speeches(
    years: list[int],
    monarchs: list[Monarch],
) -> list[Speech]:
    """Combine years with monarch data.

    Args:
        years: List of years for which speeches exist
        monarchs: List of Monarch objects

    Returns:
        List of Speech objects

    Examples:
        >>> years = [1940, 1950]
        >>> monarchs = [Monarch("Christian X", 1912, 1947), Monarch("Frederick IX", 1948, 1971)]
        >>> compute_speeches(years, monarchs)
        [Speech(1940, "Christian X"), Speech(1950, "Frederick IX")]
    """
    # Build a mapping of year -> monarch name for years they reigned on Dec 31
    year_to_monarch: dict[int, str] = {}
    current_year = datetime.now().year

    for monarch in monarchs:
        end_year = monarch.end_year if monarch.end_year is not None else current_year

        for year in range(monarch.start_year, end_year + 1):
            year_to_monarch[year] = monarch.name

    # Combine years with monarch data
    speeches: list[Speech] = []
    for year in years:
        monarch_name = year_to_monarch.get(year)
        if monarch_name is not None:
            speeches.append(Speech(year=year, monarch=monarch_name))

    return speeches


def compute_monarch_comparisons(
    word_counts: list[WordCount],
    speeches: list[Speech],
    alpha: float = 0.01,
    min_count: int = 3,
    top_n: int = 20,
) -> list[ComparisonResult]:
    """Compute weighted log-odds comparisons for each monarch vs others.

    Args:
        word_counts: List of WordCount objects from database
        speeches: List of Speech objects from database
        alpha: Dirichlet prior strength parameter
        min_count: Minimum total count to include a word
        top_n: Number of top words to return per comparison

    Returns:
        List of ComparisonResult objects, one per monarch
    """
    # Build year -> monarch mapping
    year_to_monarch = {speech.year: speech.monarch for speech in speeches}

    # Get unique monarchs
    monarchs = sorted(set(speech.monarch for speech in speeches))

    results = []

    for focal_monarch in monarchs:
        # Build focal and background word counts
        # Apply WLO normalization to merge spelling variants (e.g., 'aa' → 'å')
        focal_counts: dict[str, int] = {}
        background_counts: dict[str, int] = {}

        for word_count in word_counts:
            monarch = year_to_monarch.get(word_count.year)
            if monarch is None:
                continue

            # Normalize word for comparison (merges 'aa' → 'å', etc.)
            normalized_word = normalize_for_wlo(word_count.word)

            # Skip pure numbers (years, dates, etc.)
            if normalized_word.isdigit():
                continue

            if monarch == focal_monarch:
                focal_counts[normalized_word] = (
                    focal_counts.get(normalized_word, 0) + word_count.count
                )
            else:
                background_counts[normalized_word] = (
                    background_counts.get(normalized_word, 0) + word_count.count
                )

        # Compute weighted log-odds
        wlo_results = weighted_log_odds(
            focal_counts, background_counts, alpha=alpha, min_count=min_count
        )

        # Sort by score and take top N
        top_words = sorted(wlo_results, key=lambda r: r.wlo_score, reverse=True)[:top_n]

        # Create comparison metadata
        comparison = WLOComparison(
            comparison_type="monarch",
            focal_value=focal_monarch,
            background_type="other_monarchs",
            alpha=alpha,
            focal_corpus_size=sum(focal_counts.values()),
            background_corpus_size=sum(background_counts.values()),
        )

        results.append(ComparisonResult(comparison=comparison, top_words=top_words))

    return results


def compute_decade_comparisons(
    word_counts: list[WordCount],
    alpha: float = 0.01,
    min_count: int = 3,
    top_n: int = 20,
) -> list[ComparisonResult]:
    """Compute weighted log-odds comparisons for each decade vs others.

    Args:
        word_counts: List of WordCount objects from database
        alpha: Dirichlet prior strength parameter
        min_count: Minimum total count to include a word
        top_n: Number of top words to return per comparison

    Returns:
        List of ComparisonResult objects, one per decade
    """
    # Get all decades present in data
    decades = sorted(set((wc.year // 10) * 10 for wc in word_counts))

    results = []

    for focal_decade in decades:
        # Build focal and background word counts
        # Apply WLO normalization to merge spelling variants (e.g., 'aa' → 'å')
        focal_counts: dict[str, int] = {}
        background_counts: dict[str, int] = {}

        for word_count in word_counts:
            decade = (word_count.year // 10) * 10

            # Normalize word for comparison (merges 'aa' → 'å', etc.)
            normalized_word = normalize_for_wlo(word_count.word)

            # Skip pure numbers (years, dates, etc.)
            if normalized_word.isdigit():
                continue

            if decade == focal_decade:
                focal_counts[normalized_word] = (
                    focal_counts.get(normalized_word, 0) + word_count.count
                )
            else:
                background_counts[normalized_word] = (
                    background_counts.get(normalized_word, 0) + word_count.count
                )

        # Compute weighted log-odds
        wlo_results = weighted_log_odds(
            focal_counts, background_counts, alpha=alpha, min_count=min_count
        )

        # Sort by score and take top N
        top_words = sorted(wlo_results, key=lambda r: r.wlo_score, reverse=True)[:top_n]

        # Create comparison metadata
        comparison = WLOComparison(
            comparison_type="decade",
            focal_value=f"{focal_decade}s",
            background_type="other_decades",
            alpha=alpha,
            focal_corpus_size=sum(focal_counts.values()),
            background_corpus_size=sum(background_counts.values()),
        )

        results.append(ComparisonResult(comparison=comparison, top_words=top_words))

    return results


def compute_speech_ner(ner_json_dir: str | Path) -> list[SpeechNer]:
    """Load and parse all NER JSON files.

    Args:
        ner_json_dir: Directory containing YYYY.json NER files

    Returns:
        List of SpeechNer objects for all years

    Raises:
        ValueError: If JSON parsing fails or data is malformed
    """
    ner_dir_path = Path(ner_json_dir)
    ner_results: list[SpeechNer] = []

    for ner_file in sorted(ner_dir_path.glob("*.json")):
        year = int(ner_file.stem)

        try:
            data = json.loads(ner_file.read_text(encoding="utf-8"))

            # Parse persons
            persons = [
                PersonCount(name=p["name"], count=p["count"])
                for p in data.get("persons", [])
            ]

            # Parse places
            places = [
                PlaceCount(name=p["name"], count=p["count"])
                for p in data.get("places", [])
            ]

            # Parse events
            events = [
                EventMention(name=e["name"], is_significant=e.get("is_significant", False))
                for e in data.get("events", [])
            ]

            ner_results.append(
                SpeechNer(year=year, persons=persons, places=places, events=events)
            )

        except (KeyError, ValueError, json.JSONDecodeError) as e:
            raise ValueError(f"Failed to parse NER file {ner_file}: {e}") from e

    return ner_results
