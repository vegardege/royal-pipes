from collections import Counter
from datetime import timedelta

import dagster as dg
from pydantic import Field

from royal_pipes.config import SPEECHES_DIR
from royal_pipes.defs.resources import AnalyticsDB
from royal_pipes.extract import (
    BETTING_URL,
    load_danskespil_odds,
    load_leipzig_corpus,
    load_monarchs,
    load_official_speech,
    load_official_speeches,
)
from royal_pipes.transform import (
    compute_decade_comparisons,
    compute_monarch_comparisons,
    compute_odds_counts,
    compute_speeches,
    compute_word_counts,
)

# Partition Definitions
year_partitions = dg.DynamicPartitionsDefinition(name="years")


# ==============================================================================
# Speeches Domain: Official speeches from kongehuset.dk
# ==============================================================================


@dg.asset(
    freshness_policy=dg.FreshnessPolicy.time_window(fail_window=timedelta(days=365)),
)
async def kongehuset_speeches(context: dg.AssetExecutionContext) -> dict[int, str]:
    """Discover all speeches published to the official source."""
    context.log.info("Downloading recent speeches from Kongehuset")
    speeches = await load_official_speeches()

    if not speeches:
        raise ValueError("No speeches found, inspect site for changes")

    context.log.info(f"Found {len(speeches)} speeches")

    current_partitions = set(context.instance.get_dynamic_partitions("years"))
    new_partitions = [
        str(year) for year in speeches if str(year) not in current_partitions
    ]

    if new_partitions:
        context.instance.add_dynamic_partitions("years", new_partitions)
        context.log.info(f"Found {len(new_partitions)} new years")

    return speeches


@dg.asset_check(asset=kongehuset_speeches)
def speeches_found_count(
    _: dg.AssetCheckExecutionContext, kongehuset_speeches: dict[int, str]
) -> dg.AssetCheckResult:
    """Check that at least 10 speeches were found from the official source."""
    min_speeches = 10
    count = len(kongehuset_speeches)
    passed = count >= min_speeches

    return dg.AssetCheckResult(
        passed=passed,
        description=f"Found {count} speeches (minimum: {min_speeches})"
        if passed
        else f"Only found {count} speeches, expected at least {min_speeches}",
        metadata={"count": count, "min_count": min_speeches},
    )


@dg.asset(
    partitions_def=year_partitions,
    io_manager_key="speech_text_io",
)
async def kongehuset_speech(
    context: dg.AssetExecutionContext,
    kongehuset_speeches: dict[int, str],
) -> str:
    """Download and store a single speech.

    Stored as YYYY.txt in the XDG data directory. You can manually add files
    to the speeches directory and they will be used instead of re-scraping.
    """
    year = int(context.partition_key)
    if url := kongehuset_speeches.get(year):
        context.log.info(f"Scraping speech for {year} from {url}")
        content = await load_official_speech(url)
        context.log.info(f"Scraped {len(content)} characters for {year}")
        return content
    else:
        raise ValueError(f"No URL found for {year}")


@dg.asset_check(asset=kongehuset_speech)
def speeches_minimum_length(_: dg.AssetCheckExecutionContext) -> dg.AssetCheckResult:
    """Check that all speech files have minimum expected length."""
    min_length = 500
    failed_speeches = []

    for speech_file in sorted(SPEECHES_DIR.glob("*.txt")):
        content = speech_file.read_text(encoding="utf-8")
        if len(content) < min_length:
            failed_speeches.append((speech_file.stem, len(content)))

    passed = len(failed_speeches) == 0

    if passed:
        total_files = len(list(SPEECHES_DIR.glob("*.txt")))
        description = (
            f"All {total_files} speeches meet minimum length of {min_length} characters"
        )
    else:
        description = (
            f"{len(failed_speeches)} speeches below minimum: {failed_speeches}"
        )

    return dg.AssetCheckResult(
        passed=passed,
        description=description,
        metadata={"min_length": min_length, "failed_count": len(failed_speeches)},
    )


@dg.asset_check(asset=kongehuset_speech)
def speeches_no_duplicates(_: dg.AssetCheckExecutionContext) -> dg.AssetCheckResult:
    """Check that there are no duplicate years in the speeches.

    Duplicate years would indicate a manual error, such as having both
    2024.txt and 2024 (2).txt in the speeches directory, or that an old
    speech was accidentally written to two different yearly files.
    """
    years = []

    for speech_file in sorted(SPEECHES_DIR.glob("*.txt")):
        try:
            year = int(speech_file.stem)
            years.append(year)
        except ValueError:
            # Skip files that don't have valid year names
            pass

    # Find duplicates
    year_counts = Counter(years)
    duplicates = [year for year, count in year_counts.items() if count > 1]

    passed = len(duplicates) == 0

    if passed:
        description = f"All {len(years)} speeches have unique years"
    else:
        description = f"Found {len(duplicates)} duplicate year(s): {sorted(duplicates)}"

    return dg.AssetCheckResult(
        passed=passed,
        description=description,
        metadata={
            "total_speeches": len(years),
            "duplicate_years": sorted(duplicates),
        },
    )


@dg.asset(
    deps=[dg.AssetDep("kongehuset_speech")],
    auto_materialize_policy=dg.AutoMaterializePolicy.eager(),
)
def word_count(context: dg.AssetExecutionContext, analytics_db: AnalyticsDB) -> None:
    """Compute word counts across all speeches and store in SQLite.

    Reads all speech files from the XDG speeches directory and computes word
    frequencies per year. Results are stored in the analytics database.

    Table: word_count (year, word, count)
    """
    context.log.info("Computing word counts from all speeches")
    word_counts_data = compute_word_counts(SPEECHES_DIR)
    context.log.info(f"Found {len(word_counts_data)} word-year pairs")

    analytics_db.replace_word_count(word_counts_data)
    context.log.info(f"Stored word counts to {analytics_db.db_path}")


# ==============================================================================
# Betting Domain: Betting odds from Danske Spil
# ==============================================================================


class BettingOddsConfig(dg.Config):
    """Configuration for scraping betting odds.

    These settings allow you to customize which betting market to scrape
    each year, as the URL and section structure may change.
    """

    url: str = Field(
        default=BETTING_URL,
        description="URL of the betting page for the King's New Year speech",
    )
    section_index: int = Field(
        default=0,
        description="Which market section to scrape, starting from 0",
    )


@dg.asset(
    freshness_policy=dg.FreshnessPolicy.time_window(fail_window=timedelta(days=1)),
)
async def danskespil_odds(
    context: dg.AssetExecutionContext, config: BettingOddsConfig
) -> dict[str, float]:
    """Scrape current betting odds for which words will appear in the speech.

    This asset tracks what bookmakers think will be mentioned in the King's
    New Year speech. The URL and section can be configured per materialization
    to adapt to different years.

    Returns a dictionary mapping words/phrases to their odds.
    """
    context.log.info(
        f"Scraping odds from {config.url} (section {config.section_index})"
    )
    odds = await load_danskespil_odds(
        url=config.url, section_index=config.section_index
    )
    context.log.info(f"Scraped {len(odds)} betting options")

    if odds:
        sorted_odds = sorted(odds.items(), key=lambda x: x[1])
        context.log.info(f"Most likely:  {sorted_odds[0]}")
        context.log.info(f"Least likely: {sorted_odds[-1]}")

    return odds


@dg.asset_check(asset=danskespil_odds)
def betting_odds_minimum_count(
    _: dg.AssetCheckExecutionContext, danskespil_odds: dict[str, float]
) -> dg.AssetCheckResult:
    """Check that we scraped a reasonable number of betting options."""
    min_count = 100
    count = len(danskespil_odds)
    passed = count >= min_count

    return dg.AssetCheckResult(
        passed=passed,
        description=f"Found {count} betting options (minimum: {min_count})"
        if passed
        else f"Only found {count} betting options, expected at least {min_count}",
        metadata={"count": count, "min_count": min_count},
    )


@dg.asset(
    auto_materialize_policy=dg.AutoMaterializePolicy.eager(),
    freshness_policy=dg.FreshnessPolicy.time_window(fail_window=timedelta(hours=24)),
)
def odds(
    context: dg.AssetExecutionContext,
    analytics_db: AnalyticsDB,
    danskespil_odds: dict[str, float],
) -> None:
    """Store betting odds in the SQLite database.

    Stores the scraped odds in the 'odds' table with schema:
    - word TEXT (primary key)
    - odds REAL

    This table is atomically replaced each time the asset is materialized.

    Freshness: Expected to be updated at least once every 24 hours.
    """
    context.log.info(f"Storing {len(danskespil_odds)} betting odds to database")
    analytics_db.replace_odds(danskespil_odds)
    context.log.info(f"Stored odds to {analytics_db.db_path}")


@dg.asset_check(asset=odds)
def odds_stored_correctly(
    _: dg.AssetCheckExecutionContext, analytics_db: AnalyticsDB
) -> dg.AssetCheckResult:
    """Check that odds were stored correctly in the database."""
    with analytics_db.get_connection() as conn:
        cursor = conn.execute("SELECT COUNT(*) FROM odds")
        db_count = cursor.fetchone()[0]

        cursor = conn.execute("SELECT COUNT(*) FROM odds WHERE odds <= 1.0")
        invalid_odds = cursor.fetchone()[0]

    passed = invalid_odds == 0

    issues = []
    if not passed:
        issues.append(f"{invalid_odds} odds with value ≤ 1.0")

    return dg.AssetCheckResult(
        passed=passed,
        description=f"Stored {db_count} odds correctly"
        if passed
        else "; ".join(issues),
        metadata={
            "db_count": db_count,
            "invalid_odds": invalid_odds,
        },
    )


@dg.asset(
    deps=[dg.AssetDep("kongehuset_speech"), dg.AssetDep("odds")],
    auto_materialize_policy=dg.AutoMaterializePolicy.eager(),
)
def odds_count(
    context: dg.AssetExecutionContext,
    analytics_db: AnalyticsDB,
) -> None:
    """Count occurrences of betting odds words in historical speeches.

    Reads all speech files from disk and counts how many times each odds
    word (and its variants) appears in each year's speech.

    For words like "Politi/-et", counts both "politi" and "politiet" together.
    For multi-word phrases like "Søens Folk", counts exact phrase matches.

    Table: odds_count (year, word, count)
    """
    context.log.info("Computing odds counts from speeches")
    with analytics_db.get_connection() as conn:
        cursor = conn.execute("SELECT word FROM odds")
        odds_words = [row[0] for row in cursor.fetchall()]
    context.log.info(f"Counting {len(odds_words)} odds words across all speeches")

    odds_counts_data = compute_odds_counts(SPEECHES_DIR, odds_words)
    context.log.info(f"Found {len(odds_counts_data)} word-year pairs")

    analytics_db.replace_odds_counts(odds_counts_data)
    context.log.info(f"Stored odds counts to {analytics_db.db_path}")


@dg.asset_check(asset=odds_count)
def odds_count_valid(
    _: dg.AssetCheckExecutionContext,
    analytics_db: AnalyticsDB,
) -> dg.AssetCheckResult:
    """Check that odds counts were stored correctly with valid values."""
    with analytics_db.get_connection() as conn:
        cursor = conn.execute("SELECT COUNT(*) FROM odds_count")
        total_entries = cursor.fetchone()[0]

        cursor = conn.execute("SELECT COUNT(*) FROM odds_count WHERE count < 0")
        negative_counts = cursor.fetchone()[0]

        cursor = conn.execute("SELECT COUNT(DISTINCT word) FROM odds_count")
        unique_words = cursor.fetchone()[0]

        cursor = conn.execute("SELECT COUNT(DISTINCT year) FROM odds_count")
        unique_years = cursor.fetchone()[0]

    no_negatives = negative_counts == 0
    has_data = total_entries > 0
    passed = no_negatives and has_data

    issues = []
    if not has_data:
        issues.append("No odds count data found")
    if not no_negatives:
        issues.append(f"{negative_counts} negative counts found")

    return dg.AssetCheckResult(
        passed=passed,
        description=f"Stored {total_entries:,} odds counts ({unique_words} words × {unique_years} years)"
        if passed
        else "; ".join(issues),
        metadata={
            "total_entries": total_entries,
            "unique_words": unique_words,
            "unique_years": unique_years,
            "negative_counts": negative_counts,
        },
    )


# ==============================================================================
# Monarchs Domain: Danish monarch data from Wikipedia
# ==============================================================================


@dg.asset(
    freshness_policy=dg.FreshnessPolicy.time_window(fail_window=timedelta(days=300)),
)
async def wikipedia_monarchs(
    context: dg.AssetExecutionContext,
) -> list[tuple[str, int, int | None]]:
    """Download the list of Danish monarchs from Wikipedia.

    Returns monarchs from 1941 onwards (when New Year speeches began).
    Each monarch is represented as (name, start_year, end_year) where
    end_year is None for the current monarch.
    """
    context.log.info("Downloading monarchs from Wikipedia")
    monarchs_data = await load_monarchs()
    context.log.info(f"Found {len(monarchs_data)} monarchs from 1941 onwards")

    for name, start_year, end_year in monarchs_data:
        context.log.info(f"  {name}: {start_year}–{end_year or 'present'}")

    return monarchs_data


@dg.asset_check(asset=wikipedia_monarchs)
def monarchs_data_valid(
    _: dg.AssetCheckExecutionContext,
    wikipedia_monarchs: list[tuple[str, int, int | None]],
) -> dg.AssetCheckResult:
    """Check that monarch data is complete and valid."""
    min_monarchs = 3  # Expect at least 3 monarchs since 1941
    count = len(wikipedia_monarchs)

    issues = []
    current_monarch_count = sum(1 for _, _, end in wikipedia_monarchs if end is None)

    if current_monarch_count != 1:
        issues.append(
            f"Expected 1 current monarch (end_year=None), found {current_monarch_count}"
        )

    for name, start, end in wikipedia_monarchs:
        if end is not None and end < start:
            issues.append(f"{name} ends before starting: {start}-{end}")

    passed = count >= min_monarchs and len(issues) == 0

    description = (
        f"Found {count} valid monarchs"
        if passed
        else f"Found {count} monarchs but: {'; '.join(issues)}"
    )

    return dg.AssetCheckResult(
        passed=passed,
        description=description,
        metadata={
            "count": count,
            "min_count": min_monarchs,
            "current_monarchs": current_monarch_count,
            "issues": issues,
        },
    )


@dg.asset(
    deps=[dg.AssetDep("kongehuset_speech")],
    auto_materialize_policy=dg.AutoMaterializePolicy.eager(),
)
def speech(
    context: dg.AssetExecutionContext,
    analytics_db: AnalyticsDB,
    wikipedia_monarchs: list[tuple[str, int, int | None]],
) -> None:
    """Compute speech metadata and store in SQLite.

    Combines speech years with monarch data to create a speech metadata table.

    Table: speech (year, monarch)
    """
    context.log.info("Computing speech metadata from speeches and monarchs")

    years = sorted([int(f.stem) for f in SPEECHES_DIR.glob("*.txt")])
    context.log.info(f"Found {len(years)} speeches")

    speeches_data = compute_speeches(years, wikipedia_monarchs)
    context.log.info(f"Matched {len(speeches_data)} speeches with monarch data")

    if speeches_data:
        context.log.info("Sample speeches:")
        for year, monarch in speeches_data[:3]:
            context.log.info(f"  {year}: {monarch}")

    analytics_db.replace_speech(speeches_data)
    context.log.info(f"Stored speech metadata to {analytics_db.db_path}")


@dg.asset_check(asset=speech)
def speech_has_monarchs(
    _: dg.AssetCheckExecutionContext, analytics_db: AnalyticsDB
) -> dg.AssetCheckResult:
    """Check that all speeches have a monarch assigned.

    Verifies that every entry in the speech table has a non-empty monarch name.
    """
    with analytics_db.get_connection() as conn:
        cursor = conn.execute("""
            SELECT year
            FROM speech
            WHERE monarch IS NULL OR monarch = ''
            ORDER BY year
        """)
        missing_monarchs = [row[0] for row in cursor.fetchall()]

        cursor = conn.execute("""
            SELECT monarch, COUNT(*) as count
            FROM speech
            WHERE monarch IS NOT NULL AND monarch != ''
            GROUP BY monarch
            ORDER BY count DESC
        """)
        monarch_distribution = {row[0]: row[1] for row in cursor.fetchall()}

        cursor = conn.execute("SELECT COUNT(*) FROM speech")
        total_speeches = cursor.fetchone()[0]

    passed = len(missing_monarchs) == 0

    if passed:
        description = f"All {total_speeches} speeches have monarchs assigned"
    else:
        description = (
            f"{len(missing_monarchs)} speeches missing monarchs: {missing_monarchs}"
        )

    return dg.AssetCheckResult(
        passed=passed,
        description=description,
        metadata={
            "total_speeches": total_speeches,
            "speeches_with_monarchs": total_speeches - len(missing_monarchs),
            "missing_years": missing_monarchs,
            "monarch_distribution": monarch_distribution,
        },
    )


# ==============================================================================
# Corpus Domain: Leipzig Corpora Collection for Danish language baseline
# ==============================================================================


@dg.asset(
    freshness_policy=dg.FreshnessPolicy.time_window(fail_window=timedelta(days=300)),
)
async def leipzig_corpus(
    context: dg.AssetExecutionContext,
) -> list[tuple[str, int]]:
    """Download Danish word frequency data from Leipzig Corpora Collection.

    Downloads the Mixed 1M dataset containing Danish word frequencies from
    the Leipzig Wortschatz project. This provides a general Danish language
    corpus for comparison with the speech word frequencies.

    Returns a list of (word, count) tuples from the corpus.
    """
    context.log.info("Downloading Leipzig Corpora Collection dataset")
    corpus = await load_leipzig_corpus()
    context.log.info(f"Downloaded {len(corpus)} words from Leipzig corpus")

    if corpus:
        total_count = sum(count for _, count in corpus)
        context.log.info(f"Total word occurrences: {total_count:,}")
        context.log.info(f"Top 5 most frequent words: {corpus[:5]}")

    return corpus


@dg.asset_check(asset=leipzig_corpus)
def corpus_minimum_words(
    _: dg.AssetCheckExecutionContext, leipzig_corpus: list[tuple[str, int]]
) -> dg.AssetCheckResult:
    """Check that the Leipzig corpus contains a minimum number of words."""
    min_words = 100_000  # Expect at least 100k words from a 1M corpus
    count = len(leipzig_corpus)
    passed = count >= min_words

    return dg.AssetCheckResult(
        passed=passed,
        description=f"Found {count:,} words in corpus (minimum: {min_words:,})"
        if passed
        else f"Only found {count:,} words in corpus, expected at least {min_words:,}",
        metadata={"count": count, "min_count": min_words},
    )


@dg.asset(
    auto_materialize_policy=dg.AutoMaterializePolicy.eager(),
)
def corpus(
    context: dg.AssetExecutionContext,
    analytics_db: AnalyticsDB,
    leipzig_corpus: list[tuple[str, int]],
) -> None:
    """Store Leipzig corpus word frequencies in the SQLite database.

    This table is atomically replaced each time the asset is materialized.
    """
    context.log.info(f"Calculating frequencies for {len(leipzig_corpus)} words")

    total_count = sum(count for _, count in leipzig_corpus)
    context.log.info(f"Total word occurrences in corpus: {total_count:,}")

    corpus_with_freq = [
        (word, count, count / total_count) for word, count in leipzig_corpus
    ]

    context.log.info(f"Storing {len(corpus_with_freq)} words to database")
    analytics_db.replace_corpus(corpus_with_freq)
    context.log.info(f"Stored corpus to {analytics_db.db_path}")


@dg.asset_check(asset=corpus)
def corpus_stored_correctly(
    _: dg.AssetCheckExecutionContext, analytics_db: AnalyticsDB
) -> dg.AssetCheckResult:
    """Check that corpus data was stored correctly in the database."""
    with analytics_db.get_connection() as conn:
        cursor = conn.execute("SELECT COUNT(*) FROM corpus")
        db_count = cursor.fetchone()[0]

        cursor = conn.execute("SELECT SUM(frequency) FROM corpus")
        freq_sum = cursor.fetchone()[0] or 0

        cursor = conn.execute("SELECT COUNT(*) FROM corpus WHERE count <= 0")
        invalid_counts = cursor.fetchone()[0]

    freq_valid = 0.99 <= freq_sum <= 1.01  # Allow small floating point errors
    no_invalid = invalid_counts == 0
    passed = freq_valid and no_invalid

    issues = []
    if not freq_valid:
        issues.append(f"Frequencies sum to {freq_sum:.4f} (expected ~1.0)")
    if not no_invalid:
        issues.append(f"{invalid_counts} words with count ≤ 0")

    return dg.AssetCheckResult(
        passed=passed,
        description=f"Stored {db_count:,} words correctly (freq sum: {freq_sum:.4f})"
        if passed
        else "; ".join(issues),
        metadata={
            "db_count": db_count,
            "frequency_sum": round(freq_sum, 4),
            "invalid_counts": invalid_counts,
        },
    )


# ==============================================================================
# Comparative Analysis Domain: Weighted log-odds comparisons
# ==============================================================================


class WLOConfig(dg.Config):
    """Configuration for weighted log-odds comparisons."""

    alpha: float = Field(
        default=0.01,
        description="Dirichlet prior strength parameter (Monroe et al. 2008)",
    )
    min_count: int = Field(
        default=5,
        description="Minimum total count across both corpora to include a word",
    )
    top_n: int = Field(
        default=20,
        description="Number of top distinctive words to store per comparison",
    )


@dg.asset(
    deps=[dg.AssetDep("word_count"), dg.AssetDep("speech")],
    auto_materialize_policy=dg.AutoMaterializePolicy.eager(),
)
def wlo_comparisons(
    context: dg.AssetExecutionContext,
    analytics_db: AnalyticsDB,
    config: WLOConfig,
) -> None:
    """Compute weighted log-odds comparisons and store in SQLite.

    Implements the "Fightin' Words" algorithm from Monroe et al. (2008) to
    identify the most distinctive words for each monarch and decade compared
    to all others.

    Creates two types of comparisons:
    - Monarch comparisons: Each monarch vs all other monarchs
    - Decade comparisons: Each decade vs all other decades

    Results are stored in two tables:
    - wlo_comparisons: Metadata for each comparison
    - wlo_words: Top N most distinctive words per comparison
    """
    context.log.info(
        f"Computing WLO comparisons (alpha={config.alpha}, min_count={config.min_count}, top_n={config.top_n})"
    )

    # Read word counts and speech metadata from database
    with analytics_db.get_connection() as conn:
        cursor = conn.execute("SELECT year, word, count, is_stopword FROM word_count")
        word_counts = cursor.fetchall()
        context.log.info(f"Loaded {len(word_counts)} word-year pairs from database")

        cursor = conn.execute("SELECT year, monarch FROM speech")
        speeches = cursor.fetchall()
        context.log.info(f"Loaded {len(speeches)} speech-monarch pairs from database")

    # Compute monarch comparisons
    context.log.info("Computing monarch comparisons")
    monarch_results = compute_monarch_comparisons(
        word_counts, speeches, alpha=config.alpha, min_count=config.min_count, top_n=config.top_n
    )
    context.log.info(f"Generated {len(monarch_results)} monarch comparisons")

    # Compute decade comparisons
    context.log.info("Computing decade comparisons")
    decade_results = compute_decade_comparisons(
        word_counts, alpha=config.alpha, min_count=config.min_count, top_n=config.top_n
    )
    context.log.info(f"Generated {len(decade_results)} decade comparisons")

    # Combine all results and format for database
    all_comparisons = []
    all_words = []
    comparison_id = 1

    for comparison, top_words in monarch_results + decade_results:
        # Add comparison metadata
        all_comparisons.append((
            comparison.comparison_type,
            comparison.focal_value,
            comparison.background_type,
            comparison.alpha,
            comparison.focal_corpus_size,
            comparison.background_corpus_size,
        ))

        # Add top words with ranks
        for rank, result in enumerate(top_words, start=1):
            all_words.append((
                comparison_id,
                rank,
                result.word,
                result.wlo_score,
                result.focal_count,
                result.background_count,
                result.focal_rate,
                result.background_rate,
                result.z_score,
            ))

        comparison_id += 1

    # Store in database
    context.log.info(
        f"Storing {len(all_comparisons)} comparisons with {len(all_words)} top words"
    )
    analytics_db.replace_wlo_comparisons(all_comparisons, all_words)
    context.log.info(f"Stored WLO comparisons to {analytics_db.db_path}")

    # Log sample results
    if monarch_results:
        comparison, top_words = monarch_results[0]
        context.log.info(
            f"Sample: {comparison.focal_value} vs {comparison.background_type}"
        )
        if top_words:
            context.log.info(f"  Top word: '{top_words[0].word}' (score: {top_words[0].wlo_score:.3f})")


@dg.asset_check(asset=wlo_comparisons)
def wlo_comparisons_stored_correctly(
    _: dg.AssetCheckExecutionContext, analytics_db: AnalyticsDB
) -> dg.AssetCheckResult:
    """Check that WLO comparison data was stored correctly in the database."""
    with analytics_db.get_connection() as conn:
        cursor = conn.execute("SELECT COUNT(*) FROM wlo_comparisons")
        comparison_count = cursor.fetchone()[0]

        cursor = conn.execute("SELECT COUNT(*) FROM wlo_words")
        word_count = cursor.fetchone()[0]

        cursor = conn.execute(
            "SELECT COUNT(DISTINCT comparison_type) FROM wlo_comparisons"
        )
        comparison_types = cursor.fetchone()[0]

        cursor = conn.execute(
            """SELECT c.comparison_id, c.focal_value, COUNT(w.word) as word_count
               FROM wlo_comparisons c
               LEFT JOIN wlo_words w ON c.comparison_id = w.comparison_id
               GROUP BY c.comparison_id
               HAVING word_count = 0"""
        )
        empty_comparisons = cursor.fetchall()

    has_comparisons = comparison_count > 0
    has_words = word_count > 0
    has_both_types = comparison_types == 2  # monarch and decade
    no_empty = len(empty_comparisons) == 0

    passed = has_comparisons and has_words and has_both_types and no_empty

    issues = []
    if not has_comparisons:
        issues.append("No comparisons found")
    if not has_words:
        issues.append("No words found")
    if not has_both_types:
        issues.append(f"Expected 2 comparison types, found {comparison_types}")
    if not no_empty:
        issues.append(
            f"{len(empty_comparisons)} comparisons have no words: "
            f"{[f'{c[1]} (id={c[0]})' for c in empty_comparisons[:3]]}"
        )

    return dg.AssetCheckResult(
        passed=passed,
        description=f"Stored {comparison_count} comparisons with {word_count} words"
        if passed
        else "; ".join(issues),
        metadata={
            "comparison_count": comparison_count,
            "word_count": word_count,
            "comparison_types": comparison_types,
            "empty_comparisons": len(empty_comparisons),
        },
    )


@dg.asset_check(asset=wlo_comparisons)
def wlo_scores_valid(
    _: dg.AssetCheckExecutionContext, analytics_db: AnalyticsDB
) -> dg.AssetCheckResult:
    """Check that WLO scores are reasonable and not all zeros."""
    with analytics_db.get_connection() as conn:
        cursor = conn.execute("SELECT COUNT(*) FROM wlo_words WHERE wlo_score = 0")
        zero_scores = cursor.fetchone()[0]

        cursor = conn.execute("SELECT COUNT(*) FROM wlo_words")
        total_words = cursor.fetchone()[0]

        cursor = conn.execute(
            "SELECT AVG(ABS(wlo_score)), MIN(wlo_score), MAX(wlo_score) FROM wlo_words"
        )
        avg_abs, min_score, max_score = cursor.fetchone()

    if total_words == 0:
        return dg.AssetCheckResult(
            passed=False,
            description="No words found in wlo_words table",
        )

    zero_percent = (zero_scores / total_words * 100) if total_words > 0 else 0
    has_variation = avg_abs > 0.01  # Average absolute score should be meaningful
    not_too_many_zeros = zero_percent < 50  # Less than 50% zeros

    passed = has_variation and not_too_many_zeros

    issues = []
    if not has_variation:
        issues.append(f"Average absolute score too low: {avg_abs:.4f}")
    if not not_too_many_zeros:
        issues.append(f"{zero_percent:.1f}% of scores are zero")

    return dg.AssetCheckResult(
        passed=passed,
        description=f"Scores valid: avg={avg_abs:.3f}, range=[{min_score:.3f}, {max_score:.3f}], {zero_percent:.1f}% zeros"
        if passed
        else "; ".join(issues),
        metadata={
            "total_words": total_words,
            "zero_scores": zero_scores,
            "zero_percent": round(zero_percent, 2),
            "avg_abs_score": round(avg_abs, 4) if avg_abs else 0,
            "min_score": round(min_score, 4) if min_score else 0,
            "max_score": round(max_score, 4) if max_score else 0,
        },
    )
