from datetime import timedelta

import dagster as dg
from pydantic import Field

from royal_pipes.config import speeches_dir
from royal_pipes.defs.resources import AnalyticsDB
from royal_pipes.extract import (
    BETTING_URL,
    load_danskespil_odds,
    load_leipzig_corpus,
    load_monarchs,
    load_official_speech,
    load_official_speeches,
)
from royal_pipes.transform import compute_odds_counts, compute_speeches, compute_word_counts

year_partitions = dg.DynamicPartitionsDefinition(name="years")


@dg.asset
async def kongehuset_speeches(context: dg.AssetExecutionContext) -> dict[int, str]:
    """Discover all speeches published to the official source."""
    context.log.info("Downloading recent speeches from the official source")
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


@dg.asset(
    deps=[dg.AssetDep("kongehuset_speech")],
    auto_materialize_policy=dg.AutoMaterializePolicy.eager(),
)
def word_count(
    context: dg.AssetExecutionContext,
    analytics_db: AnalyticsDB,
) -> None:
    """Compute word counts across all speeches and store in SQLite.

    Reads all speech files from the XDG speeches directory and computes word
    frequencies per year. Results are stored in the analytics database.

    Table: word_count (year, word, count)
    """
    context.log.info("Computing word counts from all speeches")
    word_counts_data = compute_word_counts(speeches_dir())
    context.log.info(f"Found {len(word_counts_data)} word-year pairs")

    analytics_db.replace_word_count(word_counts_data)
    context.log.info(f"Stored word counts to {analytics_db.db_path}")


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
        description="Which market section to scrape (0=word list, 1=over/under, 2=combinations)",
    )


@dg.asset
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

    # Log some interesting stats
    if odds:
        sorted_odds = sorted(odds.items(), key=lambda x: x[1])
        context.log.info(f"Most likely:  {sorted_odds[0]}")
        context.log.info(f"Least likely: {sorted_odds[-1]}")

    return odds


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


@dg.asset_check(asset=kongehuset_speech)
def speeches_minimum_length(_: dg.AssetCheckExecutionContext) -> dg.AssetCheckResult:
    """Check that all speech files have minimum expected length."""
    speeches_path = speeches_dir()
    min_length = 1000
    failed_speeches = []

    for speech_file in sorted(speeches_path.glob("*.txt")):
        content = speech_file.read_text(encoding="utf-8")
        if len(content) < min_length:
            failed_speeches.append((speech_file.stem, len(content)))

    passed = len(failed_speeches) == 0

    if passed:
        total_files = len(list(speeches_path.glob("*.txt")))
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
def speeches_contain_danmark(_: dg.AssetCheckExecutionContext) -> dg.AssetCheckResult:
    """Check that all speeches mention Danmark."""
    speeches_path = speeches_dir()
    missing_danmark = []

    for speech_file in sorted(speeches_path.glob("*.txt")):
        content = speech_file.read_text(encoding="utf-8").lower()
        if "danmark" not in content:
            missing_danmark.append(speech_file.stem)

    passed = len(missing_danmark) == 0

    if passed:
        total_files = len(list(speeches_path.glob("*.txt")))
        description = f"All {total_files} speeches contain 'danmark'"
    else:
        description = f"{len(missing_danmark)} speeches missing 'danmark': {sorted(missing_danmark)}"

    return dg.AssetCheckResult(
        passed=passed,
        description=description,
        metadata={
            "missing_count": len(missing_danmark),
            "missing_years": sorted(missing_danmark),
        },
    )


@dg.asset_check(asset=word_count)
def word_count_contains_danmark(
    _: dg.AssetCheckExecutionContext, analytics_db: AnalyticsDB
) -> dg.AssetCheckResult:
    """Check that word counts include 'denmark' for all years."""
    with analytics_db.get_connection() as conn:
        cursor = conn.execute("SELECT DISTINCT year FROM word_count ORDER BY year")
        years = [row[0] for row in cursor.fetchall()]

        cursor = conn.execute(
            "SELECT DISTINCT year FROM word_count WHERE word = 'danmark' ORDER BY year"
        )
        years_with_danmark = [row[0] for row in cursor.fetchall()]

    missing_years = set(years) - set(years_with_danmark)

    passed = len(missing_years) == 0

    return dg.AssetCheckResult(
        passed=passed,
        description=f"All {len(years)} years contain 'danmark'"
        if passed
        else f"{len(missing_years)} years missing 'danmark': {sorted(missing_years)}",
        metadata={
            "total_years": len(years),
            "years_with_danmark": len(years_with_danmark),
            "missing_years": sorted(missing_years) if missing_years else [],
        },
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

    Stores results in the 'odds_count' table with schema:
    - year INTEGER
    - word TEXT (the original odds word)
    - count INTEGER

    This allows comparing betting odds against historical frequency.

    Dependencies:
    - kongehuset_speech: Needs the historical speeches
    - odds: Needs the list of odds words to count
    """
    context.log.info("Computing odds counts from speeches")

    # Get list of odds words from database
    with analytics_db.get_connection() as conn:
        cursor = conn.execute("SELECT word FROM odds")
        odds_words = [row[0] for row in cursor.fetchall()]

    context.log.info(f"Counting {len(odds_words)} odds words across all speeches")

    # Compute counts across all speeches
    odds_counts_data = compute_odds_counts(speeches_dir(), odds_words)

    context.log.info(f"Found {len(odds_counts_data)} word-year pairs")

    # Store in database
    analytics_db.replace_odds_counts(odds_counts_data)
    context.log.info(f"Stored odds counts to {analytics_db.db_path}")


@dg.asset_check(asset=danskespil_odds)
def betting_odds_minimum_count(
    _: dg.AssetCheckExecutionContext, danskespil_odds: dict[str, float]
) -> dg.AssetCheckResult:
    """Check that we scraped a reasonable number of betting options."""
    min_count = 100  # Expect at least 100 words to bet on
    count = len(danskespil_odds)
    passed = count >= min_count

    return dg.AssetCheckResult(
        passed=passed,
        description=f"Found {count} betting options (minimum: {min_count})"
        if passed
        else f"Only found {count} betting options, expected at least {min_count}",
        metadata={"count": count, "min_count": min_count},
    )


@dg.asset
async def wikipedia_monarchs(context: dg.AssetExecutionContext) -> list[tuple[str, int, int | None]]:
    """Download the list of Danish monarchs from Wikipedia.

    Returns monarchs from 1913 onwards (when New Year speeches began).
    Each monarch is represented as (name, start_year, end_year) where
    end_year is None for the current monarch.
    """
    context.log.info("Downloading monarchs from Wikipedia")
    monarchs_data = await load_monarchs()

    context.log.info(f"Found {len(monarchs_data)} monarchs from 1913 onwards")

    for name, start_year, end_year in monarchs_data:
        context.log.info(f"  {name}: {start_year}–{end_year or 'present'}")

    return monarchs_data


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

    # Get list of years from speech files
    speeches_path = speeches_dir()
    years = sorted([int(f.stem) for f in speeches_path.glob("*.txt")])

    context.log.info(f"Found {len(years)} speeches")

    # Transform: combine years with monarch data
    speeches_data = compute_speeches(years, wikipedia_monarchs)

    context.log.info(f"Matched {len(speeches_data)} speeches with monarch data")

    # Log some samples
    if speeches_data:
        context.log.info("Sample speeches:")
        for year, monarch in speeches_data[:3]:
            context.log.info(f"  {year}: {monarch}")

    # Store results
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
        # Find speeches with missing or empty monarch
        cursor = conn.execute("""
            SELECT year
            FROM speech
            WHERE monarch IS NULL OR monarch = ''
            ORDER BY year
        """)
        missing_monarchs = [row[0] for row in cursor.fetchall()]

        # Get distribution of monarchs
        cursor = conn.execute("""
            SELECT monarch, COUNT(*) as count
            FROM speech
            WHERE monarch IS NOT NULL AND monarch != ''
            GROUP BY monarch
            ORDER BY count DESC
        """)
        monarch_distribution = {row[0]: row[1] for row in cursor.fetchall()}

        # Get total count of speeches
        cursor = conn.execute("SELECT COUNT(*) FROM speech")
        total_speeches = cursor.fetchone()[0]

    passed = len(missing_monarchs) == 0

    if passed:
        description = f"All {total_speeches} speeches have monarchs assigned"
    else:
        description = f"{len(missing_monarchs)} speeches missing monarchs: {missing_monarchs}"

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


@dg.asset
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

    # Log some sample statistics
    if corpus:
        total_count = sum(count for _, count in corpus)
        context.log.info(f"Total word occurrences: {total_count:,}")
        context.log.info(f"Top 5 most frequent words: {corpus[:5]}")

    return corpus


@dg.asset(
    auto_materialize_policy=dg.AutoMaterializePolicy.eager(),
)
def corpus(
    context: dg.AssetExecutionContext,
    analytics_db: AnalyticsDB,
    leipzig_corpus: list[tuple[str, int]],
) -> None:
    """Store Leipzig corpus word frequencies in the SQLite database.

    Stores the Leipzig Corpora Collection data in the 'corpus' table with schema:
    - word TEXT (primary key)
    - count INTEGER

    This table is atomically replaced each time the asset is materialized.
    """
    context.log.info(f"Storing {len(leipzig_corpus)} words to database")
    analytics_db.replace_corpus(leipzig_corpus)
    context.log.info(f"Stored corpus to {analytics_db.db_path}")


@dg.asset_check(asset=leipzig_corpus)
def corpus_minimum_words(
    _: dg.AssetCheckExecutionContext, leipzig_corpus: list[tuple[str, int]]
) -> dg.AssetCheckResult:
    """Check that the Leipzig corpus contains a minimum number of words."""
    min_words = 100000  # Expect at least 100k words from a 1M corpus
    count = len(leipzig_corpus)
    passed = count >= min_words

    return dg.AssetCheckResult(
        passed=passed,
        description=f"Found {count:,} words in corpus (minimum: {min_words:,})"
        if passed
        else f"Only found {count:,} words in corpus, expected at least {min_words:,}",
        metadata={"count": count, "min_count": min_words},
    )
