from datetime import timedelta

import dagster as dg
from pydantic import Field

from royal_pipes.config import speeches_dir
from royal_pipes.defs.resources import AnalyticsDB
from royal_pipes.extract import (
    BETTING_URL,
    load_danskespil_odds,
    load_official_speech,
    load_official_speeches,
)
from royal_pipes.transform import compute_word_counts

year_partitions = dg.DynamicPartitionsDefinition(name="years")


@dg.asset
async def official_speeches(context: dg.AssetExecutionContext) -> dict[int, str]:
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
async def official_speech_content(
    context: dg.AssetExecutionContext,
    official_speeches: dict[int, str],
) -> str:
    """Download and store a single speech.

    Stored as YYYY.txt in the XDG data directory. You can manually add files
    to the speeches directory and they will be used instead of re-scraping.
    """
    year = int(context.partition_key)
    if url := official_speeches.get(year):
        context.log.info(f"Scraping speech for {year} from {url}")
        content = await load_official_speech(url)
        context.log.info(f"Scraped {len(content)} characters for {year}")
        return content
    else:
        raise ValueError(f"No URL found for {year}")


@dg.asset(
    deps=[dg.AssetDep("official_speech_content")],
    auto_materialize_policy=dg.AutoMaterializePolicy.eager(),
)
def word_counts(
    context: dg.AssetExecutionContext,
    analytics_db: AnalyticsDB,
) -> None:
    """Compute word counts across all speeches and store in SQLite.

    Reads all speech files from the XDG speeches directory and computes word
    frequencies per year. Results are stored in the analytics database.

    Table: word_counts (year, word, count)
    """
    context.log.info("Computing word counts from all speeches")
    word_counts_data = compute_word_counts(speeches_dir())
    context.log.info(f"Found {len(word_counts_data)} word-year pairs")

    analytics_db.replace_word_counts(word_counts_data)
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


@dg.asset_check(asset=official_speeches)
def speeches_found_count(
    _: dg.AssetCheckExecutionContext, official_speeches: dict[int, str]
) -> dg.AssetCheckResult:
    """Check that at least 10 speeches were found from the official source."""
    min_speeches = 10
    count = len(official_speeches)
    passed = count >= min_speeches

    return dg.AssetCheckResult(
        passed=passed,
        description=f"Found {count} speeches (minimum: {min_speeches})"
        if passed
        else f"Only found {count} speeches, expected at least {min_speeches}",
        metadata={"count": count, "min_count": min_speeches},
    )


@dg.asset_check(asset=official_speech_content)
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


@dg.asset_check(asset=official_speech_content)
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


@dg.asset_check(asset=word_counts)
def word_counts_contains_danmark(
    _: dg.AssetCheckExecutionContext, analytics_db: AnalyticsDB
) -> dg.AssetCheckResult:
    """Check that word counts include 'denmark' for all years."""
    with analytics_db.get_connection() as conn:
        cursor = conn.execute("SELECT DISTINCT year FROM word_counts ORDER BY year")
        years = [row[0] for row in cursor.fetchall()]

        cursor = conn.execute(
            "SELECT DISTINCT year FROM word_counts WHERE word = 'danmark' ORDER BY year"
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
    deps=[dg.AssetDep("danskespil_odds")],
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
