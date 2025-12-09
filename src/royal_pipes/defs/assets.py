import dagster as dg

from royal_pipes.load import load_official_speech, load_official_speeches

official_year_partitions = dg.DynamicPartitionsDefinition(name="official_years")


@dg.asset
async def official_speeches(context: dg.AssetExecutionContext) -> dict[int, str]:
    """Discover all speeches published to the official source."""
    context.log.info("Downloading recent speeches from the official source")
    speeches = await load_official_speeches()

    if not speeches:
        raise ValueError("No speeches found, inspect site for changes")

    context.log.info(f"Found {len(speeches)} speeches")

    current_partitions = set(context.instance.get_dynamic_partitions("official_years"))
    new_partitions = [str(year) for year in speeches if year not in current_partitions]

    if new_partitions:
        context.instance.add_dynamic_partitions("official_years", new_partitions)
        context.log.info(f"Found {len(new_partitions)} new years")

    return speeches


@dg.asset(partitions_def=official_year_partitions)
async def official_speech_content(
    context: dg.AssetExecutionContext,
    official_speeches: dict[int, str],
) -> str:
    """Get the content of a single yearly speech from the official source."""
    year = int(context.partition_key)
    url = official_speeches.get(year)

    context.log.info(f"Processing speech for year {year} from {url}")
    content = await load_official_speech(url)
    context.log.info(f"Found speech:\n\n{content}")

    return content
