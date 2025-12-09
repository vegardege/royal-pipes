import dagster as dg

from royal_pipes.load import load_official_speeches


@dg.asset
async def official_speeches(context: dg.AssetExecutionContext) -> dict[int, str]:
    context.log.info("Downloading recent speeches from the official source")
    speeches = await load_official_speeches()

    if not speeches:
        raise ValueError("No speeches found, inspect site for changes")

    context.log.info(f"Found {len(speeches)} speeches")
    return speeches
