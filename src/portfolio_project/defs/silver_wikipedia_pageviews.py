import os
from datetime import datetime
from pathlib import Path

from dagster import AssetExecutionContext, DailyPartitionsDefinition, asset

from portfolio_project.defs.silver_assets import silver_alpaca_assets
from portfolio_project.defs.wikipedia_pageviews import bronze_wikipedia_pageviews


PARTITIONS_START_DATE = os.getenv("ALPACA_PARTITIONS_START_DATE", "2020-01-01")
SILVER_WIKIPEDIA_PARTITIONS = DailyPartitionsDefinition(start_date=PARTITIONS_START_DATE)
DATA_ROOT = Path(os.getenv("PORTFOLIO_DATA_DIR", "data"))


@asset(
    name="silver_wikipedia_pageviews",
    partitions_def=SILVER_WIKIPEDIA_PARTITIONS,
    deps=[bronze_wikipedia_pageviews, silver_alpaca_assets],
    required_resource_keys={"duckdb"},
)
def silver_wikipedia_pageviews(context: AssetExecutionContext) -> None:
    """
    Normalize Wikipedia pageviews into silver parquet with asset_id.
    """
    partition_date = datetime.strptime(context.partition_key, "%Y-%m-%d").date()
    bronze_path = (
        DATA_ROOT
        / "bronze"
        / "wikipedia_pageviews"
        / f"date={partition_date}"
        / "pageviews.parquet"
    )
    if not bronze_path.exists():
        context.log.warning("Bronze Wikipedia pageviews parquet not found at %s", bronze_path)
        return

    con = context.resources.duckdb
    try:
        con.execute("SELECT 1 FROM silver.assets LIMIT 1")
    except Exception as exc:
        context.log.warning("Silver assets table missing or unreadable: %s", exc)
        return

    silver_root = DATA_ROOT / "silver" / "wikipedia_pageviews" / f"date={context.partition_key}"
    silver_root.mkdir(parents=True, exist_ok=True)
    out_path = silver_root / "pageviews.parquet"

    bronze_path_sql = bronze_path.as_posix().replace("'", "''")
    view_date_expr = (
        "COALESCE("
        "CAST(try_strptime(CAST(bronze.view_date AS VARCHAR), '%Y%m%d') AS DATE), "
        "CAST(try_strptime(CAST(bronze.view_date AS VARCHAR), '%Y-%m-%d') AS DATE)"
        ")"
    )
    base_select = f"""
        SELECT
            COALESCE(assets_symbol.asset_id, assets_name.asset_id) AS asset_id,
            bronze.granularity,
            {view_date_expr} AS view_date,
            bronze.views,
            bronze.ingested_ts
        FROM read_parquet('{bronze_path_sql}') AS bronze
        LEFT JOIN silver.assets AS assets_symbol
            ON upper(bronze.symbol) = upper(assets_symbol.symbol)
        LEFT JOIN silver.assets AS assets_name
            ON assets_symbol.asset_id IS NULL
            AND lower(trim(bronze.company_name)) = lower(trim(assets_name.name))
    """

    counts = con.execute(
        f"""
        SELECT
            count(*) AS row_count,
            sum(CASE WHEN asset_id IS NULL THEN 1 ELSE 0 END) AS missing_asset_id_count
        FROM ({base_select}) AS enriched
        """
    ).fetchone()
    if counts is None or counts[0] == 0:
        context.log.warning("No Wikipedia pageviews rows found for partition %s.", context.partition_key)
        return

    out_path_sql = out_path.as_posix().replace("'", "''")
    con.execute(
        f"""
        COPY (
            {base_select}
        )
        TO '{out_path_sql}' (FORMAT PARQUET)
        """
    )

    context.add_output_metadata(
        {
            "parquet_path": str(out_path),
            "row_count": int(counts[0]),
            "missing_asset_id_count": int(counts[1]) if counts[1] is not None else 0,
        }
    )
