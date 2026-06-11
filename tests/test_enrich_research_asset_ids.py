from pathlib import Path

import pandas as pd

from scripts.enrich_research_asset_ids import enrich_research_price_parquet


def test_enrich_research_price_parquet_fills_missing_asset_ids(tmp_path: Path) -> None:
    data_root = tmp_path / "data"
    out_path = (
        data_root / "silver" / "research_daily_prices" / "month=2026-02" / "date=2026-02-13.parquet"
    )
    out_path.parent.mkdir(parents=True, exist_ok=True)
    pd.DataFrame(
        {
            "symbol": ["AAPL", "ZZZ", "UNMAPPED"],
            "trade_date": ["2026-02-13"] * 3,
            "close": [100.0, 10.0, 1.0],
        }
    ).to_parquet(out_path, index=False)

    stats = enrich_research_price_parquet(
        data_root=data_root,
        asset_id_map_df=pd.DataFrame(
            {
                "symbol": ["AAPL", "ZZZ"],
                "asset_id": [1, 3],
            }
        ),
        dry_run=False,
    )

    assert stats == {
        "files_scanned": 1,
        "files_rewritten": 1,
        "rows_scanned": 3,
        "rows_filled": 2,
        "unmapped_rows": 1,
    }
    actual = pd.read_parquet(out_path).sort_values("symbol").reset_index(drop=True)
    assert int(actual.loc[0, "asset_id"]) == 1
    assert pd.isna(actual.loc[1, "asset_id"])
    assert int(actual.loc[2, "asset_id"]) == 3
