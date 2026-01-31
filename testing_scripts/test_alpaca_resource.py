import os
from datetime import datetime, timedelta, timezone

from dagster import build_resources
from dotenv import load_dotenv

# 👇 update this import to wherever your resource actually lives
# e.g. from src.portfolio_project.defs.resources import alpaca_resource
from alpaca_resource import alpaca_resource


def main():
    # Load .env if you're using one
    load_dotenv()

    # Quick check that env vars are present
    api_key = os.getenv("ALPACA_API_KEY")
    secret_key = os.getenv("ALPACA_SECRET_KEY")

    if not api_key or not secret_key:
        raise RuntimeError(
            "Missing ALPACA_API_KEY and/or ALPACA_SECRET_KEY in environment."
        )

    # Build the Dagster resource
    with build_resources({"alpaca": alpaca_resource}) as resources:
        client = resources.alpaca  # This is your AlpacaClient instance

        ## Use this to test the get_assets_df method

        df = client.get_assets_df()

        print(df.shape)

        ## Use this to test the get_bars method

        # Define a small test window (last 3 days, daily bars)
        end_date = datetime.now(timezone.utc)
        start_date = end_date - timedelta(days=3)

        print("Requesting bars from Alpaca...")
        print(f"Symbol: AAPL")
        print(f"Start:  {start_date}")
        print(f"End:    {end_date}")
        print(f"TF:     1d")
        print("-" * 60)

        bars_dict = client.get_bars(
            symbol="AAPL",
            start_date=start_date,
            end_date=end_date,
        )

        # Basic sanity checks / debug output
        if not bars_dict:
            print("No data returned (empty dict).")
            return

        print("\n=== Alpaca resource raw result ===")
        print(f"type(bars_dict): {type(bars_dict)}")

        # Try to show the "size" if it has a length
        if hasattr(bars_dict, "__len__"):
            print(f"len(bars_dict): {len(bars_dict)}")

        # If it's a dict, show a few top-level keys and what their values look like
        if isinstance(bars_dict, dict):
            keys = list(bars_dict.keys())
            print(f"Top-level keys ({len(keys)}): {keys[:10]}")  # up to 10 keys

            print("\nSample of first few key/value pairs:")
            for i, (k, v) in enumerate(bars_dict.items()):
                if i >= 3:  # only show first 3 keys
                    break
                preview = str(v)
                if len(preview) > 300:
                    preview = preview[:300] + " ...[truncated]"
                print(f"- Key: {k} (type={type(v)})")
                print(f"  Value preview: {preview}")

        else:
            # Not a dict – just print a short preview of the object itself
            preview = str(bars_dict)
            if len(preview) > 500:
                preview = preview[:500] + " ...[truncated]"
            print("\nNon-dict result, preview:")
            print(preview)

        print("\n✅ Alpaca resource test completed.")



if __name__ == "__main__":
    main()
