import pandas as pd


def validate_and_resample_panel(panel_df, freq="1h"):
    panel_df = panel_df.copy()
    panel_df["timestamp"] = pd.to_datetime(panel_df["timestamp"], utc=True)

    cleaned = []

    for slug, g in panel_df.groupby("market_slug"):
        print("\n" + "=" * 80)
        print("Market:", slug)
        print("Rows:", len(g))
        print("Time range:", g["timestamp"].min(), "→", g["timestamp"].max())

        g = g.sort_values("timestamp").drop_duplicates("timestamp").copy()
        g["delta"] = g["timestamp"].diff()

        print("\nUnique time deltas:")
        print(g["delta"].value_counts().head())

        expected = pd.Timedelta(freq)
        gaps = g[g["delta"] > expected]

        print(f"\nNumber of gaps > {freq}: {len(gaps)}")
        if len(gaps) > 0:
            print("\nExample gaps:")
            print(gaps[["timestamp", "delta"]].head())

        print("\nDuplicate timestamps:", g["timestamp"].duplicated().sum())

        # keep non-time metadata columns
        meta_cols = [c for c in g.columns if c not in ["timestamp", "price", "delta"]]

        g_clean = (
            g.set_index("timestamp")
            .resample(freq)
            .last()
            .ffill()
            .reset_index()
        )

        # restore market_slug if needed
        g_clean["market_slug"] = slug

        print("\nAfter resampling:")
        print("Rows:", len(g_clean))

        print("\nFinal time delta check:")
        print(g_clean["timestamp"].diff().value_counts().head())

        cleaned.append(g_clean)

    return pd.concat(cleaned, ignore_index=True)