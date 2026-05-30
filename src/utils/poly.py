import json
from datetime import datetime, timedelta, timezone
import pandas as pd
import requests

GAMMA = "https://gamma-api.polymarket.com"
CLOB = "https://clob.polymarket.com"

HEADERS = {
    "User-Agent": "research-script/0.1 (you@example.com)"
}

def _maybe_json_load(value):
    if isinstance(value, str):
        try:
            return json.loads(value)
        except json.JSONDecodeError:
            return value
    return value

def fetch_event_by_slug(slug: str, headers: dict | None = None) -> dict:
    headers = headers or HEADERS
    resp = requests.get(
        f"{GAMMA}/events",
        params={"slug": slug},
        headers=headers,
        timeout=20,
    )
    resp.raise_for_status()
    data = resp.json()

    if isinstance(data, list):
        if not data:
            raise ValueError(f"No event found for slug '{slug}'")
        return data[0]

    if not data:
        raise ValueError(f"No event found for slug '{slug}'")

    return data

def extract_market_tokens(event: dict) -> list[dict]:
    rows = []

    for market in event.get("markets", []):
        outcomes = _maybe_json_load(market.get("outcomes"))
        clob_ids = _maybe_json_load(market.get("clobTokenIds"))

        if not outcomes or not clob_ids:
            continue
        if len(outcomes) != len(clob_ids):
            continue

        token_map = dict(zip(outcomes, clob_ids))
        rows.append(
            {
                "market_question": market.get("question"),
                "market_slug": market.get("slug"),
                "token_map": token_map,
            }
        )

    if not rows:
        raise ValueError("No markets with token IDs found in event.")

    return rows

def get_yes_token_from_event(event: dict, market_question_contains: str | None = None) -> tuple[str, str]:
    markets = extract_market_tokens(event)

    if market_question_contains:
        needle = market_question_contains.lower()
        markets = [
            m for m in markets
            if needle in (m["market_question"] or "").lower()
        ]
        if not markets:
            raise ValueError(
                f"No market question matched substring: {market_question_contains!r}"
            )

    for m in markets:
        token_map = m["token_map"]
        if "Yes" in token_map:
            return token_map["Yes"], m["market_question"]

    raise ValueError("No 'Yes' token found in selected markets.")

def fetch_price_history_window(
    token_id: str,
    start_ts: int,
    end_ts: int,
    interval: str = "1h",
    fidelity: int = 60,
    headers: dict | None = None,
) -> pd.DataFrame:
    headers = headers or HEADERS

    resp = requests.get(
        f"{CLOB}/prices-history",
        params={
            "market": token_id,
            "startTs": start_ts,
            "endTs": end_ts,
            "interval": interval,
            "fidelity": fidelity,
        },
        headers=headers,
        timeout=20,
    )
    resp.raise_for_status()
    data = resp.json()

    history = data.get("history", [])
    df = pd.DataFrame(history)

    if df.empty:
        return df

    df["timestamp"] = pd.to_datetime(df["t"], unit="s", utc=True)
    df = df.rename(columns={"p": "price"}).sort_values("timestamp")
    return df[["timestamp", "price"]]

def fetch_price_history_paginated(
    token_id: str,
    days: int = 30,
    chunk_days: int = 7,
    interval: str = "1h",
    fidelity: int = 60,
    headers: dict | None = None,
) -> pd.DataFrame:
    headers = headers or HEADERS

    end_dt = datetime.now(timezone.utc)
    start_dt = end_dt - timedelta(days=days)

    all_chunks = []
    chunk_end = end_dt

    while chunk_end > start_dt:
        chunk_start = max(start_dt, chunk_end - timedelta(days=chunk_days))

        df_chunk = fetch_price_history_window(
            token_id=token_id,
            start_ts=int(chunk_start.timestamp()),
            end_ts=int(chunk_end.timestamp()),
            interval=interval,
            fidelity=fidelity,
            headers=headers,
        )

        if not df_chunk.empty:
            all_chunks.append(df_chunk)

        # move window backward
        chunk_end = chunk_start

    if not all_chunks:
        return pd.DataFrame(columns=["timestamp", "price"])

    out = (
        pd.concat(all_chunks, ignore_index=True)
        .drop_duplicates(subset="timestamp")
        .sort_values("timestamp")
        .reset_index(drop=True)
    )
    return out

def get_price_series_from_slug(
    slug: str,
    days: int = 30,
    chunk_days: int = 7,
    interval: str = "1h",
    fidelity: int = 60,
    market_question_contains: str | None = None,
    headers: dict | None = None,
) -> tuple[pd.DataFrame, dict]:
    headers = headers or HEADERS

    event = fetch_event_by_slug(slug, headers=headers)
    token_id, market_question = get_yes_token_from_event(
        event,
        market_question_contains=market_question_contains,
    )

    df = fetch_price_history_paginated(
        token_id=token_id,
        days=days,
        chunk_days=chunk_days,
        interval=interval,
        fidelity=fidelity,
        headers=headers,
    )

    meta = {
        "event_title": event.get("title"),
        "event_slug": event.get("slug"),
        "market_question": market_question,
        "yes_token_id": token_id,
    }

    return df, meta

import requests

import requests

def get_event_slugs_paginated(keyword=None, pages=10, limit=100):
    url = "https://gamma-api.polymarket.com/events"
    results = []

    for page in range(pages):
        params = {
            "limit": limit,
            "offset": page * limit,
            "active": "true",
            "closed": "false",
            "order": "volume_24hr",
            "ascending": "false",
        }

        resp = requests.get(url, params=params, timeout=20)
        resp.raise_for_status()
        data = resp.json()

        for event in data:
            title = event.get("title", "")
            slug = event.get("slug", "")

            if keyword is None or keyword.lower() in title.lower():
                results.append({
                    "slug": slug,
                    "title": title,
                    "volume_24hr": event.get("volume24hr"),
                    "volume": event.get("volume"),
                    "liquidity": event.get("liquidity"),
                    "active": event.get("active"),
                    "closed": event.get("closed"),
                })

    return results