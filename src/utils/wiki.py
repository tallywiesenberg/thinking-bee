"""Utilities for interacting with the Wikipedia API."""

from __future__ import annotations

from pathlib import Path

import time
import requests
import pandas as pd

API = "https://en.wikipedia.org/w/api.php"
HEADERS = {"User-Agent": "thinking-bee/0.1 (t.j.wies@gmail.com)"}
REQUEST_SLEEP_SEC = 0.5
CACHE_DIR = Path("data/wiki_cache/revisions")
CACHE_DIR.mkdir(parents=True, exist_ok=True)


def safe_title(title: str) -> str:
    """Convert a Wikipedia title into a filesystem-safe cache key."""
    return title.replace(":", "__").replace("/", "_").replace(" ", "_")


def wiki_timestamp(value) -> str:
    """Convert a date-like value to a Wikimedia-compatible UTC timestamp."""
    return (
        pd.Timestamp(value).tz_convert("UTC").isoformat()
        if pd.Timestamp(value).tzinfo
        else pd.Timestamp(value, tz="UTC").isoformat()
    )


def wiki_get(url, params, headers, timeout=10, max_retries=5):
    """
    Wikimedia-aware GET with retry logic.
    """

    for attempt in range(max_retries):

        try:
            resp = requests.get(
                url,
                params=params,
                headers=headers,
                timeout=timeout,
            )

            # Handle rate limits
            if resp.status_code == 429:
                retry_after = int(resp.headers.get("Retry-After", 5))

                print(
                    f"429 Too Many Requests. " f"Retrying in {retry_after} seconds..."
                )

                time.sleep(retry_after)
                continue

            resp.raise_for_status()

            data = resp.json()

            # Handle Wikimedia maxlag
            error = data.get("error", {})
            if error.get("code") == "maxlag":

                retry_after = int(resp.headers.get("Retry-After", 5))

                print(f"Maxlag detected. " f"Retrying in {retry_after} seconds...")

                time.sleep(retry_after)
                continue

            if REQUEST_SLEEP_SEC > 0:
                time.sleep(REQUEST_SLEEP_SEC)

            return resp

        except requests.exceptions.RequestException as e:

            wait = min(2**attempt, 60)

            print(f"Request failed ({e}). " f"Retrying in {wait} seconds...")

            time.sleep(wait)

    raise RuntimeError(f"Failed after {max_retries} attempts.")


def get_template_links(template_title: str) -> list[str]:
    """Given a wikipedia template title, returns list of page links related to the template"""
    params = {
        "action": "query",
        "format": "json",
        "prop": "links",
        "titles": template_title,
        "pllimit": "max",
        "plnamespace": 0,  # main/article namespace only
        "formatversion": 2,
        "maxlag": 5,
    }

    titles = []
    while True:
        resp = wiki_get(
            API,
            params=params,
            headers=HEADERS,
            timeout=10,
        )
        data = resp.json()

        pages = data.get("query", {}).get("pages", [])
        if pages:
            links = pages[0].get("links", [])
            titles.extend(link["title"] for link in links)

        if "continue" in data:
            params.update(data["continue"])
        else:
            break

    return sorted(set(titles))


def get_revision_diff(parent_id, rev_id):
    """Fetch the HTML diff between two Wikipedia revisions.

    Args:
        parent_id (int): Revision ID of the parent revision.
        rev_id (int): Revision ID to compare against the parent revision.

    Returns:
        str | None: HTML diff markup if available, otherwise None.
    """
    params = {
        "action": "compare",
        "format": "json",
        "fromrev": parent_id,
        "torev": rev_id,
        "prop": "diff|ids|title",
        "formatversion": "2",
        "maxlag": 5,
    }

    resp = wiki_get(
        API,
        params=params,
        headers=HEADERS,
        timeout=10,
    )
    data = resp.json()

    compare = data.get("compare", {})
    return compare.get("*") or compare.get("body") or compare.get("diff")


def get_revisions(
    page_title,
    talk=False,
    include_diff=False,
    sleep_sec=0.0,
    start=None,
    end=None,
):
    """Fetch Wikipedia revision history for a page.

    Args:
        page_title (str): Title of the Wikipedia page.
        talk (bool, optional): If True, fetch revisions from the talk page. Defaults to False.
        include_diff (bool, optional): If True, include HTML diffs for each revision. Defaults to False.
        sleep_sec (float, optional): Seconds to sleep between API calls when fetching diffs. Defaults to 0.0.
        start: Optional start timestamp/date for the revision window.
        end: Optional end timestamp/date for the revision window.

    Returns:
        pd.DataFrame: DataFrame containing revision history with columns: revid, parentid, timestamp, user,
                      comment, size, page_title, diff_html (if include_diff=True), comment_len, has_reply,
                      has_revert, and size_change.
    """
    if talk and not page_title.startswith("Talk:"):
        page_title = "Talk:" + page_title

    params = {
        "action": "query",
        "format": "json",
        "prop": "revisions",
        "titles": page_title,
        "rvprop": "ids|timestamp|user|comment|size",
        "rvlimit": "max",
        "rvdir": "newer",
        "formatversion": "2",
        "maxlag": 5,
    }

    if start is not None:
        params["rvstart"] = wiki_timestamp(start)

    if end is not None:
        params["rvend"] = wiki_timestamp(end)

    all_revisions = []

    while True:
        resp = wiki_get(
            API,
            params=params,
            headers=HEADERS,
            timeout=10,
        )
        data = resp.json()

        pages = data.get("query", {}).get("pages", [])
        if pages and "revisions" in pages[0]:
            all_revisions.extend(pages[0]["revisions"])

        if "continue" in data:
            params.update(data["continue"])
        else:
            break

    df = pd.DataFrame(all_revisions)

    if df.empty:
        return df

    df["timestamp"] = pd.to_datetime(df["timestamp"], utc=True)
    df["page_title"] = page_title

    if include_diff:
        diffs = []
        for _, row in df.iterrows():
            rev_id = row.get("revid")
            parent_id = row.get("parentid")

            if pd.isna(rev_id) or pd.isna(parent_id) or parent_id == 0:
                diffs.append(None)
            else:
                try:
                    diff_html = get_revision_diff(int(parent_id), int(rev_id))
                    diffs.append(diff_html)
                    if sleep_sec > 0:
                        time.sleep(sleep_sec)
                except Exception:
                    diffs.append(None)

        df["diff_html"] = diffs

    df["comment"] = df["comment"].fillna("")
    df["comment_len"] = df["comment"].str.len()
    df["has_reply"] = (
        df["comment"].str.contains("Reply", case=False, regex=False).astype(int)
    )
    df["has_revert"] = (
        df["comment"].str.contains("revert", case=False, regex=True).astype(int)
    )
    df["size_change"] = df["size"].diff().abs()

    return df


def get_revisions_cached(
    page_title,
    talk=False,
    start=None,
    end=None,
    include_diff=False,
    sleep_sec=0.0,
):
    """Fetch revisions with a local parquet cache.

    The cache key includes page title, talk/article choice, date window, and whether diffs were requested.
    For bulk collection, prefer include_diff=False and fetch diffs in a later filtered pass.
    """
    title_for_cache = page_title
    if talk and not title_for_cache.startswith("Talk:"):
        title_for_cache = "Talk:" + title_for_cache

    start_key = "none" if start is None else safe_title(str(start))
    end_key = "none" if end is None else safe_title(str(end))
    diff_key = "with_diffs" if include_diff else "metadata_only"
    cache_path = (
        CACHE_DIR
        / f"{safe_title(title_for_cache)}_{start_key}_{end_key}_{diff_key}.parquet"
    )

    if cache_path.exists():
        return pd.read_parquet(cache_path)

    df = get_revisions(
        page_title=page_title,
        talk=talk,
        include_diff=include_diff,
        sleep_sec=sleep_sec,
        start=start,
        end=end,
    )

    df.to_parquet(cache_path)
    return df


def format_revisions(revisions: pd.DataFrame | None) -> pd.DataFrame:
    """
    Format revision history into hourly aggregated features.

    Args:
        revisions (pd.DataFrame | None): DataFrame from get_revisions containing revision data, or None.

    Returns:
        pd.DataFrame: Hourly aggregated features including edits, unique_editors, new_editors,
                      total_comment_len, num_replies, num_reverts, sorted by timestamp.
    """
    if revisions is None:
        return pd.DataFrame()
    if revisions.empty:
        return revisions
    df = revisions
    df["timestamp"] = pd.to_datetime(df["timestamp"])
    df = df.sort_values("timestamp")
    df["timestamp"] = df["timestamp"].dt.floor("h")
    edits = df.groupby("timestamp").size().rename("edits")
    unique_editors = df.groupby("timestamp")["user"].nunique().rename("unique_editors")

    df["seen_before"] = df["user"].duplicated()
    df["new_editor"] = (~df["seen_before"]).astype(int)
    new_editors = df.groupby("timestamp")["new_editor"].sum().rename("new_editors")
    total_comment_len = (
        df.groupby("timestamp")["comment_len"].sum().rename("total_comment_len")
    )
    num_replies = df.groupby("timestamp")["has_reply"].sum().rename("num_replies")
    num_reverts = df.groupby("timestamp")["has_revert"].sum().rename("num_reverts")
    features = pd.concat(
        [
            edits,
            unique_editors,
            new_editors,
            total_comment_len,
            num_replies,
            num_reverts,
        ],
        axis=1,
    ).fillna(0)

    return features.reset_index().sort_values("timestamp")


def select_interesting_revisions(
    revisions: pd.DataFrame,
    min_comment_len: int = 100,
    min_size_change: int = 500,
) -> pd.DataFrame:
    """Select revisions that are more likely to be worth fetching diffs for."""
    if revisions is None or revisions.empty:
        return pd.DataFrame()

    required_cols = {"has_revert", "comment_len", "size_change"}
    missing = required_cols - set(revisions.columns)
    if missing:
        raise ValueError(f"Missing required columns: {sorted(missing)}")

    return revisions.loc[
        (revisions["has_revert"] == 1)
        | (revisions["comment_len"] >= min_comment_len)
        | (revisions["size_change"] >= min_size_change)
    ].copy()
