import json
import requests
import pandas as pd
from datetime import datetime, UTC
from pathlib import Path
from xml.etree.ElementTree import Element, SubElement, ElementTree
import html
import email.utils as eut

# --- CONFIG ---
API_URL = "https://api.tech.ec.europa.eu/search-api/prod/rest/search"

PARAMS = {
    "apiKey": "SEDIA",
    "pageSize": 100,
    "text": "***",
}

HEADERS = {
    "User-Agent": "Mozilla/5.0",
    "Accept": "application/json",
}

DATA_FILE = Path("eu_calls_snapshot.csv")
RSS_FILE = Path("eu_calls_feed.xml")

MAX_ITEMS = 100  # limit feed size

payload = {
    "sort": {"order": "DESC", "field": "startDate"},
    "query": {
        "bool": {
            "must": [
                {"terms": {"type": ["1", "2", "8"]}},
                {"terms": {"status": ["31094502"]}},
                {"term": {"programmePeriod": "2021 - 2027"}},
            ]
        }
    },
    "languages": ["en"],
    "displayFields": [
        "reference",
        "summary",
        "title",
        "url",
        "startDate",
        "deadlineDate",
        "description",
    ],
}

files = {
    k: ("blob", json.dumps(v), "application/json")
    for k, v in payload.items()
}


# --- FETCH DATA ---
def fetch_all():
    all_records = []
    page = 1

    while True:
        PARAMS["pageNumber"] = page

        try:
            res = requests.post(API_URL, params=PARAMS, files=files, headers=HEADERS, timeout=30)
            res.raise_for_status()
        except requests.RequestException as e:
            print(f"Fetch error on page {page}: {e}")
            break

        data = res.json()
        records = data.get("results", [])

        if not records:
            break

        all_records.extend(records)
        print(f"Page {page}: {len(records)} records")

        if len(records) < PARAMS["pageSize"]:
            break

        page += 1

    return all_records


# --- NORMALIZE DATA ---
def build_dataframe(records):
    if not records:
        return pd.DataFrame()

    df = pd.json_normalize(records)
    df["fetched_at"] = datetime.now(UTC)

    expected = [
        "reference",
        "summary",
        "title",
        "url",
        "startDate",
        "deadlineDate",
        "description",
    ]

    for col in expected:
        if col not in df.columns:
            df[col] = None

    df["startDate"] = pd.to_datetime(df["startDate"], errors="coerce")

    # deduplicate on reference (stable ID)
    df = df.drop_duplicates(subset=["reference"])

    df = df.sort_values("startDate", ascending=False, na_position="last")

    df.to_csv(DATA_FILE, index=False)
    print(f"Saved {len(df)} records")

    return df


# --- RSS HELPERS ---
def safe_text(value, max_len=None):
    text = html.escape(str(value or "").strip())
    return text[:max_len] if max_len else text


def format_date(dt):
    if pd.isna(dt):
        return None
    return eut.format_datetime(dt)


# --- RSS GENERATION ---
def create_rss(df, output_file):
    rss = Element("rss", version="2.0")
    channel = SubElement(rss, "channel")

    SubElement(channel, "title").text = "EU Funding Calls"
    SubElement(channel, "link").text = "https://ec.europa.eu/info/funding-tenders/opportunities/portal"
    SubElement(channel, "description").text = "Latest EU funding calls"
    SubElement(channel, "lastBuildDate").text = eut.format_datetime(datetime.now(UTC))

    count = 0

    for _, row in df.iterrows():
        if count >= MAX_ITEMS:
            break

        reference = safe_text(row.get("reference"))
        if not reference:
            continue

        title = safe_text(row.get("title") or row.get("summary"), 200)
        description = safe_text(row.get("description"), 1000)

        if not title:
            continue

        url = f"https://ec.europa.eu/info/funding-tenders/opportunities/portal/screen/opportunities/topic-details/{reference}"

        item = SubElement(channel, "item")

        SubElement(item, "guid", isPermaLink="false").text = reference
        SubElement(item, "title").text = title
        SubElement(item, "link").text = url
        SubElement(item, "description").text = description

        pub_date = format_date(row.get("startDate"))
        if pub_date:
            SubElement(item, "pubDate").text = pub_date

        count += 1

    ElementTree(rss).write(output_file, encoding="utf-8", xml_declaration=True)
    print(f"RSS created with {count} items")


# --- RUN ---
if __name__ == "__main__":
    records = fetch_all()
    df = build_dataframe(records)
    create_rss(df, RSS_FILE)
