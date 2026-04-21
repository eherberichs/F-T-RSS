import json
import requests
import pandas as pd
from datetime import datetime, UTC
from pathlib import Path
from xml.etree.ElementTree import Element, SubElement, ElementTree

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
        "metadata.identifier",
        "summary",
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
all_records = []
page = 1

while True:
    PARAMS["pageNumber"] = page

    res = requests.post(API_URL, params=PARAMS, files=files, headers=HEADERS)
    res.raise_for_status()
    data = res.json()

    records = data.get("results", [])
    if not records:
        break

    all_records.extend(records)
    print(f"Page {page}: {len(records)} records")

    if len(records) < PARAMS["pageSize"]:
        break

    page += 1

# --- DATAFRAME ---
if all_records:
    df = pd.json_normalize(all_records)
    df["fetched_at"] = datetime.now(UTC)

    print("Columns:", df.columns.tolist())

    # Ensure expected columns exist
    expected_cols = [
        "metadata.identifier",
        "summary",
        "url",
        "startDate",
        "deadlineDate",
        "description",
    ]

    for col in expected_cols:
        if col not in df.columns:
            df[col] = None

    df["startDate"] = pd.to_datetime(df["startDate"], errors="coerce")
    df = df.sort_values("startDate", ascending=False, na_position="last")

    df.to_csv(DATA_FILE, index=False)
    print(f"Saved {len(df)} records")

else:
    df = pd.DataFrame()
    print("No records found")

# --- RSS GENERATION ---
def create_rss(df, output_file):
    rss = Element("rss", version="2.0")
    channel = SubElement(rss, "channel")

    SubElement(channel, "title").text = "EU Funding Calls"
    SubElement(channel, "link").text = "https://ec.europa.eu/info/funding-tenders/opportunities/portal"
    SubElement(channel, "description").text = "Latest EU funding calls"

    for _, row in df.iterrows():
        reference = str(row.get("metadata.identifier") or "").strip()
        summary = str(row.get("summary") or "").strip()
        description = str(row.get("description") or "").strip()

        # fallback if summary missing
        title = summary if summary else description[:120]

        # Skip invalid rows
        if not reference or not title:
            continue

        item = SubElement(channel, "item")

        url = f"https://ec.europa.eu/info/funding-tenders/opportunities/portal/screen/opportunities/topic-details/{reference}"

        SubElement(item, "guid").text = reference
        SubElement(item, "title").text = title
        SubElement(item, "link").text = url

        SubElement(item, "description").text = description[:500]

    ElementTree(rss).write(output_file, encoding="utf-8", xml_declaration=True)
    print("RSS created")

# --- RUN ---
create_rss(df, RSS_FILE)
