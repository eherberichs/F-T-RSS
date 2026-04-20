import json
import requests
import pandas as pd
from datetime import datetime, UTC
from pathlib import Path
from xml.etree.ElementTree import Element, SubElement, ElementTree
import html

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
        "reference", "title", "startDate",
        "description", "deadlineDate"
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

    if "startDate" not in df.columns:
        df["startDate"] = None

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
    SubElement(channel, "description").text = "Latest EU funding opportunities"
    SubElement(channel, "lastBuildDate").text = datetime.now(UTC).strftime("%a, %d %b %Y %H:%M:%S GMT")

    for _, row in df.iterrows():
        item = SubElement(channel, "item")

        reference = str(row.get("reference", "N/A"))
        title = str(row.get("title", "No title"))
        deadline = str(row.get("deadlineDate", "N/A"))
        summary = str(row.get("description", "No description"))[:1000]

        url = f"https://ec.europa.eu/info/funding-tenders/opportunities/portal/screen/opportunities/topic-details/{reference}"

        # --- RSS STANDARD FIELDS ---
        SubElement(item, "title").text = f"{reference} - {title}"
        SubElement(item, "link").text = url
        SubElement(item, "guid").text = reference

        # --- HTML DESCRIPTION (THIS IS WHAT USERS SEE) ---
        description_html = f"""
        <b>Reference:</b> {html.escape(reference)}<br>
        <b>Deadline:</b> {html.escape(deadline)}<br>
        <b>Link:</b> <a href="{url}">View call</a><br><br>
        {html.escape(summary)}
        """

        SubElement(item, "description").text = description_html

        # --- DATE ---
        pub_date = row.get("startDate")
        if pd.notna(pub_date):
            dt = pd.to_datetime(pub_date)
            SubElement(item, "pubDate").text = dt.strftime("%a, %d %b %Y %H:%M:%S GMT")

    ElementTree(rss).write(output_file, encoding="utf-8", xml_declaration=True)
    print(f"RSS updated → {output_file}")

# --- RUN ---
create_rss(df, RSS_FILE)
