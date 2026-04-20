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
        "type", "reference", "callccm2Id",
        "title", "status", "caName", "projectAcronym",
        "startDate", "description", "deadlineDate",
        "deadlineModel", "frameworkProgramme", "typesOfAction",
    ],
}

files = {
    k: ("blob", json.dumps(v), "application/json")
    for k, v in payload.items()
}

# --- FETCH DATA (PAGINATED) ---
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

# --- SAVE CSV ---
if all_records:
    df = pd.json_normalize(all_records)
    df["fetched_at"] = datetime.now(UTC)

    print("Columns in dataframe:")
    print(df.columns.tolist())

    # Ensure startDate exists
    if "startDate" not in df.columns:
        print("⚠️ 'startDate' column missing. Using fallback.")
        df["startDate"] = None

    # Convert + sort safely
    df["startDate"] = pd.to_datetime(df["startDate"], errors="coerce")
    df = df.sort_values("startDate", ascending=False, na_position="last")

    df.to_csv(DATA_FILE, index=False)
    print(f"Saved {len(df)} records → {DATA_FILE}")

else:
    print("No records found.")
    df = pd.DataFrame()

# --- RSS GENERATION ---
def create_rss(df, output_file):
    rss = Element("rss", version="2.0")
    channel = SubElement(rss, "channel")

    SubElement(channel, "title").text = "EU Funding Calls (2021–2027)"
    SubElement(channel, "link").text = "https://ec.europa.eu/info/funding-tenders/opportunities/portal"
    SubElement(channel, "description").text = "Latest open EU funding calls"
    SubElement(channel, "lastBuildDate").text = datetime.now(UTC).strftime("%a, %d %b %Y %H:%M:%S GMT")

    for _, row in df.iterrows():
        item = SubElement(channel, "item")

        reference = row.get("reference", "N/A")
        title = row.get("title", "No title")
        deadline = row.get("deadlineDate", "")

        SubElement(item, "title").text = f"{reference} - {title} (Deadline: {deadline})"

        link = f"https://ec.europa.eu/info/funding-tenders/opportunities/portal/screen/opportunities/topic-details/{reference}"
        SubElement(item, "link").text = link

        SubElement(item, "guid").text = reference

        desc = row.get("description", "No description")
        SubElement(item, "description").text = desc[:1000]

        pub_date = row.get("startDate")
        if pd.notna(pub_date):
            try:
                dt = pd.to_datetime(pub_date)
                SubElement(item, "pubDate").text = dt.strftime("%a, %d %b %Y %H:%M:%S GMT")
            except:
                pass

    ElementTree(rss).write(output_file, encoding="utf-8", xml_declaration=True)
    print(f"RSS feed updated → {output_file}")

# --- CREATE RSS (always create file) ---
create_rss(df, RSS_FILE)
