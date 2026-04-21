import json
import requests
import pandas as pd
from datetime import datetime, UTC
from pathlib import Path
import xml.etree.ElementTree as ET
from email.utils import format_datetime

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
        "type", "identifier", "reference", "callccm2Id",
        "title", "status", "caName", "projectAcronym",
        "startDate", "description", "deadlineDate",
        "deadlineModel", "frameworkProgramme", "typesOfAction",
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

# --- SAVE CSV ---
if not all_records:
    raise ValueError("No records found")

df = pd.json_normalize(all_records)
df["fetched_at"] = datetime.now(UTC)
df.to_csv(DATA_FILE, index=False)

# --- HELPERS ---
def safe_get(row, *keys):
    for key in keys:
        val = row.get(key)

        if val is None:
            continue

        if isinstance(val, (list, tuple)) or hasattr(val, "size"):
            if len(val) == 0:
                continue
            val = val[0]

        if pd.notna(val) and val != "":
            return str(val)

    return ""

# --- CREATE RSS ---
rss = ET.Element("rss", version="2.0")
channel = ET.SubElement(rss, "channel")

ET.SubElement(channel, "title").text = "EU Funding Calls"
ET.SubElement(channel, "link").text = "https://ec.europa.eu/info/funding-tenders/opportunities/portal"
ET.SubElement(channel, "description").text = "Latest EU funding opportunities (2021–2027)"
ET.SubElement(channel, "language").text = "en"

for _, row in df.iterrows():
    title = safe_get(row, "content", "metadata.title")
    identifier = safe_get(row, "metadata.identifier")
    url = safe_get(row, "metadata.url", "url")
    description = safe_get(row, "metadata.description", "description")

    if not title and not identifier:
        continue

    item = ET.SubElement(channel, "item")

    ET.SubElement(item, "title").text = title or "No title"
    ET.SubElement(item, "link").text = url
    ET.SubElement(item, "description").text = description
    ET.SubElement(item, "guid").text = identifier or url

    start_date = safe_get(row, "metadata.startDate")
    if start_date:
        try:
            dt = pd.to_datetime(start_date, utc=True)
            ET.SubElement(item, "pubDate").text = format_datetime(dt.to_pydatetime())
        except Exception:
            pass

tree = ET.ElementTree(rss)
tree.write(RSS_FILE, encoding="utf-8", xml_declaration=True)

print("RSS updated successfully")
