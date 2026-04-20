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
        "metadata.identifier",
        "summary",
        "url",
        "startDate",
        "deadlineDate",
        "description"  # fallback if summary missing
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
    for col in [
        "metadata.identifier",
        "summary",
        "url",
        "startDate",
        "deadlineDate",
        "description",
    ]:
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

    SubElement(channel, "title").text = "EU Funding Calls (2021–2027)"
    SubElement(channel, "link").text = "https://ec.europa.eu/info/funding-tenders/opportunities/portal"
    SubElement(channel, "description").text = "Latest open EU funding calls"
    SubElement(channel, "lastBuildDate").text = datetime.now(UTC).strftime("%a, %d %b %Y %H:%M:%S GMT")

    for _, row in df.iterrows():
        reference = str(row.get("reference") or "")
        title = str(row.get("title") or "")
        description = str(row.get("description") or "")
        deadline = str(row.get("deadlineDate") or "")
        start_date = row.get("startDate")

        # Skip garbage rows
        if not reference or reference.startswith("COMPETITIVE_CALL"):
            continue

        item = SubElement(channel, "item")

        url = f"https://ec.europa.eu/info/funding-tenders/opportunities/portal/screen/opportunities/topic-details/{reference}"

        # --- RSS fields ---
        SubElement(item, "title").text = f"{reference} - {title}"
        SubElement(item, "link").text = url
        SubElement(item, "guid").text = reference

        # --- readable description ---
        desc_html = f"""
        <b>Reference:</b> {html.escape(reference)}<br>
        <b>Deadline:</b> {html.escape(deadline)}<br>
        <b>Link:</b> <a href="{url}">View call</a><br><br>
        {html.escape(description[:1000])}
        """

        SubElement(item, "description").text = desc_html

        # --- date ---
        if pd.notna(start_date):
            dt = pd.to_datetime(start_date)
            SubElement(item, "pubDate").text = dt.strftime("%a, %d %b %Y %H:%M:%S GMT")

    # ✅ PRETTY PRINT (fixes your “one-line XML” issue)
    import xml.dom.minidom as minidom
    rough_string = ElementTree(rss).write("temp.xml", encoding="utf-8", xml_declaration=True)

    with open("temp.xml", "r", encoding="utf-8") as f:
        parsed = minidom.parseString(f.read())

    with open(output_file, "w", encoding="utf-8") as f:
        f.write(parsed.toprettyxml(indent="  "))

    print(f"RSS updated → {output_file}")

# --- RUN ---
create_rss(df, RSS_FILE)
