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
        "identifier",
        "title",
        "description",
        "startDate",
        "deadlineDate",
        "status",
        "type",
        "frameworkProgramme",
        "typesOfAction",
        "caName",
        "projectAcronym",
        "callccm2Id",
        "deadlineModel",
    ],
}

files = {
    k: ("blob", json.dumps(v), "application/json")
    for k, v in payload.items()
}

# --- HELPERS ---
def safe_text(value, max_len=None):
    text = html.escape(str(value or "").strip())
    return text[:max_len] if max_len else text

def format_date(dt):
    if pd.isna(dt):
        return None
    return eut.format_datetime(dt)

def merge_cols(df, primary, fallback):
    """Safely merge two columns (no pandas boolean errors)"""
    if primary in df.columns and fallback in df.columns:
        return df[primary].fillna(df[fallback])
    if primary in df.columns:
        return df[primary]
    if fallback in df.columns:
        return df[fallback]
    return None

# --- FETCH DATA ---
def fetch_all():
    all_records = []
    page = 1

    while True:
        PARAMS["pageNumber"] = page

        res = requests.post(API_URL, params=PARAMS, files=files, headers=HEADERS, timeout=30)
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

    return all_records

# --- DATAFRAME ---
def build_dataframe(records):
    if not records:
        print("No records found")
        return pd.DataFrame()

    df = pd.json_normalize(records)
    df["fetched_at"] = datetime.now(UTC)

    print("Columns:", df.columns.tolist())

    # ✅ safe normalization
    df["reference"] = merge_cols(df, "reference", "metadata.REFERENCE")
    df["identifier"] = merge_cols(df, "identifier", "metadata.identifier")
    df["title"] = merge_cols(df, "title", "metadata.title")
    df["description"] = merge_cols(df, "description", "metadata.description")
    df["startDate"] = merge_cols(df, "startDate", "metadata.startDate")
    df["deadlineDate"] = merge_cols(df, "deadlineDate", "metadata.deadlineDate")
    df["status"] = merge_cols(df, "status", "metadata.status")
    df["type"] = merge_cols(df, "type", "metadata.type")
    df["frameworkProgramme"] = merge_cols(df, "frameworkProgramme", "metadata.frameworkProgramme")
    df["typesOfAction"] = merge_cols(df, "typesOfAction", "metadata.typesOfAction")
    df["caName"] = merge_cols(df, "caName", "metadata.caName")
    df["projectAcronym"] = merge_cols(df, "projectAcronym", "metadata.projectAcronym")
    df["callccm2Id"] = merge_cols(df, "callccm2Id", "metadata.callccm2Id")
    df["deadlineModel"] = merge_cols(df, "deadlineModel", "metadata.deadlineModel")

    # dates
    df["startDate"] = pd.to_datetime(df["startDate"], errors="coerce")

    # clean + sort
    df = df.drop_duplicates(subset=["reference"])
    df = df.sort_values("startDate", ascending=False, na_position="last")

    df.to_csv(DATA_FILE, index=False)
    print(f"Saved {len(df)} records")

    return df

# --- RSS GENERATION ---
def create_rss(df, output_file):
    rss = Element("rss", version="2.0")
    channel = SubElement(rss, "channel")

    SubElement(channel, "title").text = "EU Funding Calls"
    SubElement(channel, "link").text = "https://ec.europa.eu/info/funding-tenders/opportunities/portal"
    SubElement(channel, "description").text = "Latest EU funding calls"
    SubElement(channel, "lastBuildDate").text = eut.format_datetime(datetime.now(UTC))

    for _, row in df.iterrows():
        reference = safe_text(row.get("reference"))
        if not reference:
            continue

        title = safe_text(row.get("title") or row.get("identifier"), 200)
        if not title:
            continue

        url = f"https://ec.europa.eu/info/funding-tenders/opportunities/portal/screen/opportunities/topic-details/{reference}"

        item = SubElement(channel, "item")

        SubElement(item, "guid", isPermaLink="false").text = reference
        SubElement(item, "title").text = title
        SubElement(item, "link").text = url

        desc_parts = [
            f"<b>Type:</b> {safe_text(row.get('type'))}",
            f"<b>Identifier:</b> {safe_text(row.get('identifier'))}",
            f"<b>Reference:</b> {reference}",
            f"<b>Call ID:</b> {safe_text(row.get('callccm2Id'))}",
            f"<b>Status:</b> {safe_text(row.get('status'))}",
            f"<b>Programme:</b> {safe_text(row.get('frameworkProgramme'))}",
            f"<b>Action Type:</b> {safe_text(row.get('typesOfAction'))}",
            f"<b>CA Name:</b> {safe_text(row.get('caName'))}",
            f"<b>Project Acronym:</b> {safe_text(row.get('projectAcronym'))}",
            f"<b>Start Date:</b> {safe_text(row.get('startDate'))}",
            f"<b>Deadline:</b> {safe_text(row.get('deadlineDate'))}",
            f"<b>Deadline Model:</b> {safe_text(row.get('deadlineModel'))}",
            "<br><br>",
            safe_text(row.get("description"), 1000),
        ]

        SubElement(item, "description").text = "".join(desc_parts)

        pub_date = format_date(row.get("startDate"))
        if pub_date:
            SubElement(item, "pubDate").text = pub_date

    ElementTree(rss).write(output_file, encoding="utf-8", xml_declaration=True)
    print("RSS created")

# --- RUN ---
if __name__ == "__main__":
    records = fetch_all()
    df = build_dataframe(records)
    create_rss(df, RSS_FILE)
