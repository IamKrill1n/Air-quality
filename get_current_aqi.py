"""
Fetch real-time AQI for Hanoi from AQICN (WAQI) JSON API and store to CSV.
Requires: requests, pandas
"""

import os
import requests
import pandas as pd
from datetime import datetime
from dotenv import load_dotenv

load_dotenv()

# ---------- CONFIG ----------
CITY = "Hanoi"                     # city name used in API feed endpoint
TOKEN = os.getenv("AQICN_TOKEN")   # set your token in environment variable
OUTPUT_DIR = "data_aqicn"
os.makedirs(OUTPUT_DIR, exist_ok=True)

API_URL_TEMPLATE = "https://api.waqi.info/feed/{city}/?token={token}"

# ---------- fetch function ----------
def fetch_aqi_for_city(city: str, token: str):
    url = API_URL_TEMPLATE.format(city=city, token=token)
    resp = requests.get(url, timeout=20)
    resp.raise_for_status()
    data = resp.json()
    if data.get("status") != "ok":
        raise RuntimeError(f"API error: {data}")
    return data["data"]

# ---------- parse & save ----------
def parse_data(raw: dict):
    """
    Example raw data contains fields:
      aqi, idx, dominentpol, iaqi (dict of pollutant values), time (object), city (object)
    We flatten to a row.
    """
    row = {
        "timestamp_iso": raw.get("time", {}).get("iso"),
        "aqi": raw.get("aqi"),
        "idx": raw.get("idx"),
        "dominentpol": raw.get("dominentpol"),
        "city_name": raw.get("city", {}).get("name"),
        "geo_lat": raw.get("city", {}).get("geo", [None, None])[0],
        "geo_lon": raw.get("city", {}).get("geo", [None, None])[1]
    }
    # pollutant values
    iaqi = raw.get("iaqi", {})
    for pol, pol_data in iaqi.items():
        row[f"iaqi_{pol}"] = pol_data.get("v")
    return row

if __name__ == "__main__":
    if not TOKEN:
        raise SystemExit("Set environment variable AQICN_TOKEN with your token from aqicn.org")
    raw = fetch_aqi_for_city(CITY, TOKEN)
    row = parse_data(raw)
    print(row)
