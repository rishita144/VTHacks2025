import requests
import pandas as pd
import os
import json

# --- CONFIG ---
API_KEY = "9a6342f288c6a3057d5c93aa32b8ba27"
BASE_URL = "http://api.nessieisreal.com/enterprise"
ENDPOINTS = ["accounts", "customers", "merchants", "bills", "deposits",
             "loans", "purchases", "transfers", "withdrawals"]

def fetch_with_retry(url, retries=3, delay=2):
    for i in range(retries):
        try:
            r = requests.get(url, timeout=30)
            if r.status_code == 200:
                return r.json()
        except requests.exceptions.RequestException:
            pass
        print(f"Retry {i+1}/{retries} for {url}")
        time.sleep(delay)
    return None

def fetch_all(endpoint, limit=50):
    all_data, skip = [], 0
    while True:
        url = f"{BASE_URL}/{endpoint}?key={API_KEY}&_limit={limit}&_skip={skip}"
        data = fetch_with_retry(url)
        if not data:
            break
        all_data.extend(data)
        if len(data) < limit:
            break
        skip += limit
        time.sleep(0.5)  # slow down to avoid timeout
    return all_data

dataframes = {}
for ep in ENDPOINTS:
    print(f"Fetching {ep} ...")
    data = fetch_all(ep)
    if data:
        df = pd.DataFrame(data)
        dataframes[ep] = df
        df.to_json(f"{ep}.json", orient="records", indent=2)
        print(f"✅ {ep} → {len(df)} rows")
    else:
        print(f"❌ Failed {ep}")