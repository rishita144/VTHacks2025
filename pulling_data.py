import requests
import pandas as pd
import os
import json
import time
# --- CONFIG ---
API_KEY = "224c40ed37312905869cc5e0584b5738"
BASE_URL = "http://api.nessieisreal.com"
ENDPOINTS = ["loans"]
#http://api.nessieisreal.com/accounts/56c66be5a73e49274150727b/loans?key=224c40ed37312905869cc5e0584b5738


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
        
def fetch_account_loans(account_id):
    url = f"{BASE_URL}/accounts/{account_id}/loans?key={API_KEY}"
    r = requests.get(url)
    if r.status_code == 200:
        return r.json()
    return []


# Step 1: Get all accounts
accounts_url = f"{BASE_URL}/accounts?key={API_KEY}"
accounts = requests.get(accounts_url).json()

all_loans = []
for acc in accounts:
    acc_id = acc["_id"]
    loans = fetch_account_loans(acc_id)
    for loan in loans:
        loan["account_id"] = acc_id
        all_loans.append(loan)
    time.sleep(0.3)  # slow down to be safe

# Save as DataFrame
df_loans = pd.DataFrame(all_loans)
df_loans.to_json("loans.json", orient="records", indent=2)

print(f"✅ Pulled {len(df_loans)} loans across {len(accounts)} accounts")
