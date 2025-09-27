import requests
import time
import json

# 🏦 Step 1: Fetch customer data from Nessie API
NESSIE_API_KEY = "YOUR_NESSIE_API_KEY"  # ❗ Replace with your actual Nessie API key
nessie_url = f"http://api.nessieisreal.com/customers?key={NESSIE_API_KEY}"

print("🔄 Fetching customers from Nessie API...")
response = requests.get(nessie_url)
if response.status_code != 200:
    print("❌ Error fetching customers:", response.status_code, response.text)
    exit()

customers = response.json()
print(f"✅ Retrieved {len(customers)} customers")

# 📦 Step 2: Extract unique ZIP codes
zip_codes = set()
for c in customers:
    zip_code = c.get("address", {}).get("zip")
    if zip_code:
        zip_codes.add(str(zip_code))

print(f"📍 Found {len(zip_codes)} unique ZIP codes:", list(zip_codes))

# 💰 Step 3: Fetch income data from RapidAPI for each ZIP
income_url = "https://us-zip-code-to-income.p.rapidapi.com/"
headers = {
    "x-rapidapi-host": "us-zip-code-to-income.p.rapidapi.com",
    "x-rapidapi-key": "b09512ba34msh3255fd24a9cd3f6p151b94jsna691fc9e1c28"
}

zip_income_data = {}

print("\n🔎 Fetching median household income for each ZIP...\n")
for i, z in enumerate(zip_codes):
    try:
        params = {"zip": z}
        resp = requests.get(income_url, headers=headers, params=params)
        if resp.status_code == 200:
            data = resp.json()
            zip_income_data[z] = data
            print(f"✅ [{i+1}/{len(zip_codes)}] ZIP {z}: {data}")
        else:
            print(f"❌ ZIP {z}: {resp.status_code}")
    except Exception as e:
        print(f"⚠️ Error for ZIP {z}: {e}")
    time.sleep(1)  # ⏳ Respect API rate limits

# 💾 Step 4: Save ZIP-income mapping
with open("zip_income.json", "w") as f:
    json.dump(zip_income_data, f, indent=2)
print("\n💾 Saved ZIP income data → zip_income.json")

# 🧠 Step 5: Enrich customer data with income info
for c in customers:
    zip_code = str(c.get("address", {}).get("zip"))
    if zip_code in zip_income_data:
        c["income_info"] = zip_income_data[zip_code]

# 💾 Step 6: Save enriched customer file
with open("customers_with_income.json", "w") as f:
    json.dump(customers, f, indent=2)

print("💾 Saved enriched customers → customers_with_income.json")
print("✅ Done!")
