import requests
API_KEY = '614d45c57c1537311a4a3d53121b0492'
url = f"http://api.nessieisreal.com/branches?key={API_KEY}"
#url = f"http://api.nessieisreal.com/accounts"
response = requests.get(url)
data = response.json()
if response.status_code == 200:
    print("Branches table:")
    print(data)
else:
    print("Error:", response.status_code, response.text)
    #http://api.nessieisreal.com/enterprise/customers?key=614d45c57c1537311a4a3d53121b0492

#start from here
ef fetch_account_loans(account_id):
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
