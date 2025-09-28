import json
import pandas as pd

# Load accounts
with open(r"api_data\accounts.json") as f:
    accounts_raw = json.load(f)
accounts_list = accounts_raw['results']  # <-- get the list inside "results"
accounts = pd.json_normalize(accounts_list)

# Load customers
with open(r"api_data\customers.json") as f:
    customers_raw = json.load(f)
customers_list = customers_raw['results']  # <-- same here
customers = pd.json_normalize(customers_list)

# Merge on customer_id
merged = accounts.merge(customers, left_on="customer_id", right_on="_id", suffixes=("_account", "_customer"))
account_totals = accounts.groupby('customer_id').agg(
    total_balance=('balance', 'sum'),
    total_rewards=('rewards', 'sum'),
    num_accounts=('balance', 'count')
).reset_index()

# Merge totals with customer info
customers_flat = pd.json_normalize(customers_list)
final_df = customers_flat.merge(account_totals, left_on='_id', right_on='customer_id')

# Select columns you want
final_df = final_df[['customer_id', 'first_name', 'last_name', 'total_balance', 'total_rewards', 'num_accounts']]
print(final_df.head())