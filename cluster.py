import pandas as pd
from sklearn.cluster import KMeans
from sklearn.preprocessing import StandardScaler
import matplotlib.pyplot as plt

# Load dataset
df = pd.read_csv("customer_full_transaction_summary.csv")

# Aggregate by zip5
zip_summary = df.groupby(['zip5', 'city', 'state']).agg({
    'balance': 'mean',
    'rewards': 'mean',
    'num_bills': 'sum',
    'num_recurring': 'sum',
    'num_transfers_sent': 'sum',
    'total_transfers_sent': 'sum',
    'num_transfers_received': 'sum',
    'total_transfers_received': 'sum',
    'num_deposits': 'sum',
    'total_deposits': 'sum',
    'num_withdrawals': 'sum',
    'total_withdrawals': 'sum',
    'total_p2p_count': 'sum',
    'total_p2p_volume': 'sum',
    'total_transactions_count': 'sum',
    'total_transaction_volume': 'sum',
    'deposit_withdrawal_ratio': 'mean'
}).reset_index()

# Pick features for clustering
features = [
    'balance', 'rewards', 'num_bills', 'num_recurring',
    'num_transfers_sent', 'total_transfers_sent',
    'num_transfers_received', 'total_transfers_received',
    'num_deposits', 'total_deposits',
    'num_withdrawals', 'total_withdrawals',
    'total_p2p_count', 'total_p2p_volume',
    'total_transactions_count', 'total_transaction_volume',
    'deposit_withdrawal_ratio'
]
X = zip_summary[features].fillna(0)

# Scale features
scaler = StandardScaler()
X_scaled = scaler.fit_transform(X)

# Elbow Method
inertia = []
K = range(1, 10)
for k in K:
    model = KMeans(n_clusters=k, random_state=42)
    model.fit(X_scaled)
    inertia.append(model.inertia_)

plt.plot(K, inertia, 'bx-')
plt.xlabel('k')
plt.ylabel('Inertia')
plt.title('Elbow Method for Optimal k (ZIP-level)')
plt.show()

# Train KMeans with chosen k (say k=4)
kmeans = KMeans(n_clusters=4, random_state=42)
zip_summary['cluster'] = kmeans.fit_predict(X_scaled)

# Cluster summaries
cluster_summary = zip_summary.groupby('cluster')[features].mean()
print("\n📊 ZIP-Level Cluster Summary")
print(cluster_summary)

# Save output
zip_summary.to_csv("zip_cluster_summary.csv", index=False)
print("\n💾 Results saved to zip_cluster_summary.csv")
