import pandas as pd

# Load dataset
df = pd.read_csv("/Users/sruthisakala/VTHacks2025/zip_cluster_summary_spark.csv/part-00000-18cd97f2-d4ff-4e0e-b5a6-175e96800299-c000.csv")

# Cluster-wise averages (numeric columns only)
cluster_means = df.groupby("cluster").mean(numeric_only=True).reset_index()

# Cluster counts
cluster_counts = df['cluster'].value_counts().reset_index()
cluster_counts.columns = ['cluster', 'count']

print("Cluster-wise averages:\n", cluster_means)
print("\nCluster counts:\n", cluster_counts)
print("\nColumn types:\n", df.dtypes)

# Save to CSVs
cluster_means.to_csv("cluster_means.csv", index=False)
cluster_counts.to_csv("cluster_counts.csv", index=False)
print("✅ Saved cluster_means.csv and cluster_counts.csv")
