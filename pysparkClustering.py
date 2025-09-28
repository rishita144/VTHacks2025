from pyspark.sql import SparkSession
from pyspark.sql import functions as F
from pyspark.ml.feature import VectorAssembler, StandardScaler
from pyspark.ml.clustering import KMeans

# ------------------------
# 1️⃣ Start Spark Session
# ------------------------
spark = SparkSession.builder \
    .appName("ZipcodeClustering") \
    .getOrCreate()

# ------------------------
# 2️⃣ Load dataset
# ------------------------
df = spark.read.csv("customer_full_transaction_summary.csv", header=True, inferSchema=True)

# ------------------------
# 3️⃣ Aggregate by zip5, city, state
# ------------------------
agg_df = df.groupBy("zip5", "city", "state").agg(
    F.avg("balance").alias("balance"),
    F.avg("rewards").alias("rewards"),
    F.sum("num_bills").alias("num_bills"),
    F.sum("num_recurring").alias("num_recurring"),
    F.sum("num_transfers_sent").alias("num_transfers_sent"),
    F.sum("total_transfers_sent").alias("total_transfers_sent"),
    F.sum("num_transfers_received").alias("num_transfers_received"),
    F.sum("total_transfers_received").alias("total_transfers_received"),
    F.sum("num_deposits").alias("num_deposits"),
    F.sum("total_deposits").alias("total_deposits"),
    F.sum("num_withdrawals").alias("num_withdrawals"),
    F.sum("total_withdrawals").alias("total_withdrawals"),
    F.sum("total_p2p_count").alias("total_p2p_count"),
    F.sum("total_p2p_volume").alias("total_p2p_volume"),
    F.sum("total_transactions_count").alias("total_transactions_count"),
    F.sum("total_transaction_volume").alias("total_transaction_volume"),
    F.avg("deposit_withdrawal_ratio").alias("deposit_withdrawal_ratio")
)

# ------------------------
# 4️⃣ Define numeric columns
# ------------------------
numeric_cols = [
    "balance", "rewards", "num_bills", "num_recurring",
    "num_transfers_sent", "total_transfers_sent",
    "num_transfers_received", "total_transfers_received",
    "num_deposits", "total_deposits",
    "num_withdrawals", "total_withdrawals",
    "total_p2p_count", "total_p2p_volume",
    "total_transactions_count", "total_transaction_volume",
    "deposit_withdrawal_ratio"
]

# ------------------------
# 5️⃣ Ensure numeric and fill nulls
# ------------------------
for col in numeric_cols:
    agg_df = agg_df.withColumn(col, F.col(col).cast("double"))
agg_df = agg_df.fillna(0)

# ------------------------
# 6️⃣ IQR Capping (x3)
# ------------------------
for col in numeric_cols:
    # Only cap if column has data
    non_null_count = agg_df.filter(F.col(col).isNotNull()).count()
    if non_null_count == 0:
        continue

    quantiles = agg_df.approxQuantile(col, [0.25, 0.75], 0.01)
    if len(quantiles) < 2:
        continue

    Q1, Q3 = quantiles
    IQR = Q3 - Q1
    lower = Q1 - 3 * IQR
    upper = Q3 + 3 * IQR
    agg_df = agg_df.withColumn(
        col,
        F.when(F.col(col) < lower, lower)
         .when(F.col(col) > upper, upper)
         .otherwise(F.col(col))
    )

# ------------------------
# 7️⃣ Assemble & Scale Features
# ------------------------
assembler = VectorAssembler(inputCols=numeric_cols, outputCol="features")
assembled_df = assembler.transform(agg_df)

scaler = StandardScaler(inputCol="features", outputCol="scaled_features", withStd=True, withMean=True)
scaler_model = scaler.fit(assembled_df)
scaled_df = scaler_model.transform(assembled_df)

# ------------------------
# 8️⃣ Run KMeans (k=2)
# ------------------------
kmeans = KMeans(k=2, seed=42, featuresCol="scaled_features", predictionCol="cluster")
model = kmeans.fit(scaled_df)
clustered_df = model.transform(scaled_df)

# ------------------------
# 9️⃣ Save Results
# ------------------------
output_df = clustered_df.drop("features", "scaled_features")
output_df.coalesce(1).write.csv("zip_cluster_summary_spark.csv", header=True, mode="overwrite")

# ------------------------
# 🔟 Cluster Counts & Means
# ------------------------
cluster_counts = output_df.groupBy("cluster").count().toPandas()
cluster_means = output_df.groupBy("cluster").mean().toPandas()

cluster_counts.to_csv("cluster_counts.csv", index=False)
cluster_means.to_csv("cluster_means.csv", index=False)

print("✅ Saved zip_cluster_summary_spark.csv, cluster_counts.csv, and cluster_means.csv")
print("Cluster counts:\n", cluster_counts)
print("Cluster-wise averages:\n", cluster_means)
