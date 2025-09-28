import streamlit as st
import pandas as pd
import folium
from streamlit_folium import st_folium
import json
import geopandas as gpd
#from wordcloud import WordCloud
#import matplotlib.pyplot as plt
#from textblob import TextBlob


st.set_page_config(layout="wide", page_title="ZIP Cluster Map")

st.title("ZIP Code Cluster Map")

# ------------------------
# 1. Load your cluster CSV
# ------------------------
df = pd.read_csv(
    "/Users/sruthisakala/VTHacks2025/zip_cluster_summary_spark.csv/part-00000-18cd97f2-d4ff-4e0e-b5a6-175e96800299-c000.csv"
)
df['zip5'] = df['zip5'].astype(str)
df['cluster_numeric'] = df['cluster']  # already numeric

# ------------------------
# 2. Load ZIP shapefile
# ------------------------
shapefile_path = "/Users/sruthisakala/VTHacks2025/cb_2018_us_zcta510_500k.zip"
gdf = gpd.read_file(f"zip://{shapefile_path}")

# Keep only ZIPs present in your CSV
gdf_filtered = gdf[gdf['ZCTA5CE10'].isin(df['zip5'])].copy()
gdf_filtered['ZCTA5CE10'] = gdf_filtered['ZCTA5CE10'].astype(str)

# Merge cluster data
merged = gdf_filtered.merge(df, left_on='ZCTA5CE10', right_on='zip5', how='left')

# Simplify polygons to reduce size (tolerance can be adjusted)
merged['geometry'] = merged['geometry'].simplify(tolerance=0.01, preserve_topology=True)

# Convert to GeoJSON
geojson_data = merged.to_json()

# ------------------------
# 3. Create Folium map
# ------------------------
m = folium.Map(location=[39, -98], zoom_start=4)

# Define color function for clusters
def cluster_color(cluster):
    if cluster == 0:
        return 'green'
    elif cluster == 1:
        return 'red'
    else:
        return 'gray'

# Add polygons with color based on cluster
folium.GeoJson(
    geojson_data,
    style_function=lambda feature: {
        'fillColor': cluster_color(feature['properties']['cluster_numeric']),
        'color': 'black',
        'weight': 0.3,
        'fillOpacity': 0.7
    },
    tooltip=folium.GeoJsonTooltip(
        fields=['ZCTA5CE10', 'cluster_numeric'],
        aliases=['ZIP:', 'Cluster:'],
        localize=True
    )
).add_to(m)

# ------------------------
# 4. Display map in Streamlit
# ------------------------
st_data = st_folium(m, width=1200, height=800)

# Show cluster distribution table
#st.write("Cluster distribution:")
#st.dataframe(df['cluster_numeric'].value_counts())

cluster_means_df = pd.read_csv("/Users/sruthisakala/VTHacks2025/cluster_means.csv")

st.subheader("Cluster Means Summary")
st.dataframe(cluster_means_df.drop(columns=['num_recurring', 'num_transfers_sent', 'total_transfers_sent', 'num_transfers_received', 'total_transfers_received', 'num_deposits', 'total_deposits', 'num_withdrawals', 'total_withdrawals', 'total_p2p_count', 'total_p2p_volume','deposit_withdrawal_ratio']))

# Optional: allow filtering by cluster
#selected_cluster = st.selectbox("Select Cluster to View", cluster_means_df['cluster'].unique())
#st.dataframe(cluster_means_df[cluster_means_df['cluster'] == selected_cluster])

# ------------------------
# 6. Add simple description text
# ------------------------
st.markdown("""
**Description:**  
The clusters in this table divide ZIP codes into **high-quality (Cluster 0)** and **low-quality (Cluster 1)** segments based on their average financial and transaction behavior, including balances, rewards, number of bills, and transaction activity.  

As shown in the table:  
- **Cluster 0** (high-quality) has nearly **twice the mean balance** and **four times the mean rewards** compared to Cluster 1.  
- Cluster 0 also exhibits **significantly higher transaction activity**, with more than **20 times the number of bills, mean transaction count, and total transaction volume** than Cluster 1.  

**Implications for the company:**  
- For **Cluster 0**, the company should focus on **retention strategies** to maintain and deepen engagement with these high-value ZIP codes.  
- For **Cluster 1**, the company should focus on **acquisition and growth strategies**, aiming to increase engagement and move these ZIP codes toward higher financial activity.  

This highlights both the stark contrast between high- and low-quality ZIP codes and actionable opportunities for targeted business strategies.
""")


