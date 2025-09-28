import pandas as pd
import geopandas as gpd
import folium

# -----------------------------
# 1️⃣ Load clustered CSV
# -----------------------------
df = pd.read_csv("/Users/sruthisakala/VTHacks2025/zip_cluster_summary_spark.csv/part-00000-6a225f6a-904b-4dab-95d7-8facd14814bb-c000.csv")

# Ensure ZIP codes are strings
df['zip5'] = df['zip5'].astype(str)

# Map clusters for display if you want
df['cluster_label'] = df['cluster'].map({0: 'Low', 1: 'High'})
df['cluster_numeric'] = df['cluster']  # keep numeric for coloring

# -----------------------------
# 2️⃣ Load ZIP shapefile (ZCTA)
# -----------------------------
zip_map = gpd.read_file("/Users/sruthisakala/VTHacks2025/cb_2018_us_zcta510_500k.zip")

# Ensure ZIP column is string
zip_column = 'ZCTA5CE10'  # check your shapefile column name
zip_map[zip_column] = zip_map[zip_column].astype(str)

# -----------------------------
# 3️⃣ Merge cluster data into shapefile
# -----------------------------
map_df = zip_map.merge(df, left_on=zip_column, right_on='zip5', how='left')

# -----------------------------
# 4️⃣ Create Folium map
# -----------------------------
m = folium.Map(location=[39, -98], zoom_start=4)  # center of USA

# -----------------------------
# 5️⃣ Add colored polygons by cluster
# -----------------------------
def style_function(feature):
    cluster = feature['properties']['cluster_numeric']
    if cluster == 0:
        color = '#377eb8'  # blue = Low
    elif cluster == 1:
        color = '#e41a1c'  # red = High
    else:
        color = '#cccccc'  # gray = no data
    return {
        'fillColor': color,
        'color': 'black',
        'weight': 0.5,
        'fillOpacity': 0.7
    }

# Add polygons with tooltip
folium.GeoJson(
    map_df,
    style_function=style_function,
    tooltip=folium.GeoJsonTooltip(fields=[zip_column, 'cluster_label'],
                                  aliases=['ZIP:', 'Cluster:'])
).add_to(m)

# -----------------------------
# 6️⃣ Save to HTML
# -----------------------------
m.save("zip_cluster_map.html")
print("✅ Map saved as zip_cluster_map.html")
